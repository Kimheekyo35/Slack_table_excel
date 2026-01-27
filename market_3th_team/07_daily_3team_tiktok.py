# -*- coding: utf-8 -*-
import os
import re
import logging
from datetime import datetime as dt
from typing import Dict, List, Optional, Sequence, Iterator, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urlunparse, quote

import pytz
import requests
import gspread
from dotenv import load_dotenv
from apify_client import ApifyClient
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials

# ===================== Env & Const =====================
load_dotenv(override=True)

APIFY_TOKEN = os.getenv("APIFY")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GOOGLE_KEYFILE = os.getenv(
    "GOOGLE_KEYFILE",
    os.path.join(BASE_DIR, "vaulted-anthem-457811-s4-fb7e927161dc.json"),
)
DEFAULT_SPREADSHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1BvYpsaVKDdR8thRm-Pxyo3MaNK7n1lV8AoZYOpyBDiI/edit"
    "?gid=511556661#gid=511556661"
)
SPREADSHEET_URL = os.getenv("SPREADSHEET_URL", DEFAULT_SPREADSHEET_URL)

SHEET_NAME_KEYWORD = "온에어리스트"
REQUEST_BATCH_SIZE = int(os.getenv("REQUEST_BATCH_SIZE", "25"))
APIFY_MAX_WORKERS = int(os.getenv("APIFY_MAX_WORKERS", "24"))
SHORT_URL_MAX_WORKERS = int(os.getenv("SHORT_URL_MAX_WORKERS", "32"))
SHORT_URL_TIMEOUT = int(os.getenv("TIKTOK_REQUEST_TIMEOUT", "12"))
TEST_ROW_START = int(os.getenv("TEST_ROW_START", "2"))
TEST_ROW_END = int(os.getenv("TEST_ROW_END", "1000000000"))
APIFY_MEMORY_MB = 256
HEADER_ROW = int(os.getenv("HEADER_ROW", "8"))

SHORT_DOMAINS = {"vm.tiktok.com", "vt.tiktok.com"}
VIDEO_ID_RE = re.compile(r"/(?:video|photo)/(\d+)")
SHORT_PROXY_ENABLED = os.getenv("SHORT_PROXY_ENABLED", "0").lower() in {"1", "true", "yes", "y"}
PROXY_URL = os.getenv("PROXY_URL", "").strip()
PROXY_USER = os.getenv("USERNAME", "").strip()
PROXY_PASSWORD = os.getenv("PASSWORD", "").strip()
PROXY_HOST = os.getenv("PROXY_HOST", "isp.decodo.com").strip()
PROXY_PORT = os.getenv("PROXY_PORT", "10000").strip()
PROXY_SCHEME = os.getenv("PROXY_SCHEME", "http").strip()
SHORT_DEBUG = os.getenv("SHORT_DEBUG", "0").lower() in {"1", "true", "yes", "y"}
SHORT_DEBUG_SAMPLE = int(os.getenv("SHORT_DEBUG_SAMPLE", "10"))

CONTENT_URL_HEADERS = ["콘텐츠url"]
VIEWS_HEADERS = ["조회수"]
LIKES_HEADERS = ["좋아요"]
SAVES_HEADERS = ["저장"]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if GOOGLE_KEYFILE and not os.path.isabs(GOOGLE_KEYFILE):
    GOOGLE_KEYFILE = os.path.join(BASE_DIR, GOOGLE_KEYFILE)

# ===================== Logging =====================
def setup_logging() -> logging.Logger:
    log_dir = os.path.join("logs", "tiktok")
    os.makedirs(log_dir, exist_ok=True)
    kst = pytz.timezone("Asia/Seoul")
    current_time = dt.now(kst)
    log_filename = os.path.join(
        log_dir,
        f"tiktok_3team_update_{current_time.strftime('%Y%m%d_%H%M%S')}.log",
    )

    logger = logging.getLogger("tiktok_3team_update")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    fh = logging.FileHandler(log_filename, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)

    logging.getLogger("apify_client").setLevel(logging.WARNING)
    logging.getLogger("apify_client._http_client").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    return logger


logger = setup_logging()


def print_info(msg: str) -> None:
    logger.info(msg)

# ===================== Google Auth =====================
def authorize_gspread(json_keyfile: str):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    creds = service_account.Credentials.from_service_account_file(json_keyfile, scopes=scope)
    client = gspread.authorize(creds)
    return client

# ===================== URL utils =====================
def normalize_tiktok_url(u: str) -> Optional[str]:
    if not isinstance(u, str):
        return None
    s = u.strip().strip('"\'')

    if s.startswith("https:/") and not s.startswith("https://"):
        s = "https://" + s[len("https:/"):]
    if s.startswith("http:/") and not s.startswith("http://"):
        s = "http://" + s[len("http:/"):]

    if s.startswith("tiktok.com/") or s.startswith("www.tiktok.com/"):
        s = "https://" + s

    try:
        p = urlparse(s)
        if not p.netloc and p.path.startswith("tiktok.com"):
            netloc, _, path = p.path.partition("/")
            p = p._replace(netloc=netloc, path="/" + path)
        if not p.scheme:
            p = p._replace(scheme="https")
        p = p._replace(query="", fragment="")
        s = urlunparse(p)
    except Exception:
        return None

    if "tiktok.com" not in s:
        return None
    return s.replace("http://", "https://")


def is_short_tiktok_url(u: str) -> bool:
    try:
        p = urlparse(u)
        host = (p.netloc or "").lower()
        path = p.path or ""
        if host in SHORT_DOMAINS:
            return True
        if path.startswith("/t/") or "/t/" in path:
            return True
        return False
    except Exception:
        return False


def video_id_from_url(u: str) -> Optional[str]:
    m = VIDEO_ID_RE.search(u or "")
    return m.group(1) if m else None


def resolve_single_tiktok_url(url: str, timeout: int = 12) -> str:
    base = normalize_tiktok_url(url)
    if not base:
        return url
    if not is_short_tiktok_url(base):
        return base
    try:
        resolved, _used_proxy, _status, _err = resolve_single_tiktok_url_with_proxy(
            base,
            timeout=timeout,
            proxy_url=_build_proxy_url() if SHORT_PROXY_ENABLED else None,
        )
        return resolved
    except Exception:
        return base


def _proxy_config() -> Optional[Dict[str, str]]:
    if not (PROXY_USER and PROXY_PASSWORD and PROXY_HOST and PROXY_PORT):
        return None
    proxy = f"http://{PROXY_USER}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}"
    return {"http": proxy, "https": proxy}


def _build_proxy_url() -> Optional[str]:
    if PROXY_URL:
        return PROXY_URL
    if not (PROXY_USER and PROXY_PASSWORD and PROXY_HOST and PROXY_PORT):
        return None
    user = quote(PROXY_USER, safe="")
    password = quote(PROXY_PASSWORD, safe="")
    return f"{PROXY_SCHEME}://{user}:{password}@{PROXY_HOST}:{PROXY_PORT}"


def resolve_single_tiktok_url_with_proxy(
    url: str,
    timeout: int = 12,
    proxy_url: Optional[str] = None,
) -> Tuple[str, bool, Optional[int], str]:
    base = normalize_tiktok_url(url)
    if not base:
        return url, False, None, "invalid_url"
    if not is_short_tiktok_url(base):
        return base, False, None, "not_short"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else _proxy_config()
    try:
        resp = requests.get(
            base,
            headers=headers,
            allow_redirects=True,
            timeout=timeout,
            proxies=proxies,
        )
        final_url = normalize_tiktok_url(resp.url) or base
        return final_url, bool(proxies), resp.status_code, "ok"
    except Exception as exc:
        return base, bool(proxies), None, f"exc:{type(exc).__name__}"


def resolve_tiktok_short_urls(urls: Sequence[str], max_workers: int = SHORT_URL_MAX_WORKERS) -> Dict[str, str]:
    unique_urls: List[str] = []
    seen = set()
    for u in urls:
        norm = normalize_tiktok_url(u) or u
        if isinstance(norm, str) and norm and norm not in seen:
            seen.add(norm)
            unique_urls.append(norm)

    results: Dict[str, str] = {}
    short_list: List[str] = []
    for u in unique_urls:
        if is_short_tiktok_url(u):
            short_list.append(u)
        else:
            results[u] = u

    if not short_list:
        return results

    proxy_url = _build_proxy_url() if SHORT_PROXY_ENABLED else None
    if SHORT_PROXY_ENABLED and not proxy_url:
        logger.warning("SHORT_PROXY_ENABLED=1 이지만 프록시 설정이 비었습니다.")

    logger.info(
        "Short URL resolve: total_urls=%d short_urls=%d workers=%d",
        len(unique_urls),
        len(short_list),
        max(1, min(max_workers, len(short_list))),
    )

    workers = max(1, min(max_workers, len(short_list)))
    debug_meta: Dict[str, Dict[str, object]] = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        fut2url = {
            ex.submit(resolve_single_tiktok_url_with_proxy, u, SHORT_URL_TIMEOUT, proxy_url): u
            for u in short_list
        }
        for fut in as_completed(fut2url):
            original = fut2url[fut]
            try:
                resolved, used_proxy, status, err = fut.result()
            except Exception as exc:
                logger.warning(f"Short URL resolution failed ({original}): {exc}")
                resolved, used_proxy, status, err = original, False, None, f"exc:{type(exc).__name__}"
            results[original] = resolved
            if SHORT_DEBUG:
                debug_meta[original] = {
                    "resolved": resolved,
                    "used_proxy": used_proxy,
                    "status": status,
                    "err": err,
                }

    if SHORT_DEBUG:
        no_change = 0
        resolved_with_id = 0
        resolved_no_id = 0
        proxy_used = 0
        proxy_success = 0
        examples_no_change: List[Tuple[str, str]] = []
        examples_no_id: List[Tuple[str, str]] = []
        examples_proxy_fail: List[Tuple[str, str, Optional[int], str]] = []
        examples_proxy_success: List[Tuple[str, str]] = []
        for original in short_list:
            resolved = results.get(original, original)
            meta = debug_meta.get(original, {})
            used_proxy = bool(meta.get("used_proxy"))
            status = meta.get("status")
            err = str(meta.get("err") or "")
            if resolved == original:
                no_change += 1
                if len(examples_no_change) < SHORT_DEBUG_SAMPLE:
                    examples_no_change.append((original, resolved))
            if video_id_from_url(resolved):
                resolved_with_id += 1
                if used_proxy:
                    proxy_success += 1
                    if len(examples_proxy_success) < SHORT_DEBUG_SAMPLE:
                        examples_proxy_success.append((original, resolved))
            else:
                resolved_no_id += 1
                if len(examples_no_id) < SHORT_DEBUG_SAMPLE:
                    examples_no_id.append((original, resolved))
                if used_proxy and len(examples_proxy_fail) < SHORT_DEBUG_SAMPLE:
                    examples_proxy_fail.append((original, resolved, status, err))
            if used_proxy:
                proxy_used += 1
        logger.info(
            "SHORT_DEBUG: short_urls=%d resolved_with_video_id=%d resolved_no_video_id=%d no_change=%d proxy_used=%d proxy_success=%d",
            len(short_list),
            resolved_with_id,
            resolved_no_id,
            no_change,
            proxy_used,
            proxy_success,
        )
        if examples_no_change:
            logger.info("SHORT_DEBUG: no_change examples=%s", examples_no_change)
        if examples_no_id:
            logger.info("SHORT_DEBUG: no_video_id examples=%s", examples_no_id)
        if examples_proxy_success:
            logger.info("SHORT_DEBUG: proxy_success examples=%s", examples_proxy_success)
        if examples_proxy_fail:
            logger.info("SHORT_DEBUG: proxy_fail examples=%s", examples_proxy_fail)
    return results

# ===================== Misc utils =====================
def parse_int_value(val: Optional[str]) -> int:
    if val is None:
        return 0
    s = str(val).strip()
    if not s:
        return 0
    s = re.sub(r"[^0-9]", "", s)
    if not s:
        return 0
    try:
        return int(s)
    except Exception:
        return 0


def column_letter(col_idx: int) -> str:
    n = col_idx + 1
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def chunk_list(items: Sequence[str], size: int) -> Iterator[List[str]]:
    for i in range(0, len(items), size):
        yield list(items[i: i + size])

# ===================== Apify =====================
def get_tiktok_video_data(
    client: ApifyClient,
    video_urls: List[str],
    request_batch_size: int,
    max_workers: int,
) -> List[Dict]:
    results: List[Dict] = []
    if not video_urls:
        return results

    batches = list(chunk_list(video_urls, request_batch_size))
    total_batches = len(batches)
    effective_workers = max(1, min(max_workers, total_batches))

    def run_batch(batch_urls: List[str]) -> List[Dict]:
        run_input = {
            "type": "VIDEO",
            "urls": batch_urls,
            "limit": len(batch_urls),
            "shouldDownloadVideos": False,
            "shouldDownloadCovers": False,
            "shouldDownloadSubtitles": False,
            "shouldDownloadSlideshowImages": False,
        }
        endpoint = "https://api.apify.com/v2/acts/novi~fast-tiktok-api/run-sync-get-dataset-items"
        params = {"token": APIFY_TOKEN, "memory": APIFY_MEMORY_MB}
        r = requests.post(endpoint, params=params, json=run_input, timeout=3000)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"Apify 오류 status={r.status_code} body={r.text[:500]}")
        try:
            items = r.json()
        except Exception as exc:
            raise RuntimeError(f"Apify response parse failed: {exc}") from exc
        if isinstance(items, dict) and "data" in items:
            items = items["data"]
        if not isinstance(items, list):
            raise RuntimeError("Apify response parse failed: unexpected payload")
        return items

    future_to_meta: Dict = {}
    with ThreadPoolExecutor(max_workers=effective_workers) as executor:
        for idx, batch in enumerate(batches, 1):
            fut = executor.submit(run_batch, batch)
            future_to_meta[fut] = (idx, len(batch))

        processed_batches = 0
        for fut in as_completed(future_to_meta):
            idx, _ = future_to_meta[fut]
            try:
                batch_results = fut.result()
                results.extend(batch_results)
                processed_batches += 1
                pct = int(processed_batches * 100 / total_batches)
                logger.info(f"Apify 진행 {pct}% (batch {idx}/{total_batches})")
            except Exception as exc:
                logger.warning(f"Apify batch {idx}/{total_batches} 실패: {exc}")

    return results

# ===================== Build updates =====================
def build_updates(
    apify_results: List[Dict],
    url_entries: Dict[str, List[Dict]],
) -> Tuple[List[Dict[str, List[List[object]]]], int]:
    updates_map_by_sheet: Dict[str, Dict[str, object]] = {}
    updated_rows = set()

    def safe_sheet_name(name: str) -> str:
        escaped = str(name).replace("'", "''")
        return f"'{escaped}'"

    def add_update(sheet_title: str, col_index: int, row_number: int, value: object):
        cell_ref = f"{column_letter(col_index)}{row_number}"
        updates_map_by_sheet.setdefault(sheet_title, {})[cell_ref] = value

    url_by_video_id: Dict[str, str] = {}
    for url in url_entries.keys():
        vid = video_id_from_url(url)
        if vid:
            url_by_video_id[vid] = url

    stats_by_url: Dict[str, Dict[str, int]] = {}
    for result in apify_results:
        try:
            aweme_id = str(result.get("aweme_id", "") or result.get("id", "")).strip()
            resolved_url = url_by_video_id.get(aweme_id)

            if not resolved_url:
                share_url = (result.get("share_url") or result.get("shareUrl") or "").split("?")[0]
                nid = video_id_from_url(share_url)
                if nid and nid in url_by_video_id:
                    resolved_url = url_by_video_id[nid]
            if not resolved_url:
                continue

            stats = result.get("statistics", {}) or {}
            views = int(stats.get("play_count", 0) or 0)
            likes = int(stats.get("digg_count", 0) or 0)
            saves = int(stats.get("collect_count", 0) or 0)

            prev = stats_by_url.get(resolved_url)
            if not prev:
                stats_by_url[resolved_url] = {
                    "views": views,
                    "likes": likes,
                    "saves": saves,
                }
            else:
                prev["views"] = max(prev["views"], views)
                prev["likes"] = max(prev["likes"], likes)
                prev["saves"] = max(prev["saves"], saves)
        except Exception:
            logger.exception("Apify 결과 처리 오류")

    for url, entries in url_entries.items():
        stats = stats_by_url.get(url)
        if not stats:
            continue
        for entry in entries:
            sheet_title = entry["sheet_title"]
            row_num = int(entry["row"])
            cur = entry.get("current", {}) or {}
            cols = entry.get("cols", {}) or {}
            updated_any = False

            views_col = cols.get("views")
            if isinstance(views_col, int) and stats["views"] > int(cur.get("views", 0)):
                add_update(sheet_title, views_col, row_num, stats["views"])
                updated_any = True

            likes_col = cols.get("likes")
            if isinstance(likes_col, int) and stats["likes"] > int(cur.get("likes", 0)):
                add_update(sheet_title, likes_col, row_num, stats["likes"])
                updated_any = True

            saves_col = cols.get("saves")
            if isinstance(saves_col, int) and stats["saves"] > int(cur.get("saves", 0)):
                add_update(sheet_title, saves_col, row_num, stats["saves"])
                updated_any = True

            if updated_any:
                updated_rows.add((sheet_title, row_num))

    updates: List[Dict[str, List[List[object]]]] = []
    for sheet_title, updates_map in updates_map_by_sheet.items():
        sheet_ref = safe_sheet_name(sheet_title)
        for cell, value in updates_map.items():
            updates.append({"range": f"{sheet_ref}!{cell}", "values": [[value]]})

    return updates, len(updated_rows)


def batch_apply(spreadsheet, updates: List[Dict[str, List[List[object]]]]) -> bool:
    if not updates:
        return False
    try:
        spreadsheet.values_batch_update({"valueInputOption": "RAW", "data": updates})
        return True
    except AttributeError:
        spreadsheet.batch_update({"valueInputOption": "RAW", "data": updates})
        return True
    except Exception:
        logger.exception("시트 업데이트 실패")
        return False

# ===================== Main =====================
def _find_header_index(header_row: List[str], candidates: List[str]) -> Optional[int]:
    def norm(s: str) -> str:
        return re.sub(r"\s+", "", (s or "")).lower()

    header_norm = [norm(h) for h in header_row]
    for cand in candidates:
        c = norm(cand)
        for idx, h in enumerate(header_norm):
            if c and c in h:
                return idx
    return None


def _sheet_title_has_keyword(title: str, keyword: str) -> bool:
    def norm(s: str) -> str:
        return re.sub(r"\s+", "", (s or "")).lower()

    return norm(keyword) in norm(title or "")


def main():
    if not APIFY_TOKEN:
        logger.error("환경 변수 'APIFY'가 설정되지 않았습니다.")
        return
    if not GOOGLE_KEYFILE:
        logger.error("환경 변수 'GOOGLE_KEYFILE'이 설정되지 않았습니다.")
        return
    if not SPREADSHEET_URL:
        logger.error("스프레드시트 URL을 찾을 수 없습니다.")
        return

    kst = pytz.timezone("Asia/Seoul")
    now_kst = dt.now(kst)
    print_info(f"시작 {now_kst.strftime('%Y-%m-%d %H:%M:%S')} (KST)")

    try:
        gclient = authorize_gspread(GOOGLE_KEYFILE)
        spreadsheet = gclient.open_by_url(SPREADSHEET_URL)
    except Exception as exc:
        logger.exception(f"스프레드시트 연결 실패: {exc}")
        return

    try:
        all_sheets = spreadsheet.worksheets()
    except Exception as exc:
        logger.exception(f"워크시트 목록 로드 실패: {exc}")
        return

    target_sheets = [
        ws for ws in all_sheets
        if _sheet_title_has_keyword(ws.title or "", SHEET_NAME_KEYWORD)
    ]
    if not target_sheets:
        print_info(f"'{SHEET_NAME_KEYWORD}' 포함 시트 없음 → 종료")
        return

    sheet_by_title = {ws.title: ws for ws in target_sheets}
    print_info(f"대상 시트 {len(target_sheets)}개: {', '.join(sheet_by_title.keys())}")

    raw_url_entries: Dict[str, List[Dict]] = {}
    raw_url_order: List[str] = []
    seen_raw_urls = set()

    for sheet in target_sheets:
        try:
            all_data = sheet.get_all_values()
        except Exception as exc:
            logger.warning(f"시트 데이터 로드 실패 ({sheet.title}): {exc}")
            continue

        if not all_data or len(all_data) < HEADER_ROW:
            logger.info(f"시트 데이터 없음 ({sheet.title})")
            continue

        header_row = [
            str(c).replace("\n", " ").strip()
            for c in all_data[HEADER_ROW - 1]
        ]
        content_idx = _find_header_index(header_row, CONTENT_URL_HEADERS)
        if content_idx is None:
            logger.info(f"콘텐츠 url 헤더 없음 → 스킵 ({sheet.title})")
            continue

        views_idx = _find_header_index(header_row, VIEWS_HEADERS)
        likes_idx = _find_header_index(header_row, LIKES_HEADERS)
        saves_idx = _find_header_index(header_row, SAVES_HEADERS)

        if views_idx is None:
            logger.warning(f"조회수 헤더 없음 ({sheet.title})")
        if likes_idx is None:
            logger.warning(f"좋아요 헤더 없음 ({sheet.title})")
        if saves_idx is None:
            logger.warning(f"저장 헤더 없음 ({sheet.title})")

        for row_idx in range(HEADER_ROW, len(all_data)):
            row_number = row_idx + 1
            if row_number < TEST_ROW_START or row_number > TEST_ROW_END:
                continue
            row = all_data[row_idx]
            if content_idx >= len(row):
                continue
            raw_url_val = str(row[content_idx]).strip()
            if not raw_url_val or "tiktok.com" not in raw_url_val:
                continue

            norm = normalize_tiktok_url(raw_url_val)
            if not norm:
                continue

            cur_views = parse_int_value(row[views_idx]) if views_idx is not None and views_idx < len(row) else 0
            cur_likes = parse_int_value(row[likes_idx]) if likes_idx is not None and likes_idx < len(row) else 0
            cur_saves = parse_int_value(row[saves_idx]) if saves_idx is not None and saves_idx < len(row) else 0

            entry = {
                "sheet_title": sheet.title,
                "row": row_number,
                "current": {
                    "views": cur_views,
                    "likes": cur_likes,
                    "saves": cur_saves,
                },
                "cols": {
                    "views": views_idx,
                    "likes": likes_idx,
                    "saves": saves_idx,
                },
            }

            raw_url_entries.setdefault(norm, []).append(entry)
            if norm not in seen_raw_urls:
                seen_raw_urls.add(norm)
                raw_url_order.append(norm)

    if not raw_url_entries:
        print_info("대상 URL 없음 → 종료")
        return

    print_info("URL 정규화 진행")
    resolved_map = resolve_tiktok_short_urls(list(raw_url_entries.keys()), max_workers=SHORT_URL_MAX_WORKERS)

    url_entries: Dict[str, List[Dict]] = {}
    unique_urls: List[str] = []
    for raw_url in raw_url_order:
        resolved = resolved_map.get(raw_url, raw_url)
        resolved = normalize_tiktok_url(resolved) or resolved
        url_entries.setdefault(resolved, []).extend(raw_url_entries.get(raw_url, []))
        if resolved not in unique_urls:
            unique_urls.append(resolved)

    if not unique_urls:
        print_info("정규화 후 유효 URL 없음 → 종료")
        return

    apify_client = ApifyClient(APIFY_TOKEN)
    apify_results = get_tiktok_video_data(
        apify_client,
        unique_urls,
        request_batch_size=REQUEST_BATCH_SIZE,
        max_workers=APIFY_MAX_WORKERS,
    )

    updates, updated_rows = build_updates(apify_results, url_entries)
    if not updates:
        print_info("업데이트 없음 → 종료")
        return

    ok = batch_apply(spreadsheet, updates)
    if ok:
        print_info(f"완료: 업데이트 {updated_rows}행, 셀 {len(updates)}개 (1회 batch_update)")
    else:
        print_info("업데이트 실패 또는 없음")


if __name__ == "__main__":
    main()
