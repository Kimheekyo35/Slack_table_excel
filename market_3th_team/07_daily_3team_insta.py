#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Google Sheets ←→ Apify(Instagram) 수집기

변경점:
- 시트에 기록할 때, **기존 셀 값보다 새 지표가 클 때만** 덮어씀(작거나 같으면 스킵).

기능 요약:
- 조회수는 video_play_count만 사용(없으면 0).
- 헤더 탐색은 앵커 컬럼(기본 'X') '이후' 열만 대상으로 함.
- 조회수 헤더는 "조회수" 정확 일치 우선 → 없을 때 후보군 매칭.
- Selenium 보정 제거: Apify 결과만 사용.

필수 .env
  APIFY=<apify token>
  GOOGLE_KEYFILE=<서비스 계정 JSON 경로>
  SPREADSHEET_URL=<구글 시트 URL>

선택 .env
  INSTA_SHEET_NAME=<정확 시트명>
  INSTA_SHEET_MATCH_KEYWORD=인플루언서 리스트
  PARALLEL=6
  APIFY_TIMEOUT_SECONDS=300
  APIFY_MAX_RETRIES=4
  APIFY_RETRY_BASE=2.0
  HEADER_SCAN_LIMIT=30
  DATA_START_ROW=12
  ANCHOR_COL_LETTER=X        # 이 컬럼 '이후'만 스캔 (기본 X)

"""

import os, re, time, logging
from typing import Any, Dict, List, Optional, Sequence, Tuple, Set, NamedTuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from apify_client import ApifyClient
import gspread
from google.oauth2 import service_account
from dotenv import load_dotenv
from datetime import datetime as dt
import pytz

# ---------- 환경 ----------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, "..", "..", ".."))
load_dotenv(dotenv_path=os.path.join(PROJECT_ROOT, ".env"), override=True)
load_dotenv(dotenv_path=os.path.join(BASE_DIR, ".env"), override=True)

APIFY_TOKEN           = os.getenv("APIFY")
SPREADSHEET_URL       = os.getenv("SPREADSHEET_URL") or os.getenv("INSTA_SPREADSHEET_URL") or os.getenv("NEW_SPREADSHEET_URL")
FWEE_SPREADSHEET_URL  = os.getenv("FWEE_SPREADSHEET_URL")
GOOGLE_KEYFILE = os.getenv(
    "GOOGLE_KEYFILE",
    os.path.join(BASE_DIR, "vaulted-anthem-457811-s4-fb7e927161dc.json"),
)
if GOOGLE_KEYFILE and not os.path.isabs(GOOGLE_KEYFILE):
    base_candidate = os.path.join(BASE_DIR, GOOGLE_KEYFILE)
    root_candidate = os.path.join(PROJECT_ROOT, GOOGLE_KEYFILE)
    if os.path.exists(base_candidate):
        GOOGLE_KEYFILE = base_candidate
    elif os.path.exists(root_candidate):
        GOOGLE_KEYFILE = root_candidate
    else:
        GOOGLE_KEYFILE = base_candidate

SPECIFIC_SHEET_NAME   = os.getenv("INSTA_SHEET_NAME")
SHEET_MATCH_KEYWORD   = os.getenv("INSTA_SHEET_MATCH_KEYWORD", "인플루언서 리스트")
FWEE_SHEET_NAME       = os.getenv("FWEE_SHEET_NAME")
FWEE_SHEET_MATCH_KEYWORD = os.getenv("FWEE_SHEET_MATCH_KEYWORD", SHEET_MATCH_KEYWORD)
PARALLEL              = 10
APIFY_BATCH_SIZE      = 10
APIFY_ACTOR           = os.getenv("IG_APIFY_ACTOR", "apify/instagram-post-scraper")
APIFY_TIMEOUT_SECONDS = int(os.getenv("APIFY_TIMEOUT_SECONDS", "300"))
APIFY_MAX_RETRIES     = int(os.getenv("APIFY_MAX_RETRIES", "4"))
APIFY_RETRY_BASE      = float(os.getenv("APIFY_RETRY_BASE", "2.0"))
HEADER_SCAN_LIMIT     = int(os.getenv("HEADER_SCAN_LIMIT", "30"))
DATA_START_ROW        = int(os.getenv("DATA_START_ROW", "12"))
ANCHOR_COL_LETTER     = os.getenv("ANCHOR_COL_LETTER", "X").strip().upper()  # 이 '이후'만 스캔
FWEE_DATA_START_ROW   = int(os.getenv("FWEE_DATA_START_ROW", str(DATA_START_ROW)))
FWEE_ANCHOR_COL_LETTER= os.getenv("FWEE_ANCHOR_COL_LETTER", "W").strip().upper()
SHEET_NAME_KEYWORD    = "온에어리스트"
HEADER_ROW            = int(os.getenv("HEADER_ROW", "8"))
TEST_MAX_URLS         = int(os.getenv("TEST_MAX_URLS", "0"))
LOG_DECISIONS         = os.getenv("LOG_DECISIONS", "0").strip().lower() in ("1", "true", "yes", "y")

UA = os.getenv("UA") or (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ---------- 로깅 ----------
def setup_logging() -> logging.Logger:
    log_dir = os.path.join("logs", "insta")
    os.makedirs(log_dir, exist_ok=True)
    kst = pytz.timezone("Asia/Seoul")
    current_time = dt.now(kst)
    log_filename = os.path.join(
        log_dir,
        f"insta_3team_update_{current_time.strftime('%Y%m%d_%H%M%S')}.log",
    )

    logger = logging.getLogger("insta_3team_update")
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


log = setup_logging()

# ---------- 헤더/유틸 ----------
CONTENT_URL_HEADERS = ["콘텐츠url"]
VIEWS_HEADERS = ["조회수"]
LIKES_HEADERS = ["좋아요"]
SAVES_HEADERS = ["저장"]

class SheetConfig(NamedTuple):
    label: str
    spreadsheet_url: str
    specific_sheet_name: Optional[str]
    sheet_keyword: str
    anchor_col_letter: Optional[str]
    data_start_row: int

INT_RE = re.compile(r"\d+")
IG_URL_RE = re.compile(r"https?://(?:www\.)?instagram\.com/(?:reel|reels|p|tv)/[A-Za-z0-9_\-]+", re.I)
IG_SHORTCODE_RE = re.compile(r"instagram\.com/(?:reel|reels|p|tv)/([A-Za-z0-9_\-]+)", re.I)

def _parse_int_safe(v: Any) -> int:
    if v is None: return 0
    try:
        return max(int(v), 0)
    except Exception:
        pass
    try:
        return max(int(float(v)), 0)
    except Exception:
        pass
    s = str(v)
    m = INT_RE.findall(s.replace(",", ""))
    if m:
        try:
            return max(int("".join(m)), 0)
        except Exception:
            return 0
    return 0

def normalize_token(v: Any) -> str:
    return re.sub(r"[\s\u3000_/\\\-]+", "", str(v or "").strip().lower())

def header_matches(v: Any, cands: Sequence[str]) -> bool:
    t = normalize_token(v)
    if not t: return False
    for c in cands:
        ct = normalize_token(c)
        if ct and (t == ct or ct in t or t in ct):
            return True
    return False

def header_equals(v: Any, cands: Sequence[str]) -> bool:
    """정확 일치(정규화 후 완전 동일)만 허용"""
    t = normalize_token(v)
    if not t: return False
    for c in cands:
        if t == normalize_token(c):
            return True
    return False

def clean_row(row: Sequence[Any]) -> List[str]:
    return [str(c).replace("\n"," ").strip() for c in row]

def column_letter(idx: int) -> str:
    n = idx + 1; s = ""
    while n > 0:
        n, r = divmod(n-1, 26); s = chr(65+r) + s
    return s

def letter_to_index(letter: str) -> int:
    """A->0, B->1 ... AA->26 ..."""
    letter = (letter or "").strip().upper()
    n = 0
    for ch in letter:
        if not ('A' <= ch <= 'Z'):
            raise ValueError(f"잘못된 컬럼 문자: {letter}")
        n = n * 26 + (ord(ch) - ord('A') + 1)
    return n - 1  # zero-based

def normalize_anchor_letter(letter: Optional[str]) -> Optional[str]:
    """빈값/none/0/-1 → 제한 없음, 나머지는 대문자 컬럼 문자"""
    if letter is None:
        return None
    t = str(letter).strip()
    if not t:
        return None
    if t.lower() in ("none", "all", "any", "0", "-1"):
        return None
    return t.upper()

def find_header_row(rows: List[List[Any]]) -> int:
    limit = min(len(rows), HEADER_SCAN_LIMIT)
    for i in range(limit):
        if any(header_matches(col, ON_AIR_HEADER_CANDIDATES) for col in rows[i]):
            return i
    raise ValueError("'온에어 링크' 헤더를 포함한 행을 찾을 수 없습니다.")


def _find_header_index(header_row: List[str], candidates: Sequence[str]) -> Optional[int]:
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

def find_metric_columns_constrained(rows, header_row_idx, lookahead, start_after_col):
    found: Dict[str, int] = {}
    end = min(len(rows), header_row_idx + 1 + lookahead)

    # 1) 조회수: 정확 일치 우선
    for r in range(header_row_idx, end):
        row = clean_row(rows[r])
        for cidx in range(len(row)):
            if cidx < start_after_col:
                continue
            if header_equals(row[cidx], EXACT_HEADER["views"]):
                found["views"] = cidx
                break
        if "views" in found:
            break

    # 2) 나머지 키(조회수 포함) 후보 매칭
    for r in range(header_row_idx, end):
        row = clean_row(rows[r])
        for key, cands in METRIC_HEADER_CANDIDATES.items():
            if key in found:
                continue
            for cidx in range(len(row)):
                if cidx < start_after_col:
                    continue
                if header_matches(row[cidx], cands):
                    found[key] = cidx
                    break

    return found

# ---------- Apify 호출 ----------
def _is_rate_or_server(resp: Optional[requests.Response]) -> bool:
    try:
        code = resp.status_code if resp is not None else None
    except Exception:
        code = None
    return code in (429, 500, 502, 503, 504)

def apify_ig_run(urls: Sequence[str]) -> List[Dict[str, Any]]:
    if not APIFY_TOKEN:
        raise RuntimeError("APIFY 토큰 필요(.env: APIFY)")
    client = ApifyClient(APIFY_TOKEN)
    run_input = {
        "resultsLimit": 1,
        "skipPinnedPosts": False,
        "username": list(urls),
    }
    try:
        run = client.actor(APIFY_ACTOR).call(run_input=run_input)
    except TypeError:
        run = client.actor(APIFY_ACTOR).call(run_input=run_input)

    dataset = client.dataset(run["defaultDatasetId"])
    items = list(dataset.iterate_items())
    return items

def apify_ig_run_retry(urls: Sequence[str]) -> Optional[List[Dict[str, Any]]]:
    last = None
    for attempt in range(1, APIFY_MAX_RETRIES + 1):
        try:
            return apify_ig_run(urls)
        except Exception as e:
            last = e
            resp = getattr(e, "response", None)
            if not _is_rate_or_server(resp) and attempt >= 2:
                break
            delay = min((APIFY_RETRY_BASE ** (attempt - 1)), 30.0)
            log.warning(f"[apify] retry {attempt}/{APIFY_MAX_RETRIES} in {delay:.1f}s: {e}")
            time.sleep(delay)
    log.error(f"[apify] give up: {last}")
    return None

# ---------- Apify 아이템 → 지표 변환 ----------
def extract_all_metrics_apify(item: Dict[str, Any]) -> Dict[str, Any]:
    # 조회수: 오직 video_play_count만 사용 (없으면 0)
    views = _parse_int_safe(
      item.get("video_play_count")
      or item.get("videoPlayCount")
      or item.get("playCount")
      or item.get("viewCount")
  )

    # 좋아요: 필드 자체가 없으면 missing 처리
    like_fields = [item.get("likesCount"), item.get("like_count")]
    likes_found = any(v is not None and str(v).strip() != "" for v in like_fields)
    likes = None
    if likes_found:
        for v in like_fields:
            n = _parse_int_safe(v)
            if n >= 0:
                likes = n
                break

    comments = 0
    for v in [item.get("num_comments"), item.get("comment_count")]:
        n = _parse_int_safe(v)
        if n > 0: comments = n; break

    # 공유/저장(없으면 0) — 시트 미기입
    shares = _parse_int_safe(item.get("share_count") or item.get("shares") or item.get("send_count"))
    saves  = _parse_int_safe(item.get("save_count") or item.get("saves") or item.get("bookmark_count"))

    return {
        "views": views,
        "likes": likes,
        "likes_missing": not likes_found,
        "comments": comments,
        "shares": shares,
        "saves": saves,
    }

# ---------- Google Sheets ----------
def authorize_gspread(json_keyfile: str):
    scope = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://spreadsheets.google.com/feeds",
    ]
    creds = service_account.Credentials.from_service_account_file(json_keyfile, scopes=scope)
    return gspread.authorize(creds)

def find_target_sheet(spreadsheet, explicit_name: Optional[str], keyword: str):
    if explicit_name:
        return spreadsheet.worksheet(explicit_name)
    tok = normalize_token(keyword)
    for ws in spreadsheet.worksheets():
        if tok in normalize_token(ws.title):
            return ws
    raise ValueError(f"'{keyword}'를 포함한 시트를 찾을 수 없음.")

def extract_instagram_urls(cell_value: Any) -> List[str]:
    if cell_value is None: return []
    text = str(cell_value).strip()
    if not text: return []
    urls = IG_URL_RE.findall(text)
    if not urls and "instagram.com" in text.lower():
        urls = [text]
    return urls

def extract_shortcode_from_url(u: str) -> Optional[str]:
    if not u:
        return None
    m = IG_SHORTCODE_RE.search(str(u))
    if not m:
        return None
    return m.group(1)

def extract_item_url(item: Dict[str, Any]) -> Optional[str]:
    for key in ("inputUrl", "input_url", "url", "postUrl", "post_url"):
        if key in item:
            m = IG_URL_RE.search(str(item.get(key) or ""))
            if m:
                return m.group(0)
    return None

def extract_item_shortcode(item: Dict[str, Any]) -> Optional[str]:
    for key in ("shortCode", "shortcode", "short_code", "code"):
        if key in item and isinstance(item.get(key), str):
            return item.get(key)
    iu = extract_item_url(item)
    if iu:
        return extract_shortcode_from_url(iu)
    return None

def chunk_list(items: Sequence[str], size: int) -> List[List[str]]:
    return [list(items[i:i + size]) for i in range(0, len(items), size)]
# ---------- 메인 ----------
def run_for_sheet(gc, cfg: SheetConfig, apify_cache: Dict[str, Dict[str, int]]):
    log.info(f"[sheet:{cfg.label}] 시작")
    ss = gc.open_by_url(cfg.spreadsheet_url)
    target_sheets = [
        ws for ws in ss.worksheets()
        if _sheet_title_has_keyword(ws.title or "", cfg.sheet_keyword)
    ]
    if not target_sheets:
        log.info(f"[sheet:{cfg.label}] '{cfg.sheet_keyword}' 포함 시트 없음 → 종료")
        return

    def safe_sheet_name(name: str) -> str:
        escaped = str(name).replace("'", "''")
        return f"'{escaped}'"

    raw_url_entries: Dict[str, List[Dict[str, Any]]] = {}
    raw_url_order: List[str] = []
    seen_raw_urls: Set[str] = set()

    for ws in target_sheets:
        try:
            all_data = ws.get_all_values()
        except Exception as exc:
            log.warning(f"[sheet:{cfg.label}] 시트 데이터 로드 실패 ({ws.title}): {exc}")
            continue

        if not all_data or len(all_data) < HEADER_ROW:
            log.info(f"[sheet:{cfg.label}] 시트 데이터 없음 ({ws.title})")
            continue

        header_row = [str(c).replace("\n", " ").strip() for c in all_data[HEADER_ROW - 1]]
        content_idx = _find_header_index(header_row, CONTENT_URL_HEADERS)
        if content_idx is None:
            log.info(f"[sheet:{cfg.label}] 콘텐츠 url 헤더 없음 → 스킵 ({ws.title})")
            continue

        views_idx = _find_header_index(header_row, VIEWS_HEADERS)
        likes_idx = _find_header_index(header_row, LIKES_HEADERS)
        saves_idx = _find_header_index(header_row, SAVES_HEADERS)

        if views_idx is None:
            log.warning(f"[sheet:{cfg.label}] 조회수 헤더 없음 ({ws.title})")
        if likes_idx is None:
            log.warning(f"[sheet:{cfg.label}] 좋아요 헤더 없음 ({ws.title})")
        if saves_idx is None:
            log.warning(f"[sheet:{cfg.label}] 저장 헤더 없음 ({ws.title})")

        for row_idx in range(HEADER_ROW, len(all_data)):
            row_number = row_idx + 1
            row = all_data[row_idx]
            if content_idx >= len(row):
                continue
            raw_url_val = str(row[content_idx]).strip()
            urls = extract_instagram_urls(raw_url_val)
            if not urls:
                continue
            url = urls[0]

            cur_views = _parse_int_safe(row[views_idx]) if views_idx is not None and views_idx < len(row) else 0
            cur_likes = _parse_int_safe(row[likes_idx]) if likes_idx is not None and likes_idx < len(row) else 0
            cur_saves = _parse_int_safe(row[saves_idx]) if saves_idx is not None and saves_idx < len(row) else 0
            cur_likes_raw = str(row[likes_idx]).strip() if likes_idx is not None and likes_idx < len(row) else ""

            entry = {
                "sheet_title": ws.title,
                "row": row_number,
                "current": {
                    "views": cur_views,
                    "likes": cur_likes,
                    "saves": cur_saves,
                },
                "current_raw": {
                    "likes": cur_likes_raw,
                },
                "cols": {
                    "views": views_idx,
                    "likes": likes_idx,
                    "saves": saves_idx,
                },
            }

            raw_url_entries.setdefault(url, []).append(entry)
            if url not in seen_raw_urls:
                seen_raw_urls.add(url)
                raw_url_order.append(url)

    if not raw_url_entries:
        log.info(f"[sheet:{cfg.label}] 대상 URL 없음 → 종료")
        return

    unique_urls = list(raw_url_order)
    if TEST_MAX_URLS > 0:
        unique_urls = unique_urls[:TEST_MAX_URLS]
        log.info(f"[sheet:{cfg.label}] TEST_MAX_URLS 적용 → {len(unique_urls)}개만 처리")
    log.info(f"[sheet:{cfg.label}] 대상 URL 고유 {len(unique_urls)}개")

    shortcode_to_url: Dict[str, str] = {}
    for u in unique_urls:
        sc = extract_shortcode_from_url(u)
        if sc:
            shortcode_to_url.setdefault(sc, u)
            shortcode_to_url.setdefault(sc.lower(), u)

    # -------- Apify 병렬 호출 (캐시 사용) --------
    url_to_metrics: Dict[str, Dict[str, int]] = {}
    to_fetch = [u for u in unique_urls if u not in apify_cache]

    if to_fetch:
        batches = list(chunk_list(to_fetch, APIFY_BATCH_SIZE))
        workers = max(1, min(PARALLEL, 10, len(batches)))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(apify_ig_run_retry, batch): batch for batch in batches}
            done = 0
            total = len(futs)
            for fut in as_completed(futs):
                batch_urls = futs[fut]
                items = fut.result()
                items_by_url: Dict[str, Dict[str, Any]] = {}
                if isinstance(items, list):
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        sc = extract_item_shortcode(item)
                        if sc:
                            mu = shortcode_to_url.get(sc) or shortcode_to_url.get(sc.lower())
                            if mu:
                                items_by_url.setdefault(mu, item)
                                continue
                        iu = extract_item_url(item)
                        if iu:
                            sc2 = extract_shortcode_from_url(iu)
                            if sc2:
                                mu = shortcode_to_url.get(sc2) or shortcode_to_url.get(sc2.lower())
                                if mu:
                                    items_by_url.setdefault(mu, item)
                                    continue
                            items_by_url.setdefault(iu, item)
                for u in batch_urls:
                    item = items_by_url.get(u)
                    if isinstance(item, dict):
                        metrics = extract_all_metrics_apify(item)
                        metrics["_missing"] = False
                        apify_cache[u] = metrics
                    else:
                        apify_cache[u] = {
                            "views": 0,
                            "likes": 0,
                            "comments": 0,
                            "shares": 0,
                            "saves": 0,
                            "_missing": True,
                        }
                done += 1
                if done % 20 == 0 or done == total:
                    log.info(f"[sheet:{cfg.label}] Apify 진행 {done}/{total}")

    for u in unique_urls:
        url_to_metrics[u] = dict(apify_cache.get(u, {"views": 0, "likes": 0, "comments": 0, "shares": 0, "saves": 0}))

    if not url_to_metrics:
        log.info(f"[sheet:{cfg.label}] 업데이트할 데이터 없음.")
        return

    updates_map_by_sheet: Dict[str, Dict[str, Any]] = {}
    updated_rows: Set[Tuple[str, int]] = set()
    decision_counts = {
        "total_rows": 0,
        "updated_rows": 0,
        "skip_no_increase": 0,
        "skip_missing_cols": 0,
        "skip_apify_missing": 0,
        "likes_dash_set": 0,
    }

    def log_decision_summary() -> None:
        log.info(
            f"[sheet:{cfg.label}] 결정 로그 요약: "
            f"total_rows={decision_counts['total_rows']}, "
            f"updated_rows={decision_counts['updated_rows']}, "
            f"skip_no_increase={decision_counts['skip_no_increase']}, "
            f"skip_missing_cols={decision_counts['skip_missing_cols']}, "
            f"skip_apify_missing={decision_counts['skip_apify_missing']}, "
            f"likes_dash_set={decision_counts['likes_dash_set']}"
        )

    def add_update(sheet_title: str, col_index: int, row_number: int, value: Any) -> None:
        cell_ref = f"{column_letter(col_index)}{row_number}"
        updates_map_by_sheet.setdefault(sheet_title, {})[cell_ref] = value

    for url, entries in raw_url_entries.items():
        stats = url_to_metrics.get(url)
        if not stats:
            continue
        for entry in entries:
            decision_counts["total_rows"] += 1
            sheet_title = entry["sheet_title"]
            row_num = int(entry["row"])
            cur = entry.get("current", {}) or {}
            cur_raw = entry.get("current_raw", {}) or {}
            cols = entry.get("cols", {}) or {}
            updated_any = False
            reasons: List[str] = []

            views_col = cols.get("views")
            if isinstance(views_col, int):
                if stats["views"] > int(cur.get("views", 0)):
                    add_update(sheet_title, views_col, row_num, stats["views"])
                    updated_any = True
                    reasons.append("views_updated")
                else:
                    reasons.append("views_no_increase")
            else:
                reasons.append("views_col_missing")

            likes_col = cols.get("likes")
            if isinstance(likes_col, int):
                if stats.get("likes_missing"):
                    if not cur_raw.get("likes"):
                        add_update(sheet_title, likes_col, row_num, "-")
                        updated_any = True
                        decision_counts["likes_dash_set"] += 1
                        reasons.append("likes_missing_set_dash")
                    else:
                        reasons.append("likes_missing_skip_existing")
                else:
                    likes_val = stats.get("likes")
                    if likes_val is not None and likes_val > int(cur.get("likes", 0)):
                        add_update(sheet_title, likes_col, row_num, likes_val)
                        updated_any = True
                        reasons.append("likes_updated")
                    else:
                        reasons.append("likes_no_increase")
            else:
                reasons.append("likes_col_missing")

            saves_col = cols.get("saves")
            if isinstance(saves_col, int):
                if stats["saves"] > int(cur.get("saves", 0)):
                    add_update(sheet_title, saves_col, row_num, stats["saves"])
                    updated_any = True
                    reasons.append("saves_updated")
                else:
                    reasons.append("saves_no_increase")
            else:
                reasons.append("saves_col_missing")

            if updated_any:
                updated_rows.add((sheet_title, row_num))
                decision_counts["updated_rows"] += 1
            else:
                if stats.get("_missing"):
                    decision_counts["skip_apify_missing"] += 1
                    reasons.append("apify_missing")
                elif (
                    "views_col_missing" in reasons
                    and "likes_col_missing" in reasons
                    and "saves_col_missing" in reasons
                ):
                    decision_counts["skip_missing_cols"] += 1
                else:
                    decision_counts["skip_no_increase"] += 1

            if LOG_DECISIONS:
                cur_views = int(cur.get("views", 0))
                cur_likes = int(cur.get("likes", 0))
                cur_saves = int(cur.get("saves", 0))
                log.info(
                    "[decision] %s r%s url=%s cur(v/l/s)=%s/%s/%s new(v/l/s)=%s/%s/%s reasons=%s",
                    sheet_title,
                    row_num,
                    url,
                    cur_views,
                    cur_likes,
                    cur_saves,
                    stats.get("views"),
                    stats.get("likes"),
                    stats.get("saves"),
                    ",".join(reasons) if reasons else "updated",
                )

    if not updates_map_by_sheet:
        log_decision_summary()
        log.info(f"[sheet:{cfg.label}] 업데이트할 변경 없음(모든 신값이 기존값 이하).")
        return

    updates: List[Dict[str, List[List[Any]]]] = []
    for sheet_title, updates_map in updates_map_by_sheet.items():
        sheet_ref = safe_sheet_name(sheet_title)
        for cell, value in updates_map.items():
            updates.append({"range": f"{sheet_ref}!{cell}", "values": [[value]]})

    ss.values_batch_update({"valueInputOption": "RAW", "data": updates})
    log.info(f"[sheet:{cfg.label}] 업데이트 완료: {len(updated_rows)}행, 셀 {len(updates)}개 (1회 batch_update)")
    log_decision_summary()

def build_sheet_configs() -> List[SheetConfig]:
    if not SPREADSHEET_URL:
        raise SystemExit("SPREADSHEET_URL이 없습니다(.env)")

    return [
        SheetConfig(
            label="default",
            spreadsheet_url=SPREADSHEET_URL,
            specific_sheet_name=None,
            sheet_keyword=SHEET_NAME_KEYWORD,
            anchor_col_letter=None,
            data_start_row=HEADER_ROW + 1,
        )
    ]

def main():
    if not APIFY_TOKEN:       raise SystemExit("APIFY 토큰이 없습니다(.env: APIFY)")
    if not GOOGLE_KEYFILE:    raise SystemExit("GOOGLE_KEYFILE이 없습니다(.env)")

    configs = build_sheet_configs()
    gc = authorize_gspread(GOOGLE_KEYFILE)
    apify_cache: Dict[str, Dict[str, int]] = {}

    for cfg in configs:
        run_for_sheet(gc, cfg, apify_cache)
        break

if __name__ == "__main__":
    main()
