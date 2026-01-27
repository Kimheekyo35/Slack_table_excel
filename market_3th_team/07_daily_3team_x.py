import logging
import os
import re
import socket
import ssl
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse, urlunparse
import google_auth_httplib2
import httplib2
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError, sync_playwright
from dotenv import load_dotenv

now = datetime.now()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, "..", "..", ".."))
load_dotenv(dotenv_path=os.path.join(PROJECT_ROOT, ".env"), override=True)
load_dotenv(dotenv_path=os.path.join(BASE_DIR, ".env"), override=True)

SPREADSHEET_URL = os.getenv("SPREADSHEET_URL")
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

GOOGLE_HTTP_TIMEOUT = int(os.getenv("GOOGLE_HTTP_TIMEOUT", "120"))
GOOGLE_RETRIES = int(os.getenv("GOOGLE_RETRIES", "3"))
GOOGLE_RETRY_BACKOFF = float(os.getenv("GOOGLE_RETRY_BACKOFF", "2.0"))

URL_COLUMN_CANDIDATES = ["콘텐츠 URL", "콘텐츠URL"]
# 시트에서 적재할 5개의 메트릭 (좌측은 내부 키, 우측은 사람이 읽을 라벨)
METRIC_FIELDS: Sequence[Tuple[str, str]] = (
    ("views", "조회수"),
    ("likes", "좋아요"),
    ("retweets", "RT"),
    ("bookmarks", "저장"),
)

PLAYWRIGHT_HEADLESS = os.getenv("TWITTER_HEADLESS", "true").lower() != "false"
PLAYWRIGHT_NAV_TIMEOUT_MS = int(os.getenv("TWITTER_NAV_TIMEOUT_MS", "60000"))
PLAYWRIGHT_WAIT_AFTER_GOTO = float(os.getenv("TWITTER_WAIT_AFTER_GOTO", "1.5"))
PLAYWRIGHT_SELECTOR_TIMEOUT_MS = int(os.getenv("TWITTER_SELECTOR_TIMEOUT_MS", "8000"))

SHEET_NAME_KEYWORD = os.getenv("SHEET_NAME_KEYWORD", "온에어")
DEFAULT_HEADER_ROW = int(os.getenv("HEADER_ROW", "8"))


@dataclass
class SheetData:
    name: str
    header_row: int
    df: pd.DataFrame
    row_numbers: List[int]

# log 임력하기
import logging
# 로그 설정을 한번에 해줌
# logging.basicConfig(
#     filename = 'logs/log_twitter_googlespreadsheet.log',
#     filemode = 'w',
#     format = '%(asctime)s - %(levelname)s - %(message)s',
#     datefmt = '%Y-%m-%d %H:%M:%S',
#     # 로그 레벨을 DEBUG로 설정하여 모든 레벨의 로그를 기록
#     level = logging.DEBUG)

# logger = logging.getLogger("twitter_googlespreadsheet")

# 콘솔 로그 출력을 한 곳에서 관리
# def print(message: str) -> None:
#     logger.info(message)


# 구글 시트 URL에서 spreadsheetId만 추출
# 실제 Google Sheets API를 호출하려면 스프레드시트의 ID가 필요
def spreadsheet_id_from_url(url: str) -> str:
    match = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
    if not match:
        raise ValueError("SPREADSHEET_URL에서 ID를 찾을 수 없습니다.")
    return match.group(1)


def _normalize_text(value: Optional[str]) -> str:
    return re.sub(r"\s+", "", (value or "")).strip().lower()


def _sheet_title_has_keyword(title: str, keyword: str) -> bool:
    return _normalize_text(keyword) in _normalize_text(title)


def _quote_sheet(name: str) -> str:
    return "'" + name.replace("'", "''") + "'"


# Google Sheets API 클라이언트를 생성
def build_sheets_service(keyfile: str):
    if not os.path.exists(keyfile):
        raise FileNotFoundError(f"서비스 계정 키 파일을 찾을 수 없습니다: {keyfile}")
    creds = service_account.Credentials.from_service_account_file(
        keyfile,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    http = httplib2.Http(timeout=GOOGLE_HTTP_TIMEOUT)
    authed_http = google_auth_httplib2.AuthorizedHttp(creds, http=http)
    return build("sheets", "v4", cache_discovery=False, http=authed_http)


# API 요청이 일시적으로 실패할 때 재시도 로직 적용
def execute_with_retry(request, description: str):
    for attempt in range(1, GOOGLE_RETRIES + 1):
        try:
            return request.execute()
        except (TimeoutError, socket.timeout, ssl.SSLError, httplib2.HttpLib2Error, HttpError) as exc:
            if attempt >= GOOGLE_RETRIES:
                raise RuntimeError(f"{description} 실패: {exc}") from exc
            sleep_seconds = GOOGLE_RETRY_BACKOFF ** (attempt - 1)
            print(f"{description} 실패 (시도 {attempt}/{GOOGLE_RETRIES}): {exc} -> {sleep_seconds:.1f}s 대기")
            time.sleep(sleep_seconds)


# 시트 셀 값을 문자열로 정규화
def _cell_to_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


# 단일 시트를 DataFrame + 행 정보로 로드
def fetch_sheet_dataframe(service, spreadsheet_id: str, sheet_name: str, header_row: int) -> SheetData:
    print(f"{sheet_name} 시트 로드 (헤더 {header_row}행)")
    range_str = f"{_quote_sheet(sheet_name)}!A{header_row}:ZZ"
    response = execute_with_retry(
        service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_str,
            majorDimension="ROWS",
        ),
        f"{sheet_name} 데이터 조회",
    )

    values = response.get("values", [])
    if not values:
        print(f"{sheet_name}: 값이 없습니다.")
        return SheetData(name=sheet_name, header_row=header_row, df=pd.DataFrame(), row_numbers=[])

    header = [_cell_to_text(h) for h in values[0]]
    width = len(header)
    data_rows = values[1:]

    data_start_idx = 0
    while data_start_idx < len(data_rows) and not any(_cell_to_text(cell) for cell in data_rows[data_start_idx]):
        data_start_idx += 1

    normalized_rows: List[List[str]] = []
    row_numbers: List[int] = []
    for offset, raw_row in enumerate(data_rows[data_start_idx:]):
        row = (raw_row + [""] * (width - len(raw_row)))[:width]
        if not any(_cell_to_text(cell) for cell in row):
            break
        normalized_rows.append(row)
        row_numbers.append(header_row + 1 + data_start_idx + offset)

    if not normalized_rows:
        df = pd.DataFrame(columns=header)
    else:
        df = pd.DataFrame(normalized_rows, columns=header)
    print(f"{sheet_name}: {len(df)}행 로드 완료")
    return SheetData(name=sheet_name, header_row=header_row, df=df, row_numbers=row_numbers)


def list_onair_sheet_infos(service, spreadsheet_id: str) -> List[Tuple[str, int]]:
    response = execute_with_retry(
        service.spreadsheets().get(spreadsheetId=spreadsheet_id),
        "시트 목록 조회",
    )
    titles = [s.get("properties", {}).get("title", "") for s in response.get("sheets", [])]
    target = [
        (title, DEFAULT_HEADER_ROW)
        for title in titles
        if title and _sheet_title_has_keyword(title, SHEET_NAME_KEYWORD)
    ]
    return target


# 설정된 모든 시트를 순회 로드
def fetch_all_sheets(service, spreadsheet_id: str) -> List[SheetData]:
    datasets: List[SheetData] = []
    sheet_infos = list_onair_sheet_infos(service, spreadsheet_id)
    if not sheet_infos:
        raise RuntimeError(f"'{SHEET_NAME_KEYWORD}' 포함 시트를 찾지 못했습니다.")
    for sheet_name, header_row in sheet_infos:
        datasets.append(fetch_sheet_dataframe(service, spreadsheet_id, sheet_name, header_row))
    return datasets


# TWITTER_TARGET_COLUMNS 설정을 파싱해 5개 열을 확정(없으면 None)
def parse_target_columns_override() -> Optional[List[str]]:
    raw = os.getenv("TWITTER_TARGET_COLUMNS")
    if not raw:
        return None
    columns = [token.strip().upper() for token in raw.split(",") if token.strip()]
    if len(columns) != len(METRIC_FIELDS):
        raise ValueError(
            f"TWITTER_TARGET_COLUMNS는 {len(METRIC_FIELDS)}개 열을 지정해야 합니다."
        )
    invalid = [col for col in columns if not re.fullmatch(r"[A-Z]+", col)]
    if invalid:
        raise ValueError(f"올바르지 않은 열 표기: {', '.join(invalid)}")
    return columns


# 0-based column index를 A1 열 문자로 변환 (0 -> A, 27 -> AB)
def column_letter_from_index(index: int) -> str:
    if index < 0:
        raise ValueError("column index는 음수가 될 수 없습니다.")
    n = index + 1
    letters = []
    while n:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(ord("A") + rem))
    return "".join(reversed(letters))


# 헤더 기반으로 시트별 열 매핑을 계산 (override가 있으면 그대로 사용)
def resolve_sheet_target_columns(sheet_data: SheetData, override: Optional[Sequence[str]]) -> List[str]:
    if override:
        return list(override)
    header = list(sheet_data.df.columns)
    if not header:
        raise ValueError("헤더 행을 찾지 못했습니다.")
    resolved: List[str] = []
    for _, label in METRIC_FIELDS:
        normalized_label = label.strip()
        try:
            idx = next(i for i, h in enumerate(header) if h.strip() == normalized_label)
        except StopIteration:
            raise ValueError(f"'{label}' 헤더를 찾을 수 없습니다.") from None
        resolved.append(column_letter_from_index(idx))
    return resolved


# x.com/twitter.com URL을 정규화
def normalize_twitter_url(u: Optional[str]) -> Optional[str]:
    if not isinstance(u, str):
        return None
    s = u.strip().strip('"\'')
    if not s:
        return None
    if s.startswith("x.com/") or s.startswith("twitter.com/"):
        s = "https://" + s
    if s.startswith("https:/") and not s.startswith("https://"):
        s = "https://" + s[len("https:/"):]
    if s.startswith("http:/") and not s.startswith("http://"):
        s = "http://" + s[len("http:/"):]
    try:
        parsed = urlparse(s)
    except Exception:
        return None
    if not parsed.netloc and parsed.path.startswith("x.com"):
        netloc, _, path = parsed.path.partition("/")
        parsed = parsed._replace(netloc=netloc, path="/" + path)
    if parsed.netloc in {"x.com", "www.x.com"}:
        parsed = parsed._replace(netloc="x.com")
    if not parsed.netloc or "x.com" not in parsed.netloc:
        return None
    if not parsed.scheme:
        parsed = parsed._replace(scheme="https")
    parsed = parsed._replace(query="", fragment="")
    normalized = urlunparse(parsed)
    if normalized.startswith("http://"):
        normalized = "https://" + normalized[len("http://"):]
    return normalized


# "1.2K" 같은 문자열을 정수로 변환
def parse_count_from_text(text: Optional[str]) -> int:
    if not text:
        return 0
    cleaned = text.replace(",", "").replace(" ", "")
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)([KkMm만천]?)", cleaned)
    if match:
        number = float(match.group(1))
        suffix = match.group(2)
        if suffix.lower() == "k":
            number *= 1_000
        elif suffix.lower() == "m":
            number *= 1_000_000
        elif suffix == "만":
            number *= 10_000
        elif suffix == "천":
            number *= 1_000
        return int(number)
    digits = re.findall(r"\d+", cleaned)
    if not digits:
        return 0
    return int("".join(digits))


# 공통 버튼 셀렉터에서 카운트를 읽는 헬퍼
def _extract_button_metric(page, testid: str) -> int:
    locator = page.locator(f"button[data-testid='{testid}']").first
    try:
        locator.wait_for(state="visible", timeout=PLAYWRIGHT_SELECTOR_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        return 0
    try:
        label = locator.get_attribute("aria-label") or ""
    except Exception:
        label = ""
    return parse_count_from_text(label)


# 조회수 영역 셀렉터 여러 개를 시도
def _extract_view_count(page) -> int:
    view_text = None
    try:
        selector = "div.css-175oi2r div[role='group']"
        locator = page.locator(selector)
        locator.wait_for(state="visible", timeout=PLAYWRIGHT_SELECTOR_TIMEOUT_MS)
        label = locator.get_attribute("aria-label") or ""
        if label:
            view_text = int(label.split(",")[-1].strip(" views"))
    except PlaywrightTimeoutError:
        return 0
    except Exception:
        return 0
    if view_text:
        return view_text
    return 0

# Playwright 기반 크롤러 클래스
class PlaywrightCrawler:
    def __init__(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=PLAYWRIGHT_HEADLESS,args=["--no-sandbox","--disable-dev-shm-usage"])
        self.context = self.browser.new_context(locale="en-US")
        self.page = self.context.new_page()

    def fetch_metrics(self, url: str) -> Dict[str, int]:
        print(f"트윗 조회: {url}")
        metrics = {key: 0 for key, _ in METRIC_FIELDS}
        self.page.goto(url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_NAV_TIMEOUT_MS)
        if PLAYWRIGHT_WAIT_AFTER_GOTO > 0:
            time.sleep(PLAYWRIGHT_WAIT_AFTER_GOTO)
        metrics["views"] = _extract_view_count(self.page)
        metrics["likes"] = _extract_button_metric(self.page, "like")
        metrics["retweets"] = _extract_button_metric(self.page, "retweet")
        metrics["bookmarks"] = _extract_button_metric(self.page, "bookmark")
        return metrics
       
    def close(self):
        self.browser.close()
        self.playwright.stop()


# 한 행에 대응하는 5개 열 업데이트 payload 생성
def build_metric_updates(sheet_name: str, row_num: int, metrics: Dict[str, int], target_columns: Sequence[str]):
    updates = []
    for (key, _), column in zip(METRIC_FIELDS, target_columns):
        value = int(metrics.get(key, 0) or 0)
        updates.append({
            "range": f"'{sheet_name}'!{column}{row_num}",
            "values": [[value]],
        })
    return updates

# 행 입력 쪼개기
def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

CHUNK_SIZE = 100

# 시트 한 장을 순회하며 크롤링·업데이트 수행
def process_sheet(service, spreadsheet_id: str, sheet_data: SheetData, override_columns: Optional[Sequence[str]],crawler) -> int:
    if sheet_data.df.empty or not sheet_data.row_numbers:
        print(f"{sheet_data.name}: 처리할 데이터가 없습니다.")
        return 0

    try:
        target_columns = resolve_sheet_target_columns(sheet_data, override_columns)
    except ValueError as exc:
        print(f"{sheet_data.name}: {exc}")
        return 0

    url_column = next((col for col in URL_COLUMN_CANDIDATES if col in sheet_data.df.columns), None)
    if not url_column:
        print(f"{sheet_data.name}: URL 컬럼을 찾지 못했습니다.")
        return 0

    updates = []
    metrics_cache: Dict[str, Dict[str, int]] = {}
    
    for idx, row_num in enumerate(sheet_data.row_numbers):
        raw_url = _cell_to_text(sheet_data.df.iloc[idx].get(url_column, ""))
        url = normalize_twitter_url(raw_url)
        if not url:
            continue
        metrics = metrics_cache.get(url)
        if metrics is None:
            try:
                metrics = crawler.fetch_metrics(url)
            except PlaywrightTimeoutError as exc:
                logger.warning("%s: %s 수집 타임아웃 (%s)", sheet_data.name, url, exc)
                continue
            except Exception as exc:
                logger.warning("%s: %s 수집 실패 (%s)", sheet_data.name, url, exc)
                continue
            metrics_cache[url] = metrics
        updates.extend(build_metric_updates(sheet_data.name, row_num, metrics, target_columns))

    if not updates:
        print(f"{sheet_data.name}: 업데이트할 값이 없습니다.")
        return 0

    for chunk in chunked(updates, CHUNK_SIZE):
        execute_with_retry(
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"valueInputOption": "RAW", "data": chunk},
            ),f"{sheet_data.name} 메트릭 업데이트"
        ),print(f"{sheet_data.name} 메트릭 업데이트")
        time.sleep(2)  # 청크 간 대기 시간
    return len(updates)


# 전체 파이프라인을 실행
def main() -> None:
    try:
        override_columns = parse_target_columns_override()
    except ValueError as exc:
        print(f"TWITTER_TARGET_COLUMNS 설정 오류: {exc}")
        return
    print(f"==== 크롤링 시작: {now.strftime('%Y-%m-%d %H:%M:%S')} ====")
    spreadsheet_id = spreadsheet_id_from_url(SPREADSHEET_URL)
    service = build_sheets_service(GOOGLE_KEYFILE)
    
    sheet_datasets = fetch_all_sheets(service, spreadsheet_id)
    total_updates = 0

    # Playwright 크롤러 초기화
    crawler = PlaywrightCrawler()
    for sheet_data in sheet_datasets:
        total_updates += process_sheet(service, spreadsheet_id, sheet_data, override_columns,crawler)
        print(f"{sheet_data.name}: 완료")
        time.sleep(5)  # 시트 간 약간의 대기 시간
    print(f"전체 완료: {total_updates}개 셀 업데이트")


if __name__ == "__main__":
    main()
