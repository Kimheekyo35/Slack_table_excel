import os
import time
import re
import random
from pathlib import Path
from typing import Optional, Iterable, Tuple, List

import pandas as pd
import psycopg2
from psycopg2 import Error
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_utils import build_chrome_options, create_chrome_driver, cleanup_driver

# --------------------------------------------------------------------------- #
# 환경 설정
# --------------------------------------------------------------------------- #
load_dotenv(override=True)

DB_HOST = os.getenv("PG_HOST", "141.164.49.115")
DB_PORT = os.getenv("PG_PORT", "5432")
DB_DATABASE = os.getenv("PG_DATABASE", "benow_db")
DB_USER = os.getenv("PG_USER", "wemarketing_user")
DB_PASSWORD = os.getenv("PG_PASSWORD", "ehgus1500")

SEOUL_TZ = ZoneInfo("Asia/Seoul")

# 컬럼 이름 상수
COL_RANK = "순위"
COL_BRAND = "브랜드명"
COL_PRODUCT_NAME = "제품명"
COL_PRICE = "가격"
COL_DATETIME_TEXT = "날짜와시간"
COL_CHANNEL = "채널"
COL_CATEGORY = "카테고리"
COL_COLLECTED_AT = "수집일시"

COL_PREVIOUS_RANK = "전일 순위"
COL_RANK_DELTA = "전일 변동"
COL_STATUS = "전일대비 증감"

COL_PREVIOUS_RANK_WEEK = "전주 순위"
COL_WEEK_DELTA = "전주 변동"
COL_WEEK_STATUS = "전주대비 증감"

# 크롤링 대상 URL
BESTSELLER_URL = "https://www.yesstyle.com/en/beauty-beauty/list.html/bcc.15478_bpt.46?sb=136"
SKINCARE_URL = "https://www.yesstyle.com/en/beauty-skin-care/list.html/bcc.15544_bpt.46"
NUMBUZIN_URL = "https://www.yesstyle.com/en/numbuzin/list.html/bpt.299_bid.326359"
CRAWL_LIMIT = 100

REPORT_LIMIT = 50
COL_BRAND_KEY = "_brand_key"
COL_PRODUCT_KEY = "_product_key"

# --------------------------------------------------------------------------- #
# 유틸 함수
# --------------------------------------------------------------------------- #
def ensure_data_directory() -> Path:
    base = Path(__file__).resolve().parent
    data_dir = base / "data"
    data_dir.mkdir(exist_ok=True)
    return data_dir


def fill_missing(items: List[Optional[str]], target: int) -> List[Optional[str]]:
    if len(items) < target:
        items.extend([None] * (target - len(items)))
    return items

def _extract_primary_price(text: str) -> str:
    if not text:
        return "N/A"
    match = re.search(r"([₩￦$€£¥])\s?\d[\d,]*(?:\.\d+)?", text)
    if match:
        return match.group(0).strip()
    num = re.search(r"\d[\d,]*(?:\.\d+)?", text)
    if num:
        prefix = text[:num.start()].strip()
        symbol = ""
        if prefix and prefix[-1] in "₩￦$€£¥":
            symbol = prefix[-1]
        return f"{symbol} {num.group(0)}".strip()
    return text.strip()


VOLUME_PREFIX_PATTERN = re.compile(r"^\(\d+(?:ML|EA|G|PATCHES|PCS|PADS)\)\s*", re.IGNORECASE)


def _normalize_product_name(name: str) -> str:
    if name is None:
        return ""
    text = str(name).strip()
    text = VOLUME_PREFIX_PATTERN.sub("", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _normalize_match_key(value: Optional[str]) -> str:
    if value is None:
        return ""
    text = str(value)
    return re.sub(r"\s+", "", text)


def _add_match_keys_inplace(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return df
    if COL_BRAND in df.columns:
        df[COL_BRAND_KEY] = df[COL_BRAND].apply(_normalize_match_key)
    else:
        df[COL_BRAND_KEY] = ""
    if COL_PRODUCT_NAME in df.columns:
        df[COL_PRODUCT_KEY] = df[COL_PRODUCT_NAME].apply(_normalize_match_key)
    else:
        df[COL_PRODUCT_KEY] = ""
    return df


def _is_numbuzin_brand(value: Optional[str]) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() == "numbuzin"

def _is_fwee_brand(value: Optional[str]) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() == "fwee"


def _deduplicate_by_match_keys(df: Optional[pd.DataFrame], limit: Optional[int] = None) -> Optional[pd.DataFrame]:
    if df is None:
        return None
    work = df.copy()
    _add_match_keys_inplace(work)
    work = work.drop_duplicates(subset=[COL_BRAND_KEY, COL_PRODUCT_KEY])
    if limit is not None:
        work = work.head(limit)
    work.reset_index(drop=True, inplace=True)
    return work


def parse_list_grid_products(products: Iterable, start_rank: int = 1):
    names, prices, ranks, brands = [], [], [], []
    rank = start_rank
    for item in products:
        try:
            raw_name = item.find_element(By.CSS_SELECTOR, "[class*='itemTitle']").text
        except Exception:
            raw_name = "N/A"

        normalized_name = _normalize_product_name(raw_name)
        brand = "N/A"
        product = normalized_name
        if "-" in normalized_name:
            brand_part, product_part = normalized_name.split("-", 1)
            brand_candidate = brand_part.strip()
            product = _normalize_product_name(product_part)
            brand = brand_candidate if brand_candidate else "N/A"

        try:
            raw_price = item.find_element(By.CSS_SELECTOR, "[class*='itemPrice']").text
        except Exception:
            raw_price = "N/A"

        names.append(product)
        prices.append(_extract_primary_price(raw_price))
        ranks.append(rank)
        brands.append(brand)
        rank += 1
    return names, prices, ranks, brands


def yesstyle_scroll_crawling(
    driver: webdriver.Chrome,
    url: str,
    target_count: int = 100,
    product_selector: str = "a[class*='itemContainer']",
) -> Tuple[List[Optional[str]], List[Optional[str]], List[Optional[int]], List[Optional[str]]]:
    driver.get(url)
    time.sleep(random.uniform(2.5, 3.5))

    gathered_names: List[Optional[str]] = []
    gathered_prices: List[Optional[str]] = []
    gathered_ranks: List[Optional[int]] = []
    gathered_brands: List[Optional[str]] = []

    page_index = 0
    while len(gathered_names) < target_count:
        page_index += 1
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, product_selector))
            )
        except Exception:
            pass

        products = driver.find_elements(By.CSS_SELECTOR, product_selector)
        if not products:
            print(f"[WARN] No products found with selector '{product_selector}' at {url}.")
            break

        remaining = target_count - len(gathered_names)
        batch = products[:remaining]
        names, prices, ranks, brands = parse_list_grid_products(
            batch, start_rank=len(gathered_names) + 1
        )
        gathered_names.extend(names)
        gathered_prices.extend(prices)
        gathered_ranks.extend(ranks)
        gathered_brands.extend(brands)

        if len(gathered_names) >= target_count:
            break

        first_old = products[0]
        if not _find_and_click_next(driver):
            break
        try:
            WebDriverWait(driver, 10).until(EC.staleness_of(first_old))
        except Exception:
            pass
        time.sleep(random.uniform(1.0, 2.0))

    fill_missing(gathered_names, target_count)
    fill_missing(gathered_prices, target_count)
    fill_missing(gathered_ranks, target_count)
    fill_missing(gathered_brands, target_count)

    return gathered_names, gathered_prices, gathered_ranks, gathered_brands


def _find_and_click_next(driver: webdriver.Chrome) -> bool:
    selectors = [
        "a[class*='nextPage']",
        "a[class*='simpleDirectionButton']",
        "button[class*='productListingMain_blackButton__']",
    ]
    for selector in selectors:
        try:
            element = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            time.sleep(0.2)
            element.click()
            return True
        except Exception:
            continue
    return False

def df_to_slack_table_block(
    df:pd.DataFrame,
    columns:list[str],
    max_rows: int=50,
    column_settings: list[dict] | None =  None
) -> dict:
    view = df.loc[:,columns].head(max_rows)

    if column_settings is None:
        column_settings =[{} for _ in columns]
    
    # 열 이름 지정
    rows = []
    rows.append([{"type": "raw_text", "text": str(c)} for c in columns])

    # data
    for _,r in view.iterrows():
        rows.append([
            {"type": "raw_text", "text": "" if pd.isna(r[c]) else str(r[c])}
            for c in columns
        ])
    return {
        "blocks": [
            {
                "type": "table",
                "column_settings": column_settings,
                "rows": rows,
            }
        ]
    }
# --------------------------------------------------------------------------- #
# 메인 로직
# --------------------------------------------------------------------------- #
def main():
    options, temp_profile = build_chrome_options(profile_prefix="yesstyle_profile_")
    chrome_bin = os.getenv("CHROME_BIN")
    if chrome_bin:
        options.binary_location = chrome_bin

    page_load_timeout = int(os.getenv("SELENIUM_PAGELOAD_TIMEOUT", "60"))
    implicit_wait = int(os.getenv("SELENIUM_IMPLICIT_WAIT", "10"))
    driver = create_chrome_driver(
        options=options,
        temp_profile=temp_profile,
        page_load_timeout=page_load_timeout,
        implicit_wait=implicit_wait,
    )

    run_time = datetime.now(SEOUL_TZ)
    run_time_naive = run_time.astimezone(SEOUL_TZ).replace(tzinfo=None)
    date_str = f"{run_time.strftime('%y')}년 {run_time.month}월 {run_time.day}일 {run_time.hour}시"
    iso_timestamp = run_time.strftime("%Y-%m-%d %H:%M:%S")

    category_configs = [
        {"name": "Skincare", "url": SKINCARE_URL, "limit": CRAWL_LIMIT},
        {"name": "Numbuzin", "url": NUMBUZIN_URL, "limit": CRAWL_LIMIT},
        {"name": "Bestsellers", "url": BESTSELLER_URL, "limit": CRAWL_LIMIT},
    ]

    category_frames: dict[str, pd.DataFrame] = {}

    for config in category_configs:
        print(f"Start crawling {config['name']}...")
        names, prices, ranks, brands = yesstyle_scroll_crawling(
            driver,
            config["url"],
            target_count=config["limit"],
        )
        df = pd.DataFrame(
            {
                COL_RANK: ranks,
                COL_BRAND: brands,
                COL_PRODUCT_NAME: names,
                COL_CATEGORY: [config["name"]] * len(names),
                COL_PRICE: prices
            }
        )
        payload = df_to_slack_table_block(
            df=df,
            columns=df.columns,
            max_rows=50,
            column_settings=[
            {"align": "right"}, # rank
            {"is_wrapped": True}, # brand
            {"is_wrapped": True}, # product_name
            {"is_wrapped": True}, # category
            {"align": "right"}, # price
            ]
        )
        # category_frames[config["name"]] = df.dropna(subset=[COL_RANK]).copy()
        # numbuzin_df = df[df[COL_BRAND].apply(_is_numbuzin_brand)].copy()
        # fwee_df = df[df[COL_BRAND].apply(_is_fwee_brand)].copy()
        # 자사 브랜드만 추출
        break
    return payload

if __name__ == "__main__":
    md = main()
    print(md)
