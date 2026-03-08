"""
China PIPE / 定向增发 Historical Data Scraper
=============================================
Primary source  : East Money Datacenter (RPT_SEO_DETAIL, SEO_TYPE=1)
Reference price : baostock 20-trading-day average before issue date
PDF fallback    : pdfplumber (optional – install separately)

Output columns
--------------
stock_code          A-share ticker, e.g. "600519"
company_name        Chinese short name
announcement_date   Issue date (ISSUE_DATE from East Money)
listing_date        Date new shares started trading (ISSUE_LISTING_DATE)
lock_length_months  Lock-up period in months (parsed from string)
issue_price         Price per share placed (CNY)
reference_price     20-trading-day average before issue date (CNY)
discount_pct        (reference - issue) / reference * 100 [positive = discount]
shares_issued       Total shares placed
amount_raised_cny   Gross proceeds (CNY)
issue_objects       Names of placement recipients (string)
source              Provenance tag

Date range: 2010-01-01 to 2024-12-31
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ── constants ─────────────────────────────────────────────────────────────────
DATE_START = datetime(2010, 1, 1)
DATE_END = datetime(2024, 12, 31)
PAGE_SIZE = 500
SLEEP_BTW_PAGES = 0.6  # seconds

_EM_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://data.eastmoney.com/dxsz/",
})

# Precomputed format → expected string length for _parse_date
_DATE_FORMATS: list[tuple[str, int]] = [
    ("%Y-%m-%d %H:%M:%S", 19),
    ("%Y-%m-%d", 10),
    ("%Y%m%d", 8),
]


# ── helpers ───────────────────────────────────────────────────────────────────

def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(str(v).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _parse_date(v: Any) -> datetime | None:
    """Parse various date formats to datetime (date portion only)."""
    if not v:
        return None
    s = str(v).strip()
    for fmt, length in _DATE_FORMATS:
        try:
            return datetime.strptime(s[:length], fmt)
        except ValueError:
            pass
    return None


def _parse_date_str(v: Any) -> str | None:
    d = _parse_date(v)
    return d.strftime("%Y-%m-%d") if d else None


def _in_range(v: Any) -> bool:
    d = _parse_date(v)
    return d is not None and DATE_START <= d <= DATE_END


def _parse_lock_months(raw: Any) -> float | None:
    """
    Convert East Money's LOCKIN_PERIOD string to months.

    Examples:
        "3年"       -> 36.0
        "0.5年"     ->  6.0
        "1年"       -> 12.0
        "12个月"    -> 12.0
        "18个月"    -> 18.0
        "0.5-1年"   ->  9.0  (midpoint of range)
        "6-12个月"  ->  9.0
        "1-3年"     -> 24.0  (midpoint)
    """
    if raw is None:
        return None
    s = str(raw).strip()

    # Range pattern, e.g. "0.5-1年", "6-12个月", "1-3年"
    m = re.match(r"([\d.]+)[-~]([\d.]+)\s*(年|个月)", s)
    if m:
        lo, hi, unit = float(m.group(1)), float(m.group(2)), m.group(3)
        mid = (lo + hi) / 2
        return mid * 12 if unit == "年" else mid

    # Single value in years, e.g. "3年", "0.5年"
    m = re.match(r"([\d.]+)\s*年", s)
    if m:
        return float(m.group(1)) * 12

    # Single value in months, e.g. "12个月", "18个月"
    m = re.match(r"([\d.]+)\s*个月", s)
    if m:
        return float(m.group(1))

    # Bare number (assume months if >5, else years)
    m = re.match(r"^([\d.]+)$", s)
    if m:
        v = float(m.group(1))
        return v * 12 if v <= 5 else v

    return None


# ── East Money scraper ────────────────────────────────────────────────────────

class EastMoneyPIPEScraper:
    """
    Fetches all 定向增发 (SEO_TYPE=1) records from East Money's
    RPT_SEO_DETAIL report and filters to 2010-2024.
    """

    def _get_page(self, page: int) -> dict | None:
        params = {
            "sortColumns": "ISSUE_DATE",
            "sortTypes": "-1",
            "pageSize": PAGE_SIZE,
            "pageNumber": page,
            "reportName": "RPT_SEO_DETAIL",
            "columns": "ALL",
            "filter": '(SEO_TYPE="1")',  # 定向增发 only
            "source": "WEB",
            "client": "WEB",
        }
        for attempt in range(4):
            try:
                r = _SESSION.get(_EM_URL, params=params, timeout=30)
                r.raise_for_status()
                return r.json()
            except Exception as exc:
                logger.warning(
                    "EM page %d attempt %d: %s", page, attempt + 1, exc
                )
                time.sleep(2 ** attempt)
        return None

    @staticmethod
    def _parse_row(row: dict) -> dict | None:
        issue_date = _parse_date_str(row.get("ISSUE_DATE"))
        listing_date = _parse_date_str(row.get("ISSUE_LISTING_DATE"))

        # Keep if issue date OR listing date falls in 2010-2024
        if not (_in_range(issue_date) or _in_range(listing_date)):
            return None

        return {
            "stock_code": row.get("SECURITY_CODE", "").strip(),
            "company_name": row.get("SECURITY_NAME_ABBR", "").strip(),
            "announcement_date": issue_date,
            "listing_date": listing_date,
            "lock_length_months": _parse_lock_months(row.get("LOCKIN_PERIOD")),
            "issue_price": _safe_float(row.get("ISSUE_PRICE")),
            "reference_price": None,   # filled by ReferencePriceCalculator
            "discount_pct": None,      # filled by ReferencePriceCalculator
            "shares_issued": _safe_float(row.get("ISSUE_NUM")),
            "amount_raised_cny": _safe_float(row.get("TOTAL_RAISE_FUNDS")),
            "issue_objects": row.get("ISSUE_OBJECT") or "",
            "source": "eastmoney",
        }

    def fetch(self) -> pd.DataFrame:
        records: list[dict] = []
        page = 1
        total_pages = None

        while True:
            data = self._get_page(page)
            if data is None:
                logger.error("No response at page %d. Stopping.", page)
                break

            result = data.get("result") or {}
            rows = result.get("data") or []

            if total_pages is None:
                total_pages = int(result.get("pages", 1))
                total_count = result.get("count", "?")
                logger.info(
                    "East Money RPT_SEO_DETAIL (定向增发): "
                    "%s total records, %d pages.",
                    total_count,
                    total_pages,
                )

            parsed_page = 0
            for row in rows:
                parsed = self._parse_row(row)
                if parsed:
                    records.append(parsed)
                    parsed_page += 1

            logger.info(
                "  Page %d/%d: %d rows fetched, "
                "%d in 2010-2024 range (%d total so far).",
                page, total_pages, len(rows), parsed_page, len(records),
            )

            if page >= total_pages:
                break
            page += 1
            time.sleep(SLEEP_BTW_PAGES)

        return pd.DataFrame(records) if records else pd.DataFrame()


# ── Reference price calculator ────────────────────────────────────────────────

def _compute_discount(
    ref: float | None, ip: float | None
) -> float | None:
    if ref and ip and ref > 0:
        return round((ref - ip) / ref * 100, 4)
    return None


class ReferencePriceCalculator:
    """
    Uses baostock to fetch historical daily closing prices and compute the
    20-trading-day average before each placement's issue date.

    Formula: reference_price = mean(close) over 20 trading days ending
             on the last trading day before issue_date.
    """

    def __init__(self, lookback_days: int = 20):
        self.lookback_days = lookback_days
        self._bs = None  # cached baostock module

    def _ensure_login(self) -> None:
        if self._bs is None:
            import baostock as bs
            bs.login()
            self._bs = bs

    def _logout(self) -> None:
        if self._bs is not None:
            self._bs.logout()
            self._bs = None

    @staticmethod
    def _baostock_code(code: str) -> str:
        """Convert '600519' -> 'sh.600519', '000858' -> 'sz.000858'."""
        code = str(code).zfill(6)
        return f"sh.{code}" if code.startswith(("6", "9")) else f"sz.{code}"

    def _fetch_prices(self, code: str, end_date: str) -> pd.Series | None:
        """
        Return a Series of closing prices for the 60 calendar days
        ending on end_date (enough to cover 20+ trading days).
        """
        bs_code = self._baostock_code(code)
        start = (
            datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=60)
        ).strftime("%Y-%m-%d")
        rs = self._bs.query_history_k_data_plus(
            bs_code,
            "date,close",
            start_date=start,
            end_date=end_date,
            frequency="d",
            adjustflag="2",  # backward-adjusted
        )
        if rs.error_code != "0":
            return None

        rows = []
        while rs.error_code == "0" and rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            return None

        df = pd.DataFrame(rows, columns=["date", "close"])
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        return df.dropna().set_index("date")["close"]

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add reference_price and discount_pct columns.
        Operates on all rows where announcement_date and issue_price are set.
        """
        self._ensure_login()
        df = df.copy()

        pairs = (
            df[df["announcement_date"].notna() & df["issue_price"].notna()]
            [["stock_code", "announcement_date"]]
            .drop_duplicates()
        )

        cache: dict[tuple[str, str], float | None] = {}
        n = len(pairs)
        logger.info(
            "Computing reference prices for %d unique (stock, date) pairs...",
            n,
        )

        for i, (_, row) in enumerate(pairs.iterrows(), 1):
            key = (row["stock_code"], row["announcement_date"])
            prices = self._fetch_prices(
                row["stock_code"], row["announcement_date"]
            )
            if prices is not None and len(prices) >= 5:
                cache[key] = round(
                    float(prices.iloc[-self.lookback_days:].mean()), 4
                )
            else:
                cache[key] = None

            if i % 50 == 0 or i == n:
                logger.info("  %d/%d reference prices computed.", i, n)
            time.sleep(0.05)  # stay within baostock rate limits

        self._logout()

        # Vectorised assignment via map
        key_series = list(zip(df["stock_code"], df["announcement_date"]))
        df["reference_price"] = [cache.get(k) for k in key_series]
        df["discount_pct"] = [
            _compute_discount(cache.get(k), ip)
            for k, ip in zip(key_series, df["issue_price"])
        ]
        return df


# ── Optional PDF parser (adapted from DeepSeek baseline) ─────────────────────

class PDFParser:
    """
    Parses individual PDF files for lock period, price, and discount.
    Requires:  pip install pdfplumber PyMuPDF
    """

    _PRICE_PAT = [
        r"发行价格[：:]\s*([\d,]+\.?\d*)",
        r"定价[：:]\s*([\d,]+\.?\d*)",
        r"认购价格[：:]\s*([\d,]+\.?\d*)",
    ]
    _REF_PAT = [
        r"基准日前\d+个交易日.*?均价.*?([\d,]+\.?\d*)\s*元",
        r"市价[：:]\s*([\d,]+\.?\d*)",
        r"前\s*\d+\s*个交易日.*?均价.*?([\d,]+\.?\d*)",
    ]
    # (pattern, multiply_by_12_to_convert_years_to_months)
    _LOCK_PAT: list[tuple[str, bool]] = [
        (r"锁定期[为是：:]\s*(\d+)\s*个月", False),
        (r"限售期[为是：:]\s*(\d+)\s*个月", False),
        (r"锁定期[为是：:]\s*([\d.]+)\s*年", True),
    ]

    def __init__(self) -> None:
        try:
            import pdfplumber
            self._pdfplumber = pdfplumber
            self.available = True
        except ImportError:
            self._pdfplumber = None
            self.available = False
            logger.warning("pdfplumber not installed – PDF fallback disabled.")

    def parse(self, pdf_path: str, meta: dict) -> dict:
        if not self.available:
            return meta

        ip = ref = lock = None
        try:
            with self._pdfplumber.open(pdf_path) as pdf:
                for pg in pdf.pages:
                    text = pg.extract_text() or ""
                    for pat in self._PRICE_PAT:
                        m = re.search(pat, text)
                        if m and ip is None:
                            ip = _safe_float(m.group(1))
                    for pat in self._REF_PAT:
                        m = re.search(pat, text)
                        if m and ref is None:
                            ref = _safe_float(m.group(1))
                    for pat, is_years in self._LOCK_PAT:
                        m = re.search(pat, text)
                        if m and lock is None:
                            lock = float(m.group(1)) * (12 if is_years else 1)
        except Exception as exc:
            logger.error("PDF parse error (%s): %s", pdf_path, exc)

        disc = _compute_discount(ref, ip)
        return {
            **meta,
            "issue_price": meta.get("issue_price") or ip,
            "reference_price": meta.get("reference_price") or ref,
            "discount_pct": meta.get("discount_pct") or disc,
            "lock_length_months": meta.get("lock_length_months") or lock,
            "source": (meta.get("source") or "") + "+pdf",
        }


# ── Pipeline ──────────────────────────────────────────────────────────────────

SCHEMA = [
    "stock_code",
    "company_name",
    "announcement_date",
    "listing_date",
    "lock_length_months",
    "issue_price",
    "reference_price",
    "discount_pct",
    "shares_issued",
    "amount_raised_cny",
    "issue_objects",
    "source",
]


def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    nonnull = df.notna().sum(axis=1)
    df = df.iloc[nonnull.sort_values(ascending=False).index]
    return (
        df.drop_duplicates(
            subset=["stock_code", "announcement_date"], keep="first"
        )
        .reset_index(drop=True)
    )


def run_pipeline(
    compute_reference_prices: bool = True,
    output_path: str | None = None,
) -> pd.DataFrame:
    """
    Full pipeline:
      1. Fetch all 定向增发 records from East Money (2010-2024).
      2. Optionally compute reference prices via baostock.
      3. Deduplicate and validate.
      4. Save CSV.

    Parameters
    ----------
    compute_reference_prices
        If True, use baostock to compute 20-day average before issue date.
        Set False for a quick data pull without discount computation.
    output_path
        Where to write the CSV (utf-8-sig encoding for Excel compatibility).
    """
    logger.info("=== Step 1: Fetching East Money PIPE records ===")
    df = EastMoneyPIPEScraper().fetch()
    logger.info("Fetched %d records in date range.", len(df))

    if df.empty:
        logger.warning("No data fetched. Check network.")
        return pd.DataFrame(columns=SCHEMA)

    if compute_reference_prices:
        logger.info(
            "=== Step 2: Computing reference prices via baostock ==="
        )
        df = ReferencePriceCalculator().compute(df)
    else:
        logger.info(
            "Skipping reference price computation "
            "(compute_reference_prices=False)."
        )

    for col in SCHEMA:
        if col not in df.columns:
            df[col] = None
    df = df[SCHEMA]
    df = _deduplicate(df)
    df = (
        df.sort_values(
            ["announcement_date", "stock_code"], na_position="last"
        )
        .reset_index(drop=True)
    )

    # Sanity: drop rows with implausible discount
    if df["discount_pct"].notna().any():
        mask = (
            df["discount_pct"].between(-50, 60, inclusive="both")
            | df["discount_pct"].isna()
        )
        dropped = (~mask).sum()
        if dropped:
            logger.warning(
                "Dropping %d rows with implausible discount_pct.", dropped
            )
        df = df[mask].reset_index(drop=True)

    logger.info(
        "Final: %d records, %s to %s.",
        len(df),
        df["announcement_date"].min(),
        df["announcement_date"].max(),
    )

    if output_path:
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        logger.info("Saved -> %s", output_path)

    return df
