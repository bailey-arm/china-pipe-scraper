# china-pipe-scraper

Scrapes China private placement (定向增发 / PIPE) data from East Money and computes placement discounts via baostock. Covers 2010–2024.

## Output schema

| Column | Description |
|---|---|
| `stock_code` | A-share ticker, e.g. `600519` |
| `company_name` | Chinese short name |
| `announcement_date` | Issue date |
| `listing_date` | Date new shares started trading |
| `lock_length_months` | Lock-up period in months |
| `issue_price` | Price per share placed (CNY) |
| `reference_price` | 20-trading-day avg close before issue date (CNY) |
| `discount_pct` | `(reference − issue) / reference × 100` (positive = discount) |
| `shares_issued` | Total shares placed |
| `amount_raised_cny` | Gross proceeds (CNY) |
| `issue_objects` | Names of placement recipients |
| `source` | Data provenance tag |

## Sample data

`data/china_pipes_2010_2011.csv` — 174 placements from 2010–2011 with reference prices pre-computed.

## Install

```bash
pip install -r requirements.txt
```

`baostock` is required for reference price computation. `pdfplumber` is optional (PDF fallback).

## Usage

### CLI

```bash
# full run — fetches East Money data + computes reference prices via baostock
python run.py

# fast raw pull — skips reference price computation
python run.py --no-ref-prices

# custom output path
python run.py --output my_pipes.csv
```

### Python

```python
from scraper import run_pipeline, EastMoneyPIPEScraper, ReferencePriceCalculator

# full pipeline
df = run_pipeline(output_path="china_pipes.csv")

# step by step — fetch only
raw = EastMoneyPIPEScraper().fetch()

# narrow to a date range, then compute reference prices
mask = raw['announcement_date'].between('2015-01-01', '2020-12-31')
df = raw[mask].copy().reset_index(drop=True)
df = ReferencePriceCalculator(lookback_days=20).compute(df)
```

See `demo.ipynb` for a full walkthrough with charts.

## Data sources

- **East Money** (`datacenter-web.eastmoney.com`) — placement records via `RPT_SEO_DETAIL` (SEO_TYPE=1)
- **baostock** — historical daily closing prices for reference price computation

## Notes

- The scraper rate-limits itself (0.6s between pages, 0.05s between baostock calls).
- Reference prices use backward-adjusted closes (`adjustflag="2"` in baostock).
- Rows with `discount_pct` outside [−50%, +60%] are flagged/dropped as implausible.
