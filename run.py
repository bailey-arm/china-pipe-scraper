"""
CLI runner for the China PIPE scraper.

Usage
-----
# Full run with reference prices (slow — one baostock call per stock)
python run.py

# Skip reference price computation (fast raw pull)
python run.py --no-ref-prices

# Custom output path
python run.py --output my_pipes.csv

# Custom date range
python run.py --start 2015-01-01 --end 2019-12-31
"""

import argparse
import os
import sys

from scraper import run_pipeline, DATE_START, DATE_END


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape China PIPE (定向增发) data from East Money."
    )
    parser.add_argument(
        "--no-ref-prices",
        action="store_true",
        help="Skip reference price computation via baostock (faster).",
    )
    parser.add_argument(
        "--output",
        default="china_pipes.csv",
        help="Output CSV path (default: china_pipes.csv).",
    )
    args = parser.parse_args()

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)

    df = run_pipeline(
        compute_reference_prices=not args.no_ref_prices,
        output_path=args.output,
    )

    if df.empty:
        print("No data retrieved. Check network access and logs above.")
        sys.exit(1)

    print(f"\n{'=' * 60}")
    print(f"Records collected : {len(df):,}")
    print(f"Date range        : {df['announcement_date'].min()} → {df['announcement_date'].max()}")
    print(f"Unique tickers    : {df['stock_code'].nunique():,}")
    print(f"Discount coverage : {df['discount_pct'].notna().sum():,} / {len(df):,}")
    print(f"Lock-up coverage  : {df['lock_length_months'].notna().sum():,} / {len(df):,}")
    print(f"\nSaved to          : {os.path.abspath(args.output)}")
    print("=" * 60)

    print("\nSample (first 5 rows):")
    print(
        df[["stock_code", "company_name", "announcement_date",
            "lock_length_months", "discount_pct", "amount_raised_cny"]].head(5).to_string(index=False)
    )


if __name__ == "__main__":
    main()
