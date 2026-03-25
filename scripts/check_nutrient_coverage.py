"""Check actual micronutrient coverage in the processed data."""
import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

# Check all relevant data files
files = [
    'data/processed/BRONZE_REF_usda.parquet',
    'data/processed/BRONZE_REF_foodb_cofid_raw.parquet',
    'data/processed/SILVER_OFF_quality.parquet',
    'data/processed/SILVER_OFF_semantic.parquet',
]

for f in files:
    try:
        df = pd.read_parquet(f)
        print(f"\n{'='*60}")
        print(f"{f}")
        print(f"{'='*60}")
        print(f"Shape: {df.shape}")
        print(f"\nColumns ({len(df.columns)}):")
        for col in sorted(df.columns):
            non_null = df[col].notna().sum()
            pct = non_null / len(df) * 100
            if pct > 0:
                print(f"  {col:<35} {non_null:>6}/{len(df)} ({pct:5.1f}%)")
    except Exception as e:
        print(f"\n{f}: ERROR - {e}")
