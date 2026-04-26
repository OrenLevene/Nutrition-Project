"""Extract UK products from OFF CSV with error handling."""
import pandas as pd
import csv

print("Loading OFF products (with error handling)...")
try:
    df = pd.read_csv(
        'data/raw/off_products.csv.gz', 
        sep='\t', 
        usecols=['code', 'product_name', 'brands', 'pnns_groups_2', 'countries_tags'],
        on_bad_lines='skip',
        quoting=csv.QUOTE_NONE,
        nrows=500000
    )
    print(f"Loaded {len(df)} products")
    
    # Filter to UK
    uk_df = df[df['countries_tags'].str.contains('united-kingdom', na=False, case=False)]
    print(f"UK products: {len(uk_df)}")
    
    # Save
    uk_df.to_parquet('data/processed/off_uk_products.parquet', index=False)
    print(f"Saved to data/processed/off_uk_products.parquet")
    
except Exception as e:
    print(f"Error: {e}")
