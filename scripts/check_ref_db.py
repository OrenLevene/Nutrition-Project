"""Quick check of MERGED_FOODB_COFID_DB structure."""
import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

df = pd.read_parquet('data/processed/BRONZE_REF_foodb_cofid_raw.parquet')
print(f"Shape: {df.shape}")

# Show the description/name columns
name_cols = [c for c in df.columns if any(x in c.lower() for x in ['name', 'desc', 'food_id', 'group', 'type', 'code', 'source'])]
print(f"\nIdentifier columns:")
for c in name_cols:
    print(f"  {c}: {df[c].notna().sum()}/{len(df)} | examples: {df[c].dropna().head(3).tolist()}")

# Show the main nutrient columns (non-fatty-acid ones)
nutrient_cols = [c for c in df.columns if c not in name_cols 
                 and 'acid' not in c.lower()
                 and 'branch' not in c.lower()
                 and 'unknown' not in c.lower()
                 and 'trans' not in c.lower()
                 and '100g f' not in c.lower()]  # Skip fatty acid details
print(f"\nMain nutrient columns ({len(nutrient_cols)}):")
for c in sorted(nutrient_cols):
    non_null = df[c].notna().sum()
    pct = non_null / len(df) * 100
    if pct > 30:  # Only show well-populated ones
        print(f"  {c:<45} {non_null:>5}/{len(df)} ({pct:5.1f}%)")
