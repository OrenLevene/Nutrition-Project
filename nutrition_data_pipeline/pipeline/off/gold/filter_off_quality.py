"""
Filter OFF UK products to only those with:
- completeness > 0.5
- all 4 core macros filled (calories, protein, carbs, fat)
- UK country
"""
import pandas as pd

# Load current data
print("Loading OFF UK products...")
df = pd.read_parquet('data/processed/BRONZE_OFF_raw.parquet')
print(f"Total products loaded: {len(df)}")

# Define required columns
REQUIRED_MACROS = ['energy-kcal_100g', 'proteins_100g', 'carbohydrates_100g', 'fat_100g']

# Apply filters
print("\nApplying filters...")

# 1. Completeness > 0.5
before = len(df)
df = df[df['completeness'] > 0.5]
print(f"  Completeness > 0.5: {before} -> {len(df)}")

# 2. All 4 macros filled (not null and >= 0)
before = len(df)
for macro in REQUIRED_MACROS:
    df = df[df[macro].notna() & (df[macro] >= 0)]
print(f"  All 4 macros filled: {before} -> {len(df)}")

# 3. UK is already filtered in this dataset, but double-check
if 'countries_tags' in df.columns:
    before = len(df)
    df = df[df['countries_tags'].str.contains('united-kingdom', case=False, na=False)]
    print(f"  UK only: {before} -> {len(df)}")

# Save the filtered dataset
output_path = 'data/processed/SILVER_OFF_quality.parquet'
df.to_parquet(output_path, index=False)
print(f"\n✅ Saved {len(df)} quality UK products to {output_path}")

# Summary
print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print(f"Products: {len(df)}")
print(f"Columns: {list(df.columns)}")
print(f"\nMacro stats (per 100g):")
for macro in REQUIRED_MACROS:
    print(f"  {macro}: mean={df[macro].mean():.1f}, min={df[macro].min():.1f}, max={df[macro].max():.1f}")

print(f"\nSample products:")
for _, row in df.head(5).iterrows():
    name = row['product_name'][:50] if pd.notna(row['product_name']) else 'N/A'
    brand = row['brands'][:20] if pd.notna(row['brands']) else ''
    kcal = row['energy-kcal_100g']
    print(f"  - {name} ({brand}) - {kcal:.0f} kcal")
