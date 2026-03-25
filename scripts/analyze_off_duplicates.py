"""
Analyze duplication patterns in OFF UK products.
"""
import pandas as pd
from collections import Counter

df = pd.read_parquet('data/processed/SILVER_OFF_quality.parquet')
print(f"Total products: {len(df)}")

# Extract product name and brand from description
# Format is typically "Product Name - Brand"
df['name_brand'] = df['description'].str.strip()

# 1. Exact duplicate descriptions
print("\n" + "="*60)
print("1. EXACT DUPLICATE DESCRIPTIONS")
print("="*60)
exact_dupes = df['description'].value_counts()
exact_dupes_multi = exact_dupes[exact_dupes > 1]
print(f"Unique descriptions: {len(exact_dupes)}")
print(f"Descriptions appearing >1 time: {len(exact_dupes_multi)}")
print(f"Total products in duplicates: {exact_dupes_multi.sum()}")
print("\nTop 10 duplicated descriptions:")
for desc, count in exact_dupes_multi.head(10).items():
    print(f"  ({count}x) {desc[:70]}")

# 2. Normalize and check again
print("\n" + "="*60)
print("2. NORMALIZED DUPLICATES (lowercase, stripped)")
print("="*60)
df['desc_normalized'] = df['description'].str.lower().str.strip()
norm_dupes = df['desc_normalized'].value_counts()
norm_dupes_multi = norm_dupes[norm_dupes > 1]
print(f"Unique normalized: {len(norm_dupes)}")
print(f"Normalized appearing >1 time: {len(norm_dupes_multi)}")

# 3. Check by food_group + similar macros
print("\n" + "="*60)
print("3. PRODUCTS WITH IDENTICAL MACROS (within same food group)")
print("="*60)
# Round macros to 1 decimal for grouping
df['macro_key'] = (
    df['food_group'].fillna('Unknown').astype(str) + '|' +
    df['calories'].round(0).astype(str) + '|' +
    df['protein'].round(1).astype(str) + '|' +
    df['carbohydrate'].round(1).astype(str) + '|' +
    df['fat'].round(1).astype(str)
)
macro_dupes = df['macro_key'].value_counts()
macro_dupes_multi = macro_dupes[macro_dupes > 1]
print(f"Unique macro profiles: {len(macro_dupes)}")
print(f"Macro profiles with >1 product: {len(macro_dupes_multi)}")
print(f"Products sharing a macro profile: {macro_dupes_multi.sum()}")

# Show some examples
print("\nExample duplicate macro groups:")
for key in list(macro_dupes_multi.head(5).index):
    group = df[df['macro_key'] == key]
    print(f"\n  Group ({len(group)} products): {key.split('|')[0][:30]}")
    for _, row in group.head(3).iterrows():
        print(f"    - {row['description'][:60]}")

# 4. Potential brand duplicates (same brand, similar names)
print("\n" + "="*60)
print("4. BRAND ANALYSIS")
print("="*60)
# Extract brand (after last ' - ')
df['brand'] = df['description'].str.split(' - ').str[-1].str.strip()
brand_counts = df['brand'].value_counts()
print(f"Unique brands: {len(brand_counts)}")
print("\nTop 15 brands by product count:")
for brand, count in brand_counts.head(15).items():
    print(f"  {count:5} - {brand[:50]}")

# 5. Summary
print("\n" + "="*60)
print("SUMMARY: DEDUPLICATION POTENTIAL")
print("="*60)
print(f"Total products: {len(df)}")
print(f"Exact duplicates to remove: {exact_dupes_multi.sum() - len(exact_dupes_multi)}")
print(f"Products with shared macro profiles: {macro_dupes_multi.sum()}")
