"""
Deduplicate OFF UK products using a 3-step strategy:
1. Remove exact description duplicates
2. Normalize brand names
3. Keep one representative per brand + category + macro profile
"""
import pandas as pd
import re
from rapidfuzz import fuzz

df = pd.read_parquet('data/processed/SILVER_OFF_quality.parquet')
print(f"Starting products: {len(df)}")

# ============================================================
# STEP 1: Remove exact description duplicates
# ============================================================
print("\n" + "="*60)
print("STEP 1: Remove exact description duplicates")
print("="*60)

before = len(df)
# Keep first occurrence (could also keep highest completeness if available)
df = df.drop_duplicates(subset=['description'], keep='first')
print(f"  {before} -> {len(df)} (removed {before - len(df)} exact duplicates)")

# ============================================================
# STEP 2: Normalize brand names
# ============================================================
print("\n" + "="*60)
print("STEP 2: Normalize brand names")
print("="*60)

# Extract brand from description (after last ' - ')
df['brand_raw'] = df['description'].str.split(' - ').str[-1].str.strip()

# Brand normalization mapping
BRAND_MAPPINGS = {
    # M&S variants
    r"^m\s*&\s*s$": "Marks & Spencer",
    r"^m&s\s*food$": "Marks & Spencer",
    r"^marks\s*(and|&)\s*spencer.*": "Marks & Spencer",
    
    # Sainsbury's variants
    r"^by\s*sainsbury'?s?$": "Sainsbury's",
    r"^sainsbury'?s?$": "Sainsbury's",
    
    # Tesco variants
    r"^tesco\s*(finest|everyday\s*value)?$": "Tesco",
    
    # Aldi variants
    r"^aldi\s*.*$": "Aldi",
    
    # Lidl variants
    r"^lidl\s*.*$": "Lidl",
    
    # Co-op variants
    r"^co-?op$": "Co-op",
    r"^coop$": "Co-op",
    
    # Asda variants
    r"^asda\s*(extra\s*special)?$": "Asda",
    
    # Morrisons variants
    r"^morrisons?\s*.*$": "Morrisons",
    
    # Waitrose variants
    r"^waitrose\s*.*$": "Waitrose",
}

def normalize_brand(brand):
    if pd.isna(brand):
        return "Unknown"
    brand_lower = brand.lower().strip()
    for pattern, normalized in BRAND_MAPPINGS.items():
        if re.match(pattern, brand_lower):
            return normalized
    return brand.strip()

df['brand'] = df['brand_raw'].apply(normalize_brand)

# Show brand consolidation
brand_counts = df['brand'].value_counts()
print(f"  Unique brands after normalization: {len(brand_counts)}")
print("\n  Top 10 brands:")
for brand, count in brand_counts.head(10).items():
    print(f"    {count:5} - {brand}")

# ============================================================
# STEP 3: Keep one per brand + category + macro profile
# ============================================================
print("\n" + "="*60)
print("STEP 3: Deduplicate by brand + category + macros")
print("="*60)

# Create a grouping key: brand + food_group + rounded macros
df['macro_key'] = (
    df['brand'].fillna('Unknown') + '|' +
    df['food_group'].fillna('Unknown').astype(str) + '|' +
    df['calories'].round(0).astype(int).astype(str) + '|' +
    df['protein'].round(1).astype(str) + '|' +
    df['carbohydrate'].round(1).astype(str) + '|' +
    df['fat'].round(1).astype(str)
)

before = len(df)
# For duplicates, prefer entries with more filled nutrient columns
nutrient_cols = [col for col in df.columns if col not in 
                 ['food_id', 'description', 'data_source', 'food_group', 
                  'brand', 'brand_raw', 'macro_key', 'calories', 'protein', 
                  'carbohydrate', 'fat']]
df['nutrient_coverage'] = df[nutrient_cols].notna().sum(axis=1)

# Sort by coverage (desc) so we keep the most complete entry
df = df.sort_values('nutrient_coverage', ascending=False)
df = df.drop_duplicates(subset=['macro_key'], keep='first')

print(f"  {before} -> {len(df)} (removed {before - len(df)} macro duplicates)")

# ============================================================
# CLEANUP AND SAVE
# ============================================================
print("\n" + "="*60)
print("CLEANUP AND SAVE")
print("="*60)

# Drop temporary columns
df = df.drop(columns=['brand_raw', 'macro_key', 'nutrient_coverage'])

# Reset index
df = df.reset_index(drop=True)

# Save
output_path = 'data/processed/SILVER_OFF_deduped.parquet'
df.to_parquet(output_path, index=False)
print(f"Saved {len(df)} deduplicated products to {output_path}")

# Final summary
print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print(f"Original products: 42,283")
print(f"After deduplication: {len(df)}")
print(f"Reduction: {42283 - len(df)} products ({(42283 - len(df))/42283*100:.1f}%)")

# Sample check
print("\nSample products:")
for _, row in df.head(5).iterrows():
    print(f"  - {row['description'][:60]} ({row['brand']})")
