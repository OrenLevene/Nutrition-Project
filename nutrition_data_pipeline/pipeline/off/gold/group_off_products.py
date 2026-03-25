"""
OFF Product Grouping - Phase 1: Fuzzy Matching

Groups products by normalized name + nutrition tolerance.
Stores all brands/supermarkets per group.
"""
import pandas as pd
import re
import unicodedata
from rapidfuzz import fuzz
from collections import defaultdict

# Configuration
CALORIE_TOLERANCE = 5  # ±5 kcal
MACRO_TOLERANCE = 0.5  # ±0.5g

# Major UK supermarket brands
SUPERMARKET_BRANDS = {
    'tesco', 'sainsbury', 'sainsburys', 'asda', 'morrisons', 'morrison',
    'waitrose', 'aldi', 'lidl', 'coop', 'co-op', 'marks', 'spencer', 'm&s',
    'iceland', 'ocado', 'spar', 'budgens', 'londis', 'nisa'
}

def normalize_name(name: str) -> str:
    """Normalize product name for matching."""
    if pd.isna(name):
        return ""
    # Lowercase
    name = name.lower()
    # Remove unicode accents
    name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode()
    # Remove punctuation except spaces
    name = re.sub(r'[^\w\s]', '', name)
    # Collapse whitespace
    name = ' '.join(name.split())
    return name

def extract_brand(description: str) -> str:
    """Extract brand from description (after last ' - ')."""
    if pd.isna(description):
        return "Unknown"
    parts = description.split(' - ')
    if len(parts) > 1:
        return parts[-1].strip()
    return "Unknown"

def is_supermarket(brand: str) -> bool:
    """Check if brand is a UK supermarket."""
    brand_lower = brand.lower()
    return any(s in brand_lower for s in SUPERMARKET_BRANDS)

def get_supermarkets(brand: str) -> list:
    """Extract supermarket names from brand."""
    brand_lower = brand.lower()
    found = []
    if 'tesco' in brand_lower:
        found.append('Tesco')
    if 'sainsbury' in brand_lower:
        found.append("Sainsbury's")
    if 'asda' in brand_lower:
        found.append('Asda')
    if 'morrison' in brand_lower:
        found.append('Morrisons')
    if 'waitrose' in brand_lower:
        found.append('Waitrose')
    if 'aldi' in brand_lower:
        found.append('Aldi')
    if 'lidl' in brand_lower:
        found.append('Lidl')
    if 'coop' in brand_lower or 'co-op' in brand_lower:
        found.append('Co-op')
    if 'm&s' in brand_lower or 'marks' in brand_lower or 'spencer' in brand_lower:
        found.append('M&S')
    return found

def nutrition_matches(row1, row2) -> bool:
    """Check if two products have matching nutrition within tolerance."""
    if abs(row1['calories'] - row2['calories']) > CALORIE_TOLERANCE:
        return False
    if abs(row1['protein'] - row2['protein']) > MACRO_TOLERANCE:
        return False
    if abs(row1['carbohydrate'] - row2['carbohydrate']) > MACRO_TOLERANCE:
        return False
    if abs(row1['fat'] - row2['fat']) > MACRO_TOLERANCE:
        return False
    return True

def create_nutrition_key(row) -> str:
    """Create a nutrition fingerprint for initial grouping."""
    # Round to create buckets
    cal = int(row['calories'] // 10) * 10  # Round to nearest 10
    prot = round(row['protein'], 0)
    carb = round(row['carbohydrate'], 0)
    fat = round(row['fat'], 0)
    return f"{cal}|{prot}|{carb}|{fat}"

# Load data
print("Loading OFF UK products...")
df = pd.read_parquet('data/processed/SILVER_OFF_deduped.parquet')
print(f"Loaded {len(df)} products")

# Extract product name and brand
print("\nExtracting names and brands...")
df['brand'] = df['description'].apply(extract_brand)
df['product_name'] = df['description'].str.split(' - ').str[:-1].str.join(' - ')
df['product_name'] = df['product_name'].fillna(df['description'])
df['name_normalized'] = df['product_name'].apply(normalize_name)
df['nutrition_key'] = df.apply(create_nutrition_key, axis=1)

# Count nutrient coverage for canonical selection
nutrient_cols = [col for col in df.columns if col not in 
                 ['food_id', 'description', 'data_source', 'food_group', 
                  'brand', 'product_name', 'name_normalized', 'nutrition_key',
                  'calories', 'protein', 'carbohydrate', 'fat']]
df['nutrient_coverage'] = df[nutrient_cols].notna().sum(axis=1)

# Phase 1: Group by exact normalized name + nutrition key
print("\nPhase 1: Grouping by exact normalized name + nutrition...")
df['group_key'] = df['name_normalized'] + '|' + df['nutrition_key']

groups = []
group_id = 0

for key, group_df in df.groupby('group_key'):
    # For each group, collect all members
    members = group_df.to_dict('records')
    
    # Sort by nutrient coverage to pick canonical
    members.sort(key=lambda x: -x['nutrient_coverage'])
    canonical = members[0]
    
    # Collect all brands and supermarkets
    all_brands = list(set(m['brand'] for m in members))
    all_supermarkets = []
    for m in members:
        all_supermarkets.extend(get_supermarkets(m['brand']))
    all_supermarkets = list(set(all_supermarkets))
    
    groups.append({
        'group_id': group_id,
        'canonical_name': canonical['description'],
        'canonical_food_id': canonical['food_id'],
        'product_name': canonical['product_name'],
        'brands': all_brands,
        'supermarkets': all_supermarkets,
        'member_food_ids': [m['food_id'] for m in members],
        'member_count': len(members),
        'calories': canonical['calories'],
        'protein': canonical['protein'],
        'carbohydrate': canonical['carbohydrate'],
        'fat': canonical['fat'],
    })
    group_id += 1

# Convert to DataFrame
groups_df = pd.DataFrame(groups)

# Save first (before any unicode output issues)
output_path = 'data/processed/SILVER_OFF_grouped.parquet'
groups_df.to_parquet(output_path, index=False)

# Also save a products->group mapping
mapping = []
for _, g in groups_df.iterrows():
    for fid in g['member_food_ids']:
        mapping.append({'food_id': fid, 'group_id': g['group_id']})
mapping_df = pd.DataFrame(mapping)
mapping_df.to_parquet('data/processed/SILVER_OFF_group_map.parquet', index=False)

# Summary
print("\n" + "="*60)
print("PHASE 1 RESULTS")
print("="*60)
print(f"Total products: {len(df)}")
print(f"Total groups: {len(groups_df)}")
print(f"Groups with >1 member (duplicates): {len(groups_df[groups_df['member_count'] > 1])}")
print(f"Products in duplicate groups: {groups_df[groups_df['member_count'] > 1]['member_count'].sum()}")
print(f"\nSaved {len(groups_df)} groups to {output_path}")
print(f"Saved product->group mapping")
