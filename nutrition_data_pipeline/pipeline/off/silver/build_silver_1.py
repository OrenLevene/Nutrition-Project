import pandas as pd
import logging
from pathlib import Path
import sys

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Paths
DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
BRONZE_PATH = DATA_PIPELINE_DIR / 'data' / 'off' / 'bronze' / 'off.csv'
SILVER_PATH = DATA_PIPELINE_DIR / 'data' / 'product' / 'silver' / 'off_silver_1.csv'

# We import the exact same standardization rules used for the reference datasets!
sys.path.insert(0, str(DATA_PIPELINE_DIR / 'pipeline' / 'utils'))
from reference_cleaning import standardize_column_names, parse_weight_to_grams

# The OFF Database stores ALL nutrients in Grams (e.g. 50mg of Vitamin C is 0.05g).
# Our 53-item whitelist expects them in specific mg or ug units.
# We must scale them BEFORE standardizing.
SCALE_TO_MG = [
    'cholesterol_100g', 'vitamin-c_100g', 'vitamin-b1_100g', 'vitamin-b2_100g', 
    'vitamin-pp_100g', 'vitamin-b6_100g', 'pantothenic-acid_100g', 'calcium_100g', 
    'phosphorus_100g', 'iron_100g', 'magnesium_100g', 'zinc_100g', 'copper_100g', 
    'manganese_100g', 'caffeine_100g', 'sodium_100g', 'potassium_100g', 'chloride_100g'
]

SCALE_TO_UG = [
    'vitamin-a_100g', 'beta-carotene_100g', 'vitamin-d_100g', 'vitamin-e_100g', 
    'vitamin-k_100g', 'vitamin-b9_100g', 'folates_100g', 'vitamin-b12_100g', 
    'biotin_100g', 'fluoride_100g', 'selenium_100g', 'iodine_100g'
]

# Map OFF raw names directly to our established Pipeline targets or generic names
# that the reference_cleaning pipeline already knows how to handle!
OFF_MAPPING = {
    'energy-kcal_100g': 'Calories (kcal)',
    'energy_100g': 'Energy (kJ)',
    'energy-kj_100g': 'Energy (kJ)',
    'proteins_100g': 'Protein (g)',
    'carbohydrates_100g': 'Carbohydrate (g)',
    'fat_100g': 'Fat (g)',
    'saturated-fat_100g': 'Saturated Fat (g)',
    'monounsaturated-fat_100g': 'Monounsaturated Fat (g)',
    'polyunsaturated-fat_100g': 'Polyunsaturated Fat (g)',
    'trans-fat_100g': 'Trans Fat (g)',
    'fiber_100g': 'Fiber (g)',
    'sugars_100g': 'Sugars (g)',
    'sodium_100g': 'Sodium (mg)', # After scaling
    'salt_100g': 'Salt (g)',
    
    # Specific Fats
    'omega-3-fat_100g': 'Omega-3 (g)',
    'omega-6-fat_100g': 'Omega-6 (g)',
    'alpha-linolenic-acid_100g': 'ALA (g)',
    'eicosapentaenoic-acid_100g': 'EPA (g)',
    'docosahexaenoic-acid_100g': 'DHA (g)',
    'cholesterol_100g': 'Cholesterol (mg)',
    
    # Vitamins
    'vitamin-c_100g': 'Vitamin C (mg)',
    'vitamin-a_100g': 'Vitamin A (ug)',
    'vitamin-d_100g': 'Vitamin D (ug)',
    'vitamin-e_100g': 'Vitamin E (mg)', # Vitamin E is often mg
    'vitamin-k_100g': 'Vitamin K (ug)',
    'vitamin-b1_100g': 'Thiamin (mg)',
    'vitamin-b2_100g': 'Riboflavin (mg)',
    'vitamin-pp_100g': 'Niacin (mg)',
    'vitamin-b6_100g': 'Vitamin B6 (mg)',
    'vitamin-b9_100g': 'Folate (ug)',
    'folates_100g': 'Folate (ug)',
    'vitamin-b12_100g': 'Vitamin B12 (ug)',
    'biotin_100g': 'Biotin (ug)',
    'pantothenic-acid_100g': 'Pantothenic Acid (mg)',
    
    # Minerals
    'calcium_100g': 'Calcium (mg)',
    'phosphorus_100g': 'Phosphorus (mg)',
    'iron_100g': 'Iron (mg)',
    'magnesium_100g': 'Magnesium (mg)',
    'zinc_100g': 'Zinc (mg)',
    'copper_100g': 'Copper (mg)',
    'manganese_100g': 'Manganese (mg)',
    'selenium_100g': 'Selenium (ug)',
    'iodine_100g': 'Iodine (ug)',
    'potassium_100g': 'Potassium (mg)',
    'chloride_100g': 'Chloride (mg)',
    
    # Extras
    'caffeine_100g': 'Caffeine (mg)',
    'alcohol_100g': 'Alcohol (g)'
}

def main():
    logger.info("=" * 60)
    logger.info("STAGE: SILVER - Open Food Facts")
    logger.info("=" * 60)
    
    if not BRONZE_PATH.exists():
        logger.error(f"Bronze file {BRONZE_PATH} not found. Run build_bronze.py first.")
        return
        
    logger.info("Loading Bronze data...")
    # Use low_memory=False to handle mixed types
    df = pd.read_csv(BRONZE_PATH, low_memory=False)
    
    initial_count = len(df)
    logger.info(f"Loaded {initial_count} products.")
    
    # Step 1: Scale grams to mg/ug for specific columns based on OFF standards
    for col in SCALE_TO_MG:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce') * 1000
            
    # For Vitamin E, OFF stores in grams, we want mg.
    if 'vitamin-e_100g' in df.columns:
          df['vitamin-e_100g'] = pd.to_numeric(df['vitamin-e_100g'], errors='coerce') * 1000
    
    for col in SCALE_TO_UG:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce') * 1000000
    
    # Step 2: Rename columns to the exact target 53-item names directly
    df = df.rename(columns=OFF_MAPPING)
    
    # We must format the metadata so the standardize logic preserves it!
    df = df.rename(columns={
        'code': 'food_id', # OFF barcode becomes the food ID
        'product_name': 'description',
        'brands': 'brand_owner',
        'categories': 'category'
    })
    # Step 2.5: Parse Quantity to Total Weight (g)
    logger.info("Parsing numeric total weights...")
    if 'quantity' in df.columns:
        df['Total Weight (g)'] = df['quantity'].apply(parse_weight_to_grams)
    
    # Step 3: Run the exact same Central Standardization Engine
    df = standardize_column_names(df)
    
    # Step 4: Brand-Name Archetyping (Aggressive Deduplication)
    logger.info("Running Aggressive Brand-Name Archetyping...")
    pre_dedup = len(df)
    
    # Stage A: Drop 100% identical duplicates
    df = df.drop_duplicates()
    
    # Stage B: Group by Brand + Name
    def lists_to_str(lst):
        if isinstance(lst, list):
            # Sort for deterministic output
            clean = [str(i) for i in lst if pd.notna(i) and str(i).strip()]
            return " | ".join(sorted(set(clean))) if clean else pd.NA
        return lst

    # Columns to group by
    group_cols = ['brand_owner', 'description']
    # Ensure they are strings to avoid grouping issues with NaNs
    df[group_cols] = df[group_cols].fillna('Unknown')
    
    # Aggregation rules
    # For metadata, we collect into lists as requested
    agg_rules = {
        'food_id': lambda x: list(set(x)),
        'quantity': lambda x: list(set([i for i in x if pd.notna(i)])),
        'Total Weight (g)': lambda x: list(set([i for i in x if pd.notna(i)])),
        'ingredients_text': 'first',
        'image_url': 'first',
        'image_small_url': 'first',
        'countries_en': lambda x: list(set(x)),
        'category': 'first',
        'main_category_en': 'first'
    }
    
    # Add all remaining columns (nutrients) to agg_rules
    # We assume nutrition matches if brand + name matches.
    meta_keys = list(agg_rules.keys()) + group_cols
    for col in df.columns:
        if col not in meta_keys:
            agg_rules[col] = 'first'
            
    logger.info(f"Collapsing {pre_dedup:,} products into Brand-Name archetypes...")
    df = df.groupby(group_cols, dropna=False).agg(agg_rules).reset_index()
    
    # Re-flatten lists into pipe-separated strings
    for col in ['food_id', 'quantity', 'Total Weight (g)', 'countries_en']:
        if col in df.columns:
            df[col] = df[col].apply(lists_to_str)
            
    post_dedup = len(df)
    logger.info(f"Deduplication removed {pre_dedup - post_dedup} exact clones! Remaining: {post_dedup}")
    
    # Step 5: Save Silver Output
    SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(SILVER_PATH, index=False)
    logger.info(f"OFF Silver successfully saved to {SILVER_PATH}")

if __name__ == '__main__':
    main()
