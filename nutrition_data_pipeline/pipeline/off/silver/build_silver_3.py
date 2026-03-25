import pandas as pd
from pathlib import Path
import logging
import re

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Paths
DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
SILVER_2_PATH = DATA_PIPELINE_DIR / 'data' / 'product' / 'silver' / 'off_silver_2.csv'
SILVER_3_PATH = DATA_PIPELINE_DIR / 'data' / 'product' / 'silver' / 'off_silver_3.csv'

def normalize_name(name):
    """Normalize names for high-accuracy matching: strip units, digits, noise."""
    if pd.isna(name): return "EMPTY"
    # To string, lowercase
    name = str(name).lower()
    # Strip common weight/volume suffixes (e.g., 500g, 1kg, 250ml)
    # This also removes digits to help match "Passata x2" with "Passata"
    name = re.sub(r'\d+\s*(g|kg|ml|l|oz|lb|pc|pack|kcal|kj)\b', ' ', name)
    name = re.sub(r'\d+', ' ', name)
    # Remove punctuation/special chars
    name = re.sub(r'[^a-z ]', '', name)
    # Collapse multiple spaces
    name = re.sub(r'\s+', ' ', name).strip()
    return name if name else "EMPTY"

def lists_to_str(lst):
    """Safely converts list payloads into string representations."""
    if isinstance(lst, list):
        clean = [str(i) for i in lst if pd.notna(i) and str(i).strip()]
        return " | ".join(sorted(set(clean))) if clean else pd.NA
    return lst

def build_silver_layer_3():
    logger.info("============================================================")
    logger.info("STAGE: SILVER L3 (Name + 1DP Nutrition Match) - Open Food Facts")
    logger.info("============================================================")
    
    if not SILVER_2_PATH.exists():
        logger.error(f"Silver 2 data not found at {SILVER_2_PATH}")
        return
        
    logger.info("Loading Silver 2 data...")
    # Silver 2 already has descriptions as strings/lists, we need to handle that
    df = pd.read_csv(SILVER_2_PATH, low_memory=False)
    
    initial_count = len(df)
    logger.info(f"Loaded {initial_count:,} Archetypes from Silver 2.")
    
    # Step 1: Normalize names for high-accuracy grouping
    logger.info("Normalizing product names for semantic matching...")
    # Note: 'description' in Silver 2 might be the pipe-separated string from L2 aggregation
    # We'll take the first part of the description for the match key
    df['norm_name'] = df['description'].fillna('').apply(lambda x: normalize_name(x.split(' | ')[0]))
    
    # Step 2: Round all nutrients to 1 Decimal Place
    logger.info("Rounding all nutrition to 1 Decimal Place (1DP)...")
    nut_cols = [c for c in df.select_dtypes(include='number').columns.tolist() if c != 'Total Weight (g)']
    df[nut_cols] = df[nut_cols].round(1)
    
    # Step 3: Define Grouping Signature
    # Matching by Normalized Name + 1DP Nutrition + Category
    group_cols = ['norm_name', 'main_category_en'] + nut_cols
    
    # Handle NaNs in group cols
    df[group_cols] = df[group_cols].fillna('N/A')
    
    # Step 4: Aggressive Grouping
    logger.info("Collapsing by Name + 1DP Signature...")
    agg_rules = {
        'description': lambda x: list(set(x)),
        'brand_owner': lambda x: list(set(x)),
        'food_id': lambda x: list(set(x)),
        'Total Weight (g)': lambda x: list(set([i for i in x if pd.notna(i)])),
        'ingredients_text': 'first',
        'image_url': 'first',
        'image_small_url': 'first',
        'countries_en': lambda x: list(set(x))
    }
    
    # For any other string columns, just grab first
    for col in df.columns:
        if col not in group_cols and col not in agg_rules and col != 'norm_name':
            agg_rules[col] = 'first'
            
    collapsed_df = df.groupby(group_cols, dropna=False).agg(agg_rules).reset_index()
    
    # Restore N/A
    collapsed_df = collapsed_df.replace('N/A', pd.NA)
    
    # Re-flatten lists
    logger.info("Re-flattening metadata lists...")
    # Helper to flatten nested lists if L2 already put lists in there
    def flatten_and_join(x):
        all_items = []
        for item in x:
            if isinstance(item, str) and ' | ' in item:
                all_items.extend(item.split(' | '))
            else:
                all_items.append(str(item))
        clean = [i.strip() for i in all_items if pd.notna(i) and str(i).strip()]
        return " | ".join(sorted(set(clean))) if clean else pd.NA

    for col in ['description', 'brand_owner', 'food_id', 'countries_en', 'Total Weight (g)']:
        collapsed_df[col] = collapsed_df[col].apply(flatten_and_join)
    
    # Drop norm_name
    collapsed_df = collapsed_df.drop(columns=['norm_name'])
    
    # Save
    SILVER_3_PATH.parent.mkdir(parents=True, exist_ok=True)
    collapsed_df.to_csv(SILVER_3_PATH, index=False)
    
    final_count = len(collapsed_df)
    logger.info(f"Silver L3 High-Accuracy Match Complete!")
    logger.info(f"Silver 2 Archetypes: {initial_count:,}")
    logger.info(f"Silver 3 Archetypes: {final_count:,}")
    logger.info(f"Successfully collapsed an additional {initial_count - final_count:,} rows!")
    logger.info(f"Saved to {SILVER_3_PATH}")

if __name__ == "__main__":
    build_silver_layer_3()
