import pandas as pd
from pathlib import Path
import ast
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Paths
DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
SILVER_1_PATH = DATA_PIPELINE_DIR / 'data' / 'product' / 'silver' / 'off_silver_1.csv'
SILVER_2_PATH = DATA_PIPELINE_DIR / 'data' / 'product' / 'silver' / 'off_silver_2.csv'

def lists_to_str(lst):
    """Safely converts list payloads into string representations so Pandas doesn't crash."""
    if isinstance(lst, list):
        # Remove nans or non-strings from list output for cleanliness
        clean = [str(i) for i in lst if pd.notna(i) and str(i).strip()]
        return " | ".join(sorted(set(clean))) if clean else pd.NA
    return lst

def build_silver_layer_2():
    logger.info("============================================================")
    logger.info("STAGE: SILVER L2 (Quantitative Deduplication) - Open Food Facts")
    logger.info("============================================================")
    
    if not SILVER_1_PATH.exists():
        logger.error(f"Silver 1 data not found at {SILVER_1_PATH}")
        return
        
    logger.info("Loading Silver 1 data...")
    df = pd.read_csv(SILVER_1_PATH, low_memory=False)
    
    initial_count = len(df)
    logger.info(f"Loaded {initial_count} pristine standardized products.")
    
    # Identify our strictly numeric nutrient columns (Exclude Total Weight so we can agg it!)
    numeric_nutrients = [c for c in df.select_dtypes(include='number').columns.tolist() if c != 'Total Weight (g)']
    
    # We group by EVERY single numeric value + primary category
    # (If two foods have slightly different Vitamin C, they are distinct!)
    group_cols = numeric_nutrients + ['main_category_en']
    
    # We want to mathematically prove missing data === missing data so they group perfectly
    # Fill NAs with a static string so Pandas groupby doesn't throw them out 
    logger.info("Prepping analytical signatures...")
    df[group_cols] = df[group_cols].fillna('N/A')
    
    # Define aggregation rules for metadata:
    # We collect all distinct brands and names into lists!
    agg_rules = {
        'description': lambda x: list(set(x)),
        'brand_owner': lambda x: list(set(x)),
        'food_id': lambda x: list(set(x)),       # Keep all barcodes
        'Total Weight (g)': lambda x: list(set([i for i in x if pd.notna(i)])),
        'ingredients_text': 'first',             # Just take one reference ingredient list
        'image_url': 'first',
        'image_small_url': 'first',
        'countries_en': lambda x: list(set(x))
    }
    
    # For any other string columns not in our agg rules, just grab the first valid one
    for col in df.columns:
        if col not in group_cols and col not in agg_rules:
            agg_rules[col] = 'first'
            
    logger.info("Running massive Macro-Signature Grouping Engine... (this takes 1-2 minutes)")
    collapsed_df = df.groupby(group_cols, dropna=False).agg(agg_rules).reset_index()
    
    # Restore N/A back to actual nulls
    collapsed_df = collapsed_df.replace('N/A', pd.NA)
    
    # Format lists into clean padded strings ("Tesco | Sainsbury | Asda")
    logger.info("Formatting aggregated metadata lists...")
    for col in ['description', 'brand_owner', 'food_id', 'countries_en']:
        if col in collapsed_df.columns:
            collapsed_df[col] = collapsed_df[col].apply(lists_to_str)
            
    # Save the output
    SILVER_2_PATH.parent.mkdir(parents=True, exist_ok=True)
    collapsed_df.to_csv(SILVER_2_PATH, index=False)
    
    final_count = len(collapsed_df)
    shrunk = initial_count - final_count
    
    logger.info(f"Silver L2 Multi-Branded Shrink Complete!")
    logger.info(f"Original items: {initial_count:,}")
    logger.info(f"Collapsed Generic Signatures: {final_count:,}")
    logger.info(f"Successfully collapsed {shrunk:,} strictly-redundant variations!")
    logger.info(f"Saved to {SILVER_2_PATH}")

if __name__ == "__main__":
    build_silver_layer_2()
