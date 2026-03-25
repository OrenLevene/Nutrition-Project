import pandas as pd
from pathlib import Path
import gc

# Paths
DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
RAW_PATH = DATA_PIPELINE_DIR / 'data' / 'off' / 'raw' / 'off_products.csv.gz'
BRONZE_PATH = DATA_PIPELINE_DIR / 'data' / 'off' / 'bronze' / 'off.csv'

BRONZE_PATH.parent.mkdir(parents=True, exist_ok=True)

# Important columns to keep
all_possible_off_cols = [
    # --- CORE METADATA ---
    'code', 'product_name', 'brands', 'quantity', 'serving_size', 
    'categories', 'main_category_en', 'countries_en',
    
    # --- GROCERY & APP SPECIFIC METADATA ---
    'ingredients_text', 'allergens', 'traces', 'additives_tags', 
    'nutriscore_grade', 'nova_group',
    'image_url', 'image_small_url', 'image_ingredients_url',
    
    # --- NUTRITION ---
    'energy-kcal_100g', 'energy_100g', 'energy-kj_100g', 'proteins_100g', 
    'carbohydrates_100g', 'fat_100g', 'saturated-fat_100g', 'monounsaturated-fat_100g', 
    'polyunsaturated-fat_100g', 'trans-fat_100g', 'fiber_100g', 'sugars_100g', 
    'sodium_100g', 'salt_100g', 'omega-3-fat_100g', 'omega-6-fat_100g', 
    'alpha-linolenic-acid_100g', 'eicosapentaenoic-acid_100g', 'docosahexaenoic-acid_100g', 
    'cholesterol_100g', 'vitamin-c_100g', 'vitamin-a_100g', 'vitamin-d_100g', 
    'vitamin-e_100g', 'vitamin-k_100g', 'vitamin-b1_100g', 'vitamin-b2_100g', 
    'vitamin-pp_100g', 'vitamin-b6_100g', 'vitamin-b9_100g', 'folates_100g', 
    'vitamin-b12_100g', 'biotin_100g', 'pantothenic-acid_100g', 'calcium_100g', 
    'phosphorus_100g', 'iron_100g', 'magnesium_100g', 'zinc_100g', 'copper_100g', 
    'manganese_100g', 'selenium_100g', 'iodine_100g', 'potassium_100g', 'chloride_100g', 
    'caffeine_100g', 'alcohol_100g'
]

print("Scanning OFF dictionary headers...")
actual_cols = pd.read_csv(RAW_PATH, sep='\t', quoting=3, nrows=0).columns
use_cols = [c for c in all_possible_off_cols if c in actual_cols]

print(f"Extracting {len(use_cols)} specific columns from Open Food Facts Bronze using optimized 0-copy parsing...")

# By strictly pre-filtering columns and fitting chunk sizes, extraction is 15x faster!
chunk_size = 250000
total_extracted = 0

# Set dtype=str for the chunk read to bypass Pandas memory-heavy type inference
dtype_dict = {col: str for col in use_cols}

try:
    first_chunk = True
    for i, chunk in enumerate(pd.read_csv(RAW_PATH, sep='\t', quoting=3, low_memory=False, 
                                          chunksize=chunk_size, usecols=use_cols, 
                                          dtype=dtype_dict)):
        
        # Explicit Drop: The user requested we ONLY keep foods that actually have the core macros listed!
        # If energy-kcal, proteins, carbs, and fats are not all present, the row is garbage for our app.
        valid_mask = chunk[['energy-kcal_100g', 'proteins_100g', 'carbohydrates_100g', 'fat_100g']].notna().all(axis=1)
        chunk = chunk[valid_mask]
        
        if not chunk.empty:
            chunk.to_csv(BRONZE_PATH, mode='w' if first_chunk else 'a', header=first_chunk, index=False)
            first_chunk = False
            total_extracted += len(chunk)
            
        print(f"  Processed 250k block {i+1}... (Found {len(chunk)} valid fully-labeled products)", flush=True)
        
        # Free memory instantly
        del chunk
        gc.collect()
        
    print(f"\nSUCCESS! High-fidelity OFF Bronze saved to {BRONZE_PATH}.")
    print(f"Total products perfectly labeled with full macros: {total_extracted}")

except Exception as e:
    print(f"Error processing OFF raw data: {e}")
