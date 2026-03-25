import pandas as pd
import numpy as np
import logging
import re
import sys
from pathlib import Path
import gc

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Paths
DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
BRONZE_PATH = DATA_PIPELINE_DIR / 'data' / 'off' / 'bronze' / 'off.csv'
SILVER_DIR = DATA_PIPELINE_DIR / 'data' / 'product' / 'silver'
GLOBAL_SILVER_PATH = SILVER_DIR / 'off_unified_silver.csv'

# Import utils
sys.path.insert(0, str(DATA_PIPELINE_DIR / 'pipeline' / 'utils'))
from reference_cleaning import standardize_column_names, parse_weight_to_grams

OFF_META_MAPPING = {'code': 'food_id','product_name': 'description','brands': 'brand_owner','categories': 'category'}
OFF_MAPPING = {'energy-kcal_100g': 'Calories (kcal)','proteins_100g': 'Protein (g)','carbohydrates_100g': 'Carbohydrate (g)','fat_100g': 'Fat (g)','saturated-fat_100g': 'Saturated Fat (g)','monounsaturated-fat_100g': 'Monounsaturated Fat (g)','polyunsaturated-fat_100g': 'Polyunsaturated Fat (g)','trans-fat_100g': 'Trans Fat (g)','fiber_100g': 'Fiber (g)','sugars_100g': 'Sugars (g)','sodium_100g': 'Sodium (mg)','salt_100g': 'Salt (g)','calcium_100g': 'Calcium (mg)','iron_100g': 'Iron (mg)','magnesium_100g': 'Magnesium (mg)','potassium_100g': 'Potassium (mg)','vitamin-c_100g': 'Vitamin C (mg)','vitamin-a_100g': 'Vitamin A (ug)','vitamin-d_100g': 'Vitamin D (ug)','vitamin-b12_100g': 'Vitamin B12 (ug)'}

def selective_normalize(text):
    if pd.isna(text) or not str(text).strip(): return "unknown", ""
    t = str(text).lower()
    size_pattern = r'\b(\d+(\.\d+)?\s*(g|kg|ml|l|oz|lb|pc|pack|kcal|kj))\b'
    sizes = [s[0] for s in re.findall(size_pattern, t)]
    name = re.sub(size_pattern, ' ', t)
    name = re.sub(r'[^a-zA-Z0-9% ]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return (name if name else "unknown"), (" | ".join(sizes) if sizes else "")

def build_unified_silver():
    logger.info("=" * 60)
    logger.info("STAGE: GLOBAL SILVER REFINEMENT (Strict 10% Merge with Stats)")
    logger.info("=" * 60)
    
    if not BRONZE_PATH.exists(): return
    logger.info("Loading Bronze data...")
    df = pd.read_csv(BRONZE_PATH, low_memory=False)
    
    # 1. Pre-filtering for core macros
    logger.info("Filtering products for core macro completeness...")
    df = df.rename(columns=OFF_META_MAPPING).rename(columns=OFF_MAPPING)
    core_macros = ['Calories (kcal)', 'Fat (g)', 'Protein (g)', 'Carbohydrate (g)']
    
    # Ensure all core macros are present in columns
    for c in core_macros:
        if c not in df.columns: df[c] = np.nan
        
    initial_count = len(df)
    df = df.dropna(subset=core_macros)
    logger.info(f"Products: {initial_count:,} -> {len(df):,} (with full macros)")

    # 2. Cleanup & Normalization
    df = df.drop(columns=[c for c in ['image_url', 'image_small_url', 'image_ingredients_url'] if c in df.columns])
    if 'quantity' in df.columns: df['Total Weight (g)'] = df['quantity'].apply(parse_weight_to_grams)
    df = standardize_column_names(df)
    
    nut_cols = [c for c in df.columns if any(u in c.lower() for u in ['(g)', '(mg)', '(ug)', '(kcal)', '(kj)'])]
    df[nut_cols] = df[nut_cols].round(2)
    
    logger.info("Normalizing names and extracting sizes...")
    norm_results = df['description'].apply(selective_normalize)
    df['norm_name'] = [r[0] for r in norm_results]
    df['size_variation'] = [r[1] for r in norm_results]
    
    # 3. Smart Merge (Per-Name Group)
    logger.info("Phase 2: Full-Profile 10% Smart Merging...")
    
    df['nutri_count'] = df[nut_cols].notna().sum(axis=1)
    df['row_index'] = range(len(df))
    
    group_cols = ['norm_name', 'main_category_en']
    df = df.sort_values(group_cols + ['nutri_count', 'row_index'], ascending=[True, True, False, False])
    
    def smart_merge_group(sub):
        if len(sub) <= 1: return sub.copy()
        sub = sub.copy()
        archetypes = [sub.iloc[0].to_dict()]
        
        for i in range(1, len(sub)):
            row = sub.iloc[i]
            matched = False
            for arc in archetypes:
                is_compat = True
                for nut in nut_cols:
                    v1, v2 = arc[nut], row[nut]
                    if pd.notna(v1) and pd.notna(v2):
                        diff = abs(v1 - v2)
                        mx = max(abs(v1), abs(v2), 1e-9)
                        if diff / mx > 0.10: 
                            is_compat = False; break
                
                if is_compat:
                    sub.at[row['row_index'], 'arc_id'] = archetypes.index(arc)
                    for nut in nut_cols:
                        if pd.isna(arc[nut]): arc[nut] = row[nut]
                    matched = True; break
            
            if not matched:
                new_arc = row.to_dict()
                archetypes.append(new_arc)
                sub.at[row['row_index'], 'arc_id'] = len(archetypes) - 1
                
        return sub

    df['arc_id'] = 0
    multi_groups = df.groupby(group_cols).filter(lambda x: len(x) > 1)
    single_groups = df.groupby(group_cols).filter(lambda x: len(x) == 1)
    
    logger.info(f"Applying logic to {len(multi_groups):,} candidate rows...")
    df_multi = multi_groups.groupby(group_cols, group_keys=False).apply(smart_merge_group)
    df = pd.concat([single_groups, df_multi])

    # 4. Final Aggregation (Min, Max, Avg)
    logger.info("Generating Statistics (Avg, Min, Max)...")
    final_sig = group_cols + ['arc_id']
    
    agg_rules = {}
    for c in nut_cols: agg_rules[c] = ['mean', 'min', 'max', 'count']
    
    squash_cols = ['food_id', 'quantity', 'Total Weight (g)', 'brand_owner', 'category', 'countries_en', 'ingredients_text', 'size_variation']
    def squash(x):
        items = sorted(set([str(i).strip() for i in x if pd.notna(i) and str(i).strip() and str(i) != 'unknown']))
        return " | ".join(items) if items else pd.NA
    for c in squash_cols: 
        if c in df.columns: agg_rules[c] = squash
    agg_rules['description'] = 'first'

    final_df = df.groupby(final_sig, dropna=False).agg(agg_rules)
    
    final_df.columns = [f"{c}_{a}" if a in ['min', 'max'] else c for c, a in final_df.columns]
    
    # Post-process Min/Max
    for c in nut_cols:
        count_col = f"{c}_count"
        min_col = f"{c}_min"
        max_col = f"{c}_max"
        if count_col in final_df.columns:
            exact_match = (final_df[min_col] == final_df[max_col])
            single_row = (final_df[count_col] == 1)
            final_df.loc[exact_match | single_row, [min_col, max_col]] = np.nan
            final_df = final_df.drop(columns=[count_col])

    # Final Save
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(GLOBAL_SILVER_PATH, index=False)
    logger.info(f"DONE! Global Gold Silver archetypes: {len(final_df):,}")
    logger.info(f"Saved to {GLOBAL_SILVER_PATH}")

if __name__ == "__main__":
    build_unified_silver()
