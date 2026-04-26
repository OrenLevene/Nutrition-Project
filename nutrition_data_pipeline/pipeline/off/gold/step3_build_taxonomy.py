"""
OFF Gold Pipeline — Phase 3: Semantic Taxonomy Clustering

Collapses distinct brands' versions of the same food into a distinct food variant node.
Enforces an 85% text similarity match (AFTER aggressively stripping brand names from the product name).
Enforces a 15% strict nutritional tolerance max-deviation on ALL overlapping macro and micro
nutrients.
"""
import pandas as pd
import numpy as np
import logging
import re
from pathlib import Path
from rapidfuzz import fuzz, process

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
INPUT_PATH = DATA_PIPELINE_DIR / 'data' / 'off' / 'gold' / 'off_uk_deduped.csv'
OUTPUT_PATH = DATA_PIPELINE_DIR / 'data' / 'off' / 'gold' / 'off_uk_taxonomy.csv'

FUZZY_THRESHOLD = 85

def get_tolerance_floor(col_name):
    """
    Absolute floor values to prevent mathematical 15% max-deviation from fracturing
    products on trivial amounts (e.g. 0.1g vs 0.2g fat is a 100% diff, but trivial).
    """
    col_lower = col_name.lower()
    if '(kcal)' in col_lower or '(kj)' in col_lower:
        return 15.0
    if '(mg)' in col_lower:
        return 15.0  # e.g. Sodium mg
    if '(ug)' in col_lower or '(mcg)' in col_lower:
        return 5.0
    # Default is (g) for standard macros
    return 1.5

def is_nutrition_match(r1, r2, nut_cols):
    """
    Returns True if ALL overlapping nutrients are within 15% (or the absolute floor).
    If a nutrient is null in either r1 or r2, it is skipped.
    """
    for col in nut_cols:
        v1 = r1.get(col)
        v2 = r2.get(col)
        
        if pd.notna(v1) and pd.notna(v2):
            try:
                f1 = float(v1)
                f2 = float(v2)
            except ValueError:
                continue
                
            diff = abs(f1 - f2)
            floor = get_tolerance_floor(col)
            allowed = max(floor, 0.15 * max(f1, f2))
            if diff > allowed:
                return False
    return True

def strip_brands(name, brand_str):
    """
    Strips known brand names out of the product name so that semantic matching
    groups 'Tesco Mature Cheddar' and 'Sainsburys Mature Cheddar' perfectly.
    """
    name_str = str(name).strip()
    if pd.isna(brand_str) or not str(brand_str).strip():
        return name_str
        
    brands = [b.strip().lower() for b in str(brand_str).split('|')]
    name_lower = name_str.lower()
    
    # Extra stopwords that hang around brands
    stopwords = ['by', 'taste the difference', 'finest', 'extra special', 'the', 'fs']
    
    for b in brands:
        if len(b) > 2: # only strip meaningful brand strings
            # Escape regex chars in brand name
            pattern = r'\b' + re.escape(b) + r'\b'
            name_lower = re.sub(pattern, '', name_lower)

    for sw in stopwords:
        pattern = r'\b' + re.escape(sw) + r'\b'
        name_lower = re.sub(pattern, '', name_lower)
        
    # Clean up punctuation artifacts (like dangling hyphens or commas)
    name_lower = re.sub(r'[\-\,]', ' ', name_lower)
    name_lower = re.sub(r'\s+', ' ', name_lower).strip()
    
    # If we stripped it so hard nothing is left, return original
    return name_lower if name_lower else name_str


def merge_cluster_rows(rows_list, nut_cols):
    """
    Merges an array of products into a single Taxonomy Node.
    - Aggregates all nested brands/weights into pipe-delimited strings
    - Averages all nutrients across the cluster
    - Assigns the shortest stripped name as the node name
    """
    base = rows_list[0].copy()
    
    # Separate all distinct metadata values
    all_brands = set()
    all_weights = set()
    total_products = 0
    
    stripped_names = []
    
    for r in rows_list:
        stripped_names.append(r['_stripped_name'])
        
        total_products += r.get('dedup_count', 1)  # carrying over from phase 2
        
        b_str = str(r.get('brand_owner', ''))
        if b_str and b_str != 'nan':
            for b in b_str.split('|'):
                all_brands.add(b.strip())
                
        q_str = str(r.get('quantity', ''))
        if q_str and q_str != 'nan':
            for q in q_str.split('|'):
                all_weights.add(q.strip())
                
    # Define node name: shortest distinct stripped string
    stripped_names = [n for n in stripped_names if len(n) > 2]
    if stripped_names:
        stripped_names.sort(key=len)
        base['food_variant_name'] = stripped_names[0]
    else:
        base['food_variant_name'] = str(base.get('norm_name', ''))
        
    # Reassign structural outputs
    base['brand_owner'] = ' | '.join(sorted(all_brands)) if all_brands else np.nan
    base['quantity'] = ' | '.join(sorted(all_weights)) if all_weights else np.nan
    base['brand_count'] = len(all_brands)
    base['product_count'] = total_products
    
    # Calculate geometric means for nutrients
    for col in nut_cols:
        vals = []
        for r in rows_list:
            v = r.get(col)
            if pd.notna(v) and str(v).strip() and str(v).lower() != 'nan':
                try:
                    vals.append(float(v))
                except ValueError:
                    pass
        if vals:
            base[col] = float(np.mean(vals))
        else:
            base[col] = np.nan
            
    # Cleanup temp fields from dict
    if '_stripped_name' in base:
         del base['_stripped_name']
         
    return base


def run_phase3():
    logger.info("=" * 60)
    logger.info("OFF GOLD PHASE 3: Semantic Taxonomy Clustering")
    logger.info("=" * 60)

    if not INPUT_PATH.exists():
        logger.error(f"Cannot find {INPUT_PATH}. Run step2_dedup_spelling.py first.")
        return

    df = pd.read_csv(INPUT_PATH, low_memory=False)
    total_input = len(df)
    logger.info(f"  Loaded: {total_input:,} deduped products")

    # Dedup single + category rows only
    dedup_mask = df['food_type_label'].isin(['single', 'category'])
    df_dedup = df[dedup_mask].copy()
    df_other = df[~dedup_mask].copy()
    logger.info(f"  Clustering targets (single+category): {len(df_dedup):,}")
    logger.info(f"  Kept as-is (composite+supplement): {len(df_other):,}")

    nut_cols = [c for c in df.columns
                if any(u in c.lower() for u in ['(g)', '(mg)', '(ug)', '(kcal)', '(kj)'])]

    # Pre-compute stripped names for the whole dataframe
    logger.info("  Stripping brand names from product strings for semantic matching...")
    df_dedup['_stripped_name'] = df_dedup.apply(lambda r: strip_brands(r.get('norm_name'), r.get('brand_owner')), axis=1)

    # Partitioning Strategy
    def make_block_key(row):
        cat = row['main_category_en']
        if pd.notna(cat) and str(cat).strip():
            return str(cat).strip()
            
        # For uncategorized, bin by coarse macros instead of first-word
        kcal = row.get('Calories (kcal)', 0)
        prot = row.get('Protein (g)', 0)
        
        # Handle missing safely
        try: kcal = float(kcal) if pd.notna(kcal) else 0.0
        except ValueError: kcal = 0.0
        try: prot = float(prot) if pd.notna(prot) else 0.0
        except ValueError: prot = 0.0
        
        # Bin boundaries: 100 kcal, 10g protein
        bin_kcal = int(round(kcal / 100.0) * 100)
        bin_prot = int(round(prot / 10.0) * 10)
        
        return f'__NOCAT_K{bin_kcal}_P{bin_prot}'

    df_dedup['_group_cat'] = df_dedup.apply(make_block_key, axis=1)
    grouped = df_dedup.groupby(['food_type_label', '_group_cat'], dropna=False)
    total_groups = len(grouped)
    logger.info(f"\n  Processing {total_groups:,} partitions...")

    results = []
    total_merged = 0
    processed_groups = 0

    for (food_type, cat), group in grouped:
        processed_groups += 1
        if processed_groups % 500 == 0:
            logger.info(f"    {processed_groups:,}/{total_groups:,} partitions...")

        group = group.reset_index(drop=True)

        if len(group) == 1:
            row = group.iloc[0].to_dict()
            row['food_variant_name'] = row.get('_stripped_name', row.get('norm_name'))
            row['brand_count'] = len(str(row.get('brand_owner','')).split('|')) if pd.notna(row.get('brand_owner')) else 0
            row['product_count'] = row.get('dedup_count', 1)
            if '_stripped_name' in row: del row['_stripped_name']
            results.append(row)
            continue

        archetypes = []  # List of lists of dicts
        arc_docs = []    # Parallel array storing the stripped names representing each archetype
        
        for _, row in group.iterrows():
            row_dict = row.to_dict()
            stripped_name = row_dict['_stripped_name']
            
            # Fast-path exact text match check first
            matched = False
            best_idx = -1
            best_score = -1
            
            if arc_docs:
                candidates = process.extract(
                    stripped_name, arc_docs,
                    scorer=fuzz.token_sort_ratio,
                    score_cutoff=FUZZY_THRESHOLD,
                    limit=5  # check top 5 matches in case the #1 text match fails the nutrition check
                )
                
                # Check semantic candidates against the NUTRITION safety valve
                for match_str, score, arc_idx in candidates:
                    # We check the macro deviation against the BASE member of that archetype
                    base_member = archetypes[arc_idx][0]
                    if is_nutrition_match(row_dict, base_member, nut_cols):
                        matched = True
                        best_idx = arc_idx
                        break # Successfully found text+nutrition match
            
            if matched:
                archetypes[best_idx].append(row_dict)
            else:
                archetypes.append([row_dict])
                arc_docs.append(stripped_name)

        # Merge clusters into output
        for arc_rows in archetypes:
            merged_row = merge_cluster_rows(arc_rows, nut_cols)
            results.append(merged_row)
            if len(arc_rows) > 1:
                total_merged += len(arc_rows) - 1

    logger.info(f"\n  Taxonomy Clusters built: {len(df_dedup):,} \u2192 {len(results):,} distinct nodes")
    
    # Format unmerged types (composites/supplements) to match structure
    df_other['food_variant_name'] = df_other['norm_name']
    df_other['brand_count'] = 1
    df_other['product_count'] = df_other['dedup_count'].fillna(1)
    
    df_taxonomy = pd.DataFrame(results)
    df_taxonomy = df_taxonomy.drop(columns=['_group_cat'], errors='ignore')
    
    final = pd.concat([df_taxonomy, df_other], ignore_index=True)

    # ── Report ──
    logger.info("\n" + "=" * 60)
    logger.info("PHASE 3 RESULTS: FINAL TAXONOMY")
    logger.info("=" * 60)
    logger.info(f"  Input:     {total_input:,}")
    logger.info(f"  Output Node Count: {len(final):,}")
    logger.info(f"  Reduction: {total_input - len(final):,} nodes collapsed ({(total_input - len(final))/total_input*100:.1f}%)")
    
    # Show Top Taxonomy Nodes
    if 'product_count' in final.columns:
        top_nodes = final.nlargest(15, 'product_count')
        logger.info(f"\n  Top Taxonomy Nodes (Brands Merged):")
        for _, r in top_nodes.iterrows():
            brands = str(r.get('brand_owner', ''))[:40]
            logger.info(f"    {str(r.get('food_variant_name', ''))[:45]:<45}  [size: {r['product_count']:>3}] brands={brands}...")

    final.to_csv(OUTPUT_PATH, index=False)
    logger.info(f"\n  Saved Taxonomy to: {OUTPUT_PATH}")

if __name__ == '__main__':
    run_phase3()
