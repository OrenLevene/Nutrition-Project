"""
OFF Gold Pipeline — Phase 2: Spelling Deduplication (Very Tight Fuzzy Merge)

Collapses near-identical product names within the same (food_type_label, main_category_en)
partition. Uses a very high fuzzy threshold (93+) to catch only genuine typos and trivial
formatting differences, NOT semantically different products.

Reuses the same partitioned block-merge strategy as build_unified_reference.py.
"""
import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
from rapidfuzz import fuzz, process

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
INPUT_PATH = DATA_PIPELINE_DIR / 'data' / 'off' / 'gold' / 'off_uk_labeled.csv'
OUTPUT_PATH = DATA_PIPELINE_DIR / 'data' / 'off' / 'gold' / 'off_uk_deduped.csv'

FUZZY_THRESHOLD = 93  # Very high — only true typos/formatting differences

# Columns that get concatenated (pipe-delimited) when merging duplicates
SQUASH_COLS = ['food_id', 'brand_owner', 'quantity', 'countries_en', 'ingredients_text']


def merge_duplicate_rows(rows_list, nut_cols):
    """
    Merge a list of near-identical product rows into a single representative row.
    
    - Nutrition: take the row with highest nutri_count (most complete data)
    - Metadata: concatenate brand_owner, quantity, food_id across all duplicates
    """
    if len(rows_list) == 1:
        out = rows_list[0].copy()
        out['dedup_count'] = 1
        return out

    # Sort by completeness — most complete row becomes the base
    rows_list.sort(key=lambda r: -r.get('nutri_count', 0))
    base = rows_list[0].copy()

    # Squash metadata columns
    for col in SQUASH_COLS:
        all_vals = []
        for r in rows_list:
            v = str(r.get(col, '')).strip()
            if v and v != 'nan' and v != 'unknown' and v != 'NaN':
                all_vals.append(v)
        unique_vals = sorted(set(all_vals))
        base[col] = ' | '.join(unique_vals) if unique_vals else np.nan

    base['dedup_count'] = len(rows_list)
    return base


def run_phase2():
    logger.info("=" * 60)
    logger.info("OFF GOLD PHASE 2: Spelling Deduplication (Fuzzy Threshold=93)")
    logger.info("=" * 60)

    if not INPUT_PATH.exists():
        logger.error(f"Cannot find {INPUT_PATH}. Run step1_classify_categories.py first.")
        return

    df = pd.read_csv(INPUT_PATH, low_memory=False)
    total_input = len(df)
    logger.info(f"  Loaded: {total_input:,} labeled products")

    # Only dedup single + category rows (composites and supplements kept as-is)
    dedup_mask = df['food_type_label'].isin(['single', 'category'])
    df_dedup = df[dedup_mask].copy()
    df_other = df[~dedup_mask].copy()
    logger.info(f"  Dedup candidates (single+category): {len(df_dedup):,}")
    logger.info(f"  Kept as-is (composite+supplement): {len(df_other):,}")

    # Identify nutrient columns
    nut_cols = [c for c in df.columns
                if any(u in c.lower() for u in ['(g)', '(mg)', '(ug)', '(kcal)', '(kj)'])]

    # ── Block-merge by (food_type_label, category, first_word) ──
    # For products WITH a category, partition by (food_type_label, category).
    # For products WITHOUT a category (~60% of data), use the first word of
    # norm_name as a blocking key. Typos like "tescos chedder" vs "tesco cheddar"
    # share the first word, so they'll still match. Products with completely
    # different first words can never be typo-duplicates anyway.
    def make_block_key(row):
        cat = row['main_category_en']
        if pd.notna(cat) and str(cat).strip():
            return str(cat).strip()
        # No category: use first word of norm_name for blocking
        name = str(row.get('norm_name', '')).strip()
        first_word = name.split()[0] if name.split() else '__EMPTY__'
        return f'__NOCAT_{first_word}'

    df_dedup['_group_cat'] = df_dedup.apply(make_block_key, axis=1)

    grouped = df_dedup.groupby(['food_type_label', '_group_cat'], dropna=False)
    total_groups = len(grouped)
    logger.info(f"\n  Processing {total_groups:,} partitions (subcategorised by first word)...")

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
            row['dedup_count'] = 1
            results.append(row)
            continue

        # Build fuzzy archetypes within this partition
        arc_name_list = []
        arc_name_to_idx = {}
        archetypes = []  # Each is a list of row dicts

        for _, row in group.iterrows():
            row_dict = row.to_dict()
            name = str(row_dict.get('norm_name', ''))
            matched = False

            if arc_name_list:
                candidates = process.extract(
                    name, arc_name_list,
                    scorer=fuzz.token_sort_ratio,
                    score_cutoff=FUZZY_THRESHOLD,
                    limit=1
                )
                if candidates:
                    match_name, score, _ = candidates[0]
                    arc_idx = arc_name_to_idx[match_name]
                    archetypes[arc_idx].append(row_dict)
                    arc_name_list.append(name)
                    arc_name_to_idx[name] = arc_idx
                    matched = True

            if not matched:
                idx = len(archetypes)
                archetypes.append([row_dict])
                arc_name_list.append(name)
                arc_name_to_idx[name] = idx

        # Merge each archetype group
        for arc_rows in archetypes:
            merged_row = merge_duplicate_rows(arc_rows, nut_cols)
            results.append(merged_row)
            if len(arc_rows) > 1:
                total_merged += len(arc_rows) - 1

    logger.info(f"\n  Dedup complete: {len(df_dedup):,} → {len(results):,} rows")
    logger.info(f"  Typo-duplicates collapsed: {total_merged:,}")

    # Reassemble with composites and supplements
    df_deduped = pd.DataFrame(results)
    df_deduped = df_deduped.drop(columns=['_group_cat'], errors='ignore')

    # Add dedup_count to non-deduped rows
    df_other['dedup_count'] = 1

    final = pd.concat([df_deduped, df_other], ignore_index=True)

    # ── Report ──
    logger.info("\n" + "=" * 60)
    logger.info("PHASE 2 RESULTS")
    logger.info("=" * 60)
    logger.info(f"  Input:     {total_input:,}")
    logger.info(f"  Output:    {len(final):,}")
    logger.info(f"  Reduction: {total_input - len(final):,} rows ({(total_input - len(final))/total_input*100:.1f}%)")
    logger.info(f"\n  Label distribution:")
    for label, count in final['food_type_label'].value_counts().items():
        logger.info(f"    {label:<15} {count:>7,}")

    # Spot-check: show some merged groups
    merged_rows = df_deduped[df_deduped['dedup_count'] > 1].nlargest(10, 'dedup_count')
    if len(merged_rows) > 0:
        logger.info(f"\n  Top merged groups:")
        for _, r in merged_rows.iterrows():
            brands = str(r.get('brand_owner', ''))[:40]
            logger.info(f"    {str(r['norm_name'])[:45]:<45}  x{r['dedup_count']:<3}  brands={brands}")

    # Save
    final.to_csv(OUTPUT_PATH, index=False)
    logger.info(f"\n  Saved: {OUTPUT_PATH}")


if __name__ == '__main__':
    run_phase2()
