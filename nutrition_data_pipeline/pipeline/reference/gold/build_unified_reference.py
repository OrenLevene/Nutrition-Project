"""Build Reference Gold Layer 1: Unified Reference Data
=========================================================
Merges USDA, CoFID, and FooDB silver datasets into one deduplicated
"gold standard" reference file:
  - rapidfuzz token_sort_ratio (>=60) for fuzzy name matching
  - 10% nutrient tolerance for merge validation
  - Min/Max/Avg statistical ranges for cross-source entries
  - Exports a merge log for human review
"""

import pandas as pd
import numpy as np
import re
import logging
from pathlib import Path
from rapidfuzz import fuzz, process

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────────
DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_SILVER = DATA_PIPELINE_DIR / 'data' / 'reference' / 'silver'
DATA_GOLD   = DATA_PIPELINE_DIR / 'data' / 'reference' / 'gold'

# ── Schema Mapping ───────────────────────────────────────────────────────────
# Each source uses slightly different metadata column names.
# We unify them into: ref_name, source, source_id, category.

USDA_META = {
    'description': 'ref_name',
    'fdc_id':      'source_id',
    'food_category': 'category',
}

COFID_META = {
    'description': 'ref_name',
    'food_code':   'source_id',
}

FOODB_META = {
    'description': 'ref_name',
    'food_id':     'source_id',
    'food_group':  'category',
}

# Columns we definitely want to keep as metadata (not nutrients)
UNIFIED_META_COLS = {
    'ref_name', 'source', 'source_id', 'category',
    'data_source', 'food_subgroup', 'food_type',
    'name_scientific', 'wikipedia_id',
    'food_category_id', 'data_type', 'publication_date',
    'Total Weight (g)', 'food_code',
}




# ── Load & Unify ─────────────────────────────────────────────────────────────
def load_and_unify():
    """Load all three silver datasets, unify schemas, concatenate."""
    frames = []

    # ── USDA ──
    try:
        usda = pd.read_csv(DATA_SILVER / 'usda.csv')
        usda = usda.rename(columns=USDA_META)
        usda['source'] = 'USDA'
        logger.info(f"  USDA  : {len(usda):>6,} foods, {len(usda.columns)} cols")
        frames.append(usda)
    except FileNotFoundError:
        logger.warning("  USDA silver not found — skipping")

    # ── CoFID ──
    try:
        cofid = pd.read_csv(DATA_SILVER / 'cofid.csv')
        cofid = cofid.rename(columns=COFID_META)
        cofid['source'] = 'CoFID'
        # CoFID has a data_source column; category may be missing
        if 'category' not in cofid.columns:
            cofid['category'] = np.nan
        logger.info(f"  CoFID : {len(cofid):>6,} foods, {len(cofid.columns)} cols")
        frames.append(cofid)
    except FileNotFoundError:
        logger.warning("  CoFID silver not found — skipping")

    # ── FooDB ──
    try:
        foodb = pd.read_csv(DATA_SILVER / 'foodb.csv')
        # FooDB has both 'food_group' and 'category' columns; drop the
        # existing 'category' (which is a less useful duplicate) before
        # renaming 'food_group' → 'category' to avoid duplicate columns.
        if 'category' in foodb.columns and 'food_group' in foodb.columns:
            foodb = foodb.drop(columns=['category'])
        foodb = foodb.rename(columns=FOODB_META)
        foodb['source'] = 'FooDB'
        logger.info(f"  FooDB : {len(foodb):>6,} foods, {len(foodb.columns)} cols")
        frames.append(foodb)
    except FileNotFoundError:
        logger.warning("  FooDB silver not found — skipping")

    if not frames:
        raise FileNotFoundError("No silver reference files found!")

    # Concatenate — columns not in all frames become NaN automatically
    combined = pd.concat(frames, ignore_index=True)
    logger.info(f"  Combined: {len(combined):,} rows, {len(combined.columns)} columns")
    return combined


# ── Nutrient Compatibility Check ─────────────────────────────────────────────
def nutrients_compatible(arc_nutrients, row, nut_cols, tolerance=0.10, min_shared=3):
    """Check if all shared (non-NaN) nutrients are within tolerance.
    Also requires at least min_shared shared values to prevent
    vacuous matches from sparse data."""
    shared_count = 0
    for nut in nut_cols:
        v_arc = arc_nutrients.get(nut, np.nan)
        v_row = row.get(nut, np.nan)
        if pd.notna(v_arc) and pd.notna(v_row):
            shared_count += 1
            mx = max(abs(v_arc), abs(v_row), 1e-9)
            if abs(v_arc - v_row) / mx > tolerance:
                return False
    return shared_count >= min_shared


# ── Smart Merge Engine (rapidfuzz) ───────────────────────────────────────────
def smart_merge_fuzzy(df, nut_cols, fuzzy_threshold=60, min_shared=3, initial_archetypes=None, discard_unmatched=False):
    """
    For each food row:
      1. Compare its name against all existing archetype names using
         rapidfuzz.fuzz.token_sort_ratio.
      2. Among fuzzy-matched candidates, check 10% nutrient compatibility.
      3. If both pass → merge into that archetype.
    Returns (gold_df, merge_log, archetypes)
    """
    if len(df) == 0:
        return pd.DataFrame(), [], (initial_archetypes or [])

    # Sort by nutrient completeness so the most data-rich rows seed archetypes
    df = df.copy()
    df['_completeness'] = df[nut_cols].notna().sum(axis=1)
    df = df.sort_values('_completeness', ascending=False).reset_index(drop=True)

    archetypes = initial_archetypes if initial_archetypes is not None else []
    arc_name_list = []    # flat list of all archetype names (for rapidfuzz batch)
    arc_name_to_idx = {}  # name → archetype index
    merge_log = []        # track every merge for human review

    # Pre-populate fuzzy index if we have initial archetypes
    for idx, arc in enumerate(archetypes):
        for aname in arc['all_names']:
            arc_name_list.append(aname)
            arc_name_to_idx[aname] = idx

    total_rows = len(df)
    progress_step = max(1, total_rows // 10)

    for row_idx, row in df.iterrows():
        if (row_idx + 1) % progress_step == 0:
            logger.info(f"    {row_idx+1:,}/{total_rows:,} rows processed...")

        name = row['ref_name'] if pd.notna(row['ref_name']) else ''
        matched = False

        # Use rapidfuzz process.extract for batch comparison (C-optimized)
        if arc_name_list:
            candidates = process.extract(
                name, arc_name_list,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=fuzzy_threshold,
                limit=20  # check top 20 fuzzy matches
            )

            # candidates = [(matched_name, score, index_in_list), ...]
            # Sort by score descending to try best match first
            for match_name, score, match_list_idx in candidates:
                arc_idx = arc_name_to_idx[match_name]
                arc = archetypes[arc_idx]

                # Nutrient check
                if not nutrients_compatible(arc['nutrients'], row, nut_cols, min_shared=min_shared):
                    continue

                # ── MERGE ──
                merge_log.append({
                    'merged_name': name,
                    'into_archetype': arc['best_name'],
                    'fuzzy_score': round(score, 1),
                    'merged_source': row.get('source', ''),
                    'archetype_source': ' | '.join(sorted(set(s for s in arc['sources'] if s))),
                })

                arc['all_names'].add(name)
                # Add name to the flat list for future lookups
                arc_name_list.append(name)
                arc_name_to_idx[name] = arc_idx

                for nut in nut_cols:
                    val = row.get(nut, np.nan)
                    if pd.notna(val):
                        if pd.isna(arc['nutrients'][nut]):
                            arc['nutrients'][nut] = val
                        arc['all_values'][nut].append(val)

                arc['sources'].append(row.get('source', ''))
                arc['source_ids'].append(str(row.get('source_id', '')))
                if pd.notna(row.get('category')):
                    arc['categories'].add(str(row['category']))

                if len(name) < len(arc['best_name']):
                    arc['best_name'] = name

                matched = True
                break

        if not matched:
            if discard_unmatched:
                continue
            idx = len(archetypes)
            archetypes.append({
                'best_name': name,
                'all_names': {name},
                'nutrients': {n: row.get(n, np.nan) for n in nut_cols},
                'all_values': {n: [row.get(n, np.nan)] if pd.notna(row.get(n, np.nan)) else [] for n in nut_cols},
                'sources': [row.get('source', '')],
                'source_ids': [str(row.get('source_id', ''))],
                'categories': {str(row['category'])} if pd.notna(row.get('category')) else set(),
            })
            arc_name_list.append(name)
            arc_name_to_idx[name] = idx

    # Build output DataFrame
    results = []
    for arc in archetypes:
        out = {}
        sorted_names = sorted(arc['all_names'], key=len)
        out['ref_name'] = sorted_names[0]
        out['name_variations'] = ' | '.join(sorted_names)
        out['source'] = ' | '.join(sorted(set(s for s in arc['sources'] if s)))
        out['source_id'] = ' | '.join(sorted(set(s for s in arc['source_ids'] if s and s != 'nan')))
        out['category'] = ' | '.join(sorted(arc['categories'])) if arc['categories'] else np.nan
        out['source_count'] = len(set(s for s in arc['sources'] if s))

        for nut in nut_cols:
            vals = arc['all_values'][nut]
            if vals:
                avg = np.mean(vals)
                mn  = np.min(vals)
                mx  = np.max(vals)
                out[nut] = round(avg, 2)
                if len(vals) > 1 and mn != mx:
                    out[f'{nut}_Min'] = round(mn, 2)
                    out[f'{nut}_Max'] = round(mx, 2)
                else:
                    out[f'{nut}_Min'] = np.nan
                    out[f'{nut}_Max'] = np.nan
            else:
                out[nut] = np.nan
                out[f'{nut}_Min'] = np.nan
                out[f'{nut}_Max'] = np.nan

        results.append(out)

    return pd.DataFrame(results), merge_log, archetypes


# ── Main Pipeline ────────────────────────────────────────────────────────────
def build_unified_reference():
    logger.info("=" * 60)
    logger.info("REFERENCE GOLD LAYER 1: Unified Reference Data")
    logger.info("=" * 60)

    # 1. Load & Unify
    logger.info("\n── Step 1: Loading Silver Sources ──")
    combined = load_and_unify()
    total_input = len(combined)

    # 2. Round all nutrients to 2 decimal places
    logger.info("\n── Step 2: Rounding nutrients to 2DP ──")
    nut_cols = [c for c in combined.columns
                if any(u in c.lower() for u in ['(g)', '(mg)', '(ug)', '(kcal)', '(kj)'])
                and c not in UNIFIED_META_COLS]
    combined[nut_cols] = combined[nut_cols].apply(pd.to_numeric, errors='coerce').round(2)
    logger.info(f"  Nutrient columns identified: {len(nut_cols)}")

    # 3. Two-Step Merging: Primary (USDA/CoFID) then Secondary (FooDB)
    primary_df = combined[combined['source'] != 'FooDB'].reset_index(drop=True)
    foodb_df = combined[combined['source'] == 'FooDB'].reset_index(drop=True)

    logger.info("\n── Step 3: Merging Primary Sources (USDA + CoFID) ──")
    logger.info("  Requires 5 shared nutrients, score >= 60...")
    gold_primary, merge_log_p, archetypes = smart_merge_fuzzy(
        primary_df, nut_cols, 
        fuzzy_threshold=60, 
        min_shared=5
    )

    logger.info("\n── Step 4: Enriching with FooDB ──")
    logger.info("  Requires 0 shared nutrients, score >= 80, discards unmatched...")
    gold, merge_log_s, _ = smart_merge_fuzzy(
        foodb_df, nut_cols, 
        fuzzy_threshold=80, 
        min_shared=0, 
        initial_archetypes=archetypes, 
        discard_unmatched=True
    )
    
    merge_log = merge_log_p + merge_log_s

    # Save merge log for human review
    if merge_log:
        log_df = pd.DataFrame(merge_log)
        log_path = DATA_GOLD / 'merge_log.csv'
        log_df.to_csv(log_path, index=False)
        logger.info(f"  Merge log saved → {log_path} ({len(log_df)} merges)")

    # 5. Drop fully empty nutrient rows (no data at all)
    has_any_nutrient = gold[nut_cols].notna().any(axis=1)
    before_drop = len(gold)
    gold = gold[has_any_nutrient].reset_index(drop=True)
    if before_drop > len(gold):
        logger.info(f"  Dropped {before_drop - len(gold)} rows with zero nutrient data")

    # 6. Sort output columns logically
    meta_out = ['ref_name', 'name_variations', 'source', 'source_id', 'source_count', 'category']
    # Nutrient columns: Avg, then Min/Max pairs
    nut_out = []
    for c in sorted(nut_cols):
        nut_out.append(c)
        if f'{c}_Min' in gold.columns:
            nut_out.append(f'{c}_Min')
        if f'{c}_Max' in gold.columns:
            nut_out.append(f'{c}_Max')
    
    # Only include columns that actually exist
    final_cols = [c for c in meta_out if c in gold.columns]
    final_cols += [c for c in nut_out if c in gold.columns]
    gold = gold[final_cols]

    # 7. Report
    logger.info("\n" + "=" * 60)
    logger.info("RESULTS")
    logger.info("=" * 60)
    logger.info(f"  Input rows   : {total_input:,} (USDA + CoFID + FooDB)")
    logger.info(f"  Output rows  : {len(gold):,} unified archetypes")
    logger.info(f"  Reduction    : {total_input - len(gold):,} rows merged ({(1 - len(gold)/total_input)*100:.1f}%)")

    # Source breakdown
    logger.info(f"\n  Source Composition:")
    for src_label in ['USDA', 'CoFID', 'FooDB']:
        count = gold['source'].str.contains(src_label, na=False).sum()
        logger.info(f"    {src_label:<8}: appears in {count:,} archetypes")

    multi_source = (gold['source_count'] > 1).sum()
    logger.info(f"\n  Cross-source entries (merged from 2+ databases): {multi_source:,}")

    # Macro coverage
    logger.info(f"\n  Macro Coverage:")
    for col in ['Calories (kcal)', 'Protein (g)', 'Carbohydrate (g)', 'Fat (g)', 'Fiber (g)']:
        if col in gold.columns:
            pct = gold[col].notna().sum() / len(gold) * 100
            logger.info(f"    {col:<25} {pct:>5.1f}%")

    # 8. Save
    DATA_GOLD.mkdir(parents=True, exist_ok=True)
    out_path = DATA_GOLD / 'reference_unified_gold.csv'
    gold.to_csv(out_path, index=False)
    logger.info(f"\n  SUCCESS: Saved → {out_path}")
    logger.info(f"  File size: {out_path.stat().st_size / 1024 / 1024:.1f} MB")


if __name__ == '__main__':
    build_unified_reference()
