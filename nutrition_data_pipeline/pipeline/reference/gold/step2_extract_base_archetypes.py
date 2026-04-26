"""Build Reference Gold Layer 1: Unified Reference Data
=========================================================
Merges USDA and CoFID silver datasets into one deduplicated
"gold standard" reference file.

New Logic:
  - Extracts base ingredient names (removes cooking methods)
  - Identifies the "Raw/Dry" base variant for an archetype
  - Preserves MACRO nutrients exactly from the Base variant
  - Computes MIN/MAX/AVG for MICRO nutrients across all variants
"""

import pandas as pd
import numpy as np
import logging
import re
from pathlib import Path
from rapidfuzz import fuzz, process

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────────
DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_SILVER = DATA_PIPELINE_DIR / 'data' / 'reference' / 'silver'
DATA_GOLD   = DATA_PIPELINE_DIR / 'data' / 'reference' / 'gold'

# ── Schema Mapping ───────────────────────────────────────────────────────────
USDA_META = {
    'description': 'ref_name',
    'fdc_id':      'source_id',
}

COFID_META = {
    'description': 'ref_name',
    'food_code':   'source_id',
}

UNIFIED_META_COLS = {
    'ref_name', 'original_name', 'source', 'source_id', 'semantic_descriptor',
    'data_source', 'data_type', 'food_category'
}

MACRO_KEYWORDS = ('calorie', 'energy', 'protein', 'carbohydrate', 'fiber', 'sugar', 'starch', 'fat', 'cholesterol', 'alcohol', 'omega', 'ala', 'epa', 'dha')

def is_macro(col_name):
    lower = col_name.lower()
    return any(kw in lower for kw in MACRO_KEYWORDS)

def strip_cooking_suffix(name):
    """Removes cooking methods from the tail of a string to isolate the core ingredient."""
    if pd.isna(name): return ''
    name = str(name)
    # Match from the comma preceding a cooking verb to the end of the string
    match = re.search(r',?\s*([^,]*?(?:fried|roasted|boiled|cooked|steamed|poached|microwaved|baked|canned|flesh|skin|raw|fresh|dry|dried).*?$)', name, re.IGNORECASE)
    if match:
        base = name[:match.start()].strip()
        if base: return base
    return name.strip()

import os
def summarize_variations(names):
    names = list(set(names))
    if len(names) <= 1:
        return names[0] if names else ""
        
    prefix = os.path.commonprefix(names)
    last_comma = prefix.rfind(',')
    if last_comma != -1:
        prefix = prefix[:last_comma+1]
    else:
        last_space = prefix.rfind(' ')
        if last_space != -1:
            prefix = prefix[:last_space+1]
            
    if len(prefix) < 4:
        return ' | '.join(names)

    suffixes = sorted(list(set([n[len(prefix):].strip(' ,') for n in names])))
    suffixes = [s if s else "base" for s in suffixes]
    
    return f"{prefix.strip(' ,')} [{', '.join(suffixes)}]"

def load_and_unify():
    gold_path = DATA_GOLD / 'unified_gold.csv'
    if not gold_path.exists():
        logger.error(f"Cannot find {gold_path}. Run build_gold.py first.")
        return pd.DataFrame()
        
    combined = pd.read_csv(gold_path, low_memory=False)
    
    if 'semantic_descriptor' not in combined.columns:
         combined['semantic_descriptor'] = 'unknown | unknown | unknown'
    combined['semantic_descriptor'] = combined['semantic_descriptor'].fillna('unknown | unknown | unknown')
    
    # Store original and strip for matching
    combined['original_name'] = combined['description']
    combined['ref_name'] = combined['description'].apply(strip_cooking_suffix)
    
    logger.info(f"  Loaded Unified Gold: {len(combined):,} rows, {len(combined.columns)} columns")
    return combined

def block_merge_fuzzy(df, nut_cols, macro_cols, micro_cols, fuzzy_threshold=80):
    if len(df) == 0:
        return pd.DataFrame(), []

    # Sort by completeness to seed archetypes with data-rich rows
    df['_completeness'] = df[nut_cols].notna().sum(axis=1)
    df = df.sort_values('_completeness', ascending=False).reset_index(drop=True)

    archetypes = []
    merge_log = []

    total_rows = len(df)
    processed = 0

    # Group by food_type_label (single vs category) AND semantic_descriptor to prevent cross-taxonomy and cross-tier merges
    grouped = df.groupby(['food_type_label', 'semantic_descriptor'], dropna=False)

    for (food_type, desc), group in grouped:
        group = group.reset_index(drop=True)
        arc_name_list = []
        arc_name_to_idx = {}
        block_archetypes = []
        
        for row_idx, row in group.iterrows():
            processed += 1
            if processed % 1000 == 0:
                logger.info(f"    {processed:,}/{total_rows:,} rows processed...")

            name = row['ref_name'] if pd.notna(row['ref_name']) else ''
            matched = False

            if arc_name_list:
                candidates = process.extract(
                    name, arc_name_list,
                    scorer=fuzz.token_sort_ratio,
                    score_cutoff=fuzzy_threshold,
                    limit=5
                )

                if candidates:
                    match_name, score, match_list_idx = candidates[0]
                    arc_idx = arc_name_to_idx[match_name]
                    arc = block_archetypes[arc_idx]

                    merge_log.append({
                        'merged_name': row['original_name'],
                        'base_target': arc['ref_name'],
                        'fuzzy_score': round(score, 1),
                        'semantic_block': desc,
                        'merged_source': row.get('source', '')
                    })

                    arc['all_base_names'].add(name)
                    arc['rows'].append(row.to_dict())
                    
                    arc_name_list.append(name)
                    arc_name_to_idx[name] = arc_idx
                    matched = True

            if not matched:
                idx = len(block_archetypes)
                block_archetypes.append({
                    'ref_name': name,
                    'all_base_names': {name},
                    'semantic_descriptor': desc,
                    'rows': [row.to_dict()]
                })
                arc_name_list.append(name)
                arc_name_to_idx[name] = idx
                
        archetypes.extend(block_archetypes)

    # ── Aggregation Logic ──
    results = []
    for arc in archetypes:
        # 1. Identify "Base" row (prioritize raw/dry/shortest)
        best_score = -99999
        base_row = None
        
        for r in arc['rows']:
            og_name = str(r['original_name']).lower()
            score = 0
            if 'raw' in og_name:
                score += 15
            if ' dry' in og_name or 'dried' in og_name:
                score += 10
            
            score -= (len(og_name) * 0.05) # Shorter is better
            
            if any(term in og_name for term in ['fried', 'cooked', 'boiled', 'roasted', 'canned']):
                score -= 20
                
            if score > best_score:
                best_score = score
                base_row = r

        # Filter out hydrated/cooked/fried variants so their diluted water/oil weight doesn't crash the base averages!
        cooking_keywords = ['cooked', 'boiled', 'steamed', 'brewed', 'prepared', 'poached', 'drained', 'fried', 'roasted', 'baked', 'microwaved', 'sauteed', 'stewed', 'grilled', 'broiled']
        dry_rows = [r for r in arc['rows'] if not any(w in str(r['original_name']).lower() for w in cooking_keywords)]
        
        # If EVERYTHING had a cooking keyword (e.g. Canned beans), or the base itself was cooked, use all rows.
        if not dry_rows or any(w in str(base_row['original_name']).lower() for w in cooking_keywords):
            valid_rows = arc['rows']
        else:
            valid_rows = dry_rows

        # Build output dict
        out = {}
        out['ref_name'] = sorted(arc['all_base_names'], key=len)[0]
        out['name_variations'] = summarize_variations([r['original_name'] for r in valid_rows])
        out['base_ingredient_source'] = base_row['original_name']
        out['semantic_descriptor'] = arc['semantic_descriptor']
        out['source'] = ' | '.join(sorted(set(str(r.get('source', '')) for r in valid_rows if pd.notna(r.get('source')))))
        out['source_id'] = ' | '.join(sorted(set(str(r.get('source_id', '')) for r in valid_rows if pd.notna(r.get('source_id')))))
        out['source_count'] = len(valid_rows)
        
        has_discrepancy = False

        # Macros -> Taken STRICTLY from Base Row
        for nut in macro_cols:
            val = base_row.get(nut, np.nan)
            out[nut] = val if pd.notna(val) else np.nan

        # Micros -> Averaged across all strictly VALID variants
        for nut in micro_cols:
            vals = [r.get(nut) for r in valid_rows if pd.notna(r.get(nut))]
            if vals:
                avg = np.mean(vals)
                mn  = np.min(vals)
                mx  = np.max(vals)
                out[nut] = round(avg, 2)
                if len(vals) > 1 and mn != mx:
                    out[f'{nut}_Min'] = round(mn, 2)
                    out[f'{nut}_Max'] = round(mx, 2)
                    if mx > 0 and (mx - mn) / mx > 0.10:
                        has_discrepancy = True
            else:
                out[nut] = np.nan
        
        out['data_quality_discrepancy'] = has_discrepancy
        results.append(out)

    return pd.DataFrame(results), merge_log

def build_unified_reference():
    logger.info("=" * 60)
    logger.info("REFERENCE GOLD LAYER: Intelligent Base Ingredient Reduction")
    logger.info("=" * 60)

    combined = load_and_unify()
    total_input = len(combined)

    nut_cols = [c for c in combined.columns
                if any(u in c.lower() for u in ['(g)', '(mg)', '(ug)', '(kcal)', '(kj)'])
                and c not in UNIFIED_META_COLS]
    
    macro_cols = [c for c in nut_cols if is_macro(c)]
    micro_cols = [c for c in nut_cols if c not in macro_cols]
    
    logger.info(f"  Macros Identified: {len(macro_cols)}")
    logger.info(f"  Micros Identified: {len(micro_cols)}")

    combined[nut_cols] = combined[nut_cols].apply(pd.to_numeric, errors='coerce').round(2)

    logger.info("\n── Block Merging & Aggregation ──")
    gold, merge_log = block_merge_fuzzy(combined, nut_cols, macro_cols, micro_cols, fuzzy_threshold=80)

    if merge_log:
        pd.DataFrame(merge_log).to_csv(DATA_GOLD / 'merge_log.csv', index=False)

    has_any_nutrient = gold[nut_cols].notna().any(axis=1)
    gold = gold[has_any_nutrient].reset_index(drop=True)

    meta_out = ['ref_name', 'base_ingredient_source', 'name_variations', 'semantic_descriptor', 'source', 'source_id', 'source_count', 'data_quality_discrepancy']
    
    nut_out = []
    for c in sorted(nut_cols):
        nut_out.append(c)
        if c in micro_cols:
            if f'{c}_Min' in gold.columns: nut_out.append(f'{c}_Min')
            if f'{c}_Max' in gold.columns: nut_out.append(f'{c}_Max')
    
    final_cols = [c for c in meta_out if c in gold.columns] + [c for c in nut_out if c in gold.columns]
    gold = gold[final_cols]

    logger.info("\n" + "=" * 60)
    logger.info("RESULTS")
    logger.info("=" * 60)
    logger.info(f"  Input rows   : {total_input:,} (Variants)")
    logger.info(f"  Output rows  : {len(gold):,} (Base Archetypes)")
    logger.info(f"  Reduction    : {total_input - len(gold):,} rows consolidated ({(1 - len(gold)/total_input)*100:.1f}%)")

    DATA_GOLD.mkdir(parents=True, exist_ok=True)
    out_path = DATA_GOLD / 'reference_unified_gold.csv'
    gold.to_csv(out_path, index=False)
    logger.info(f"\n  SUCCESS: Saved \u2192 {out_path}")

if __name__ == '__main__':
    build_unified_reference()
