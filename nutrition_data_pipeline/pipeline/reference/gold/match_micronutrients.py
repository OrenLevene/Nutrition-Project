"""
Triple-Gate Micronutrient Matcher v2

Fixes from v1:
1. Prefer raw/dried reference entries over cooked ones
2. Skip reference entries with missing or obviously wrong macros
3. Use top-5 semantic matches + pick the one with best macro agreement
4. Relaxed macro gate for high semantic confidence
"""
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import sys

sys.stdout.reconfigure(encoding='utf-8')

# ===== Configuration =====
MODEL_NAME = 'all-MiniLM-L6-v2'

# Macro columns
OFF_MACROS = {'calories': 'calories', 'protein': 'protein', 
              'carbohydrate': 'carbohydrate', 'fat': 'fat'}
REF_MACROS = {'calories': 'Energy (KCAL)', 'protein': 'Protein (G)',
              'carbohydrate': 'Carbohydrate, by difference (G)', 'fat': 'Total lipid (fat) (G)'}

# Key micronutrient columns to inherit
MICRONUTRIENT_COLS = [
    'Fiber, total dietary (G)', 'Sugars, Total (G)', 'Cholesterol (MG)',
    'Sodium, Na (MG)', 'Calcium, Ca (MG)', 'Iron, Fe (MG)',
    'Magnesium, Mg (MG)', 'Phosphorus, P (MG)', 'Potassium, K (MG)',
    'Zinc, Zn (MG)', 'Copper, Cu (MG)', 'Manganese, Mn (MG)',
    'Selenium, Se (UG)', 'Thiamin (MG)', 'Riboflavin (MG)', 'Niacin (MG)',
    'Vitamin B-6 (MG)', 'Folate, total (UG)', 'Vitamin B-12 (UG)',
    'Vitamin A, RAE (UG)', 'Total Vitamin D', 'Total Vitamin E',
    'Vitamin C, total ascorbic acid (MG)', 'Total Omega-3 (G)',
    'Total Omega-6 (G)', 'Iodine, I (UG)', 'Biotin (UG)',
    'Chloride (MG)', 'Water (G)', 'Starch (G)',
]


def clean_reference_db(ref_df):
    """
    Clean the reference DB:
    1. Remove entries with obviously wrong macros (fat > 100g per 100g etc.)
    2. Flag raw/cooked state
    """
    ref = ref_df.copy()
    
    # Flag obviously wrong values
    for col in REF_MACROS.values():
        if col in ref.columns:
            # Fat/protein/carbs can't exceed 100g per 100g
            if col != 'Energy (KCAL)':
                bad = ref[col] > 100
                ref.loc[bad, col] = np.nan
            # Calories can't exceed 900 per 100g (pure fat = ~900)
            else:
                bad = ref[col] > 900
                ref.loc[bad, col] = np.nan
    
    # Tag food state
    desc_lower = ref['description'].str.lower()
    ref['is_raw'] = desc_lower.str.contains(
        'raw|dried|dry|uncooked|fresh$', regex=True, na=False
    )
    ref['is_cooked'] = desc_lower.str.contains(
        'boiled|cooked|baked|fried|roasted|grilled|steamed|stewed', regex=True, na=False
    )
    
    # Has valid macros?
    ref['has_macros'] = True
    for col in REF_MACROS.values():
        if col in ref.columns:
            ref['has_macros'] = ref['has_macros'] & ref[col].notna()
    
    print(f"  Reference entries with valid macros: {ref['has_macros'].sum()}/{len(ref)}")
    print(f"  Raw/dried entries: {ref['is_raw'].sum()}")
    print(f"  Cooked entries: {ref['is_cooked'].sum()}")
    
    return ref


def macro_deviation(off_row, ref_row):
    """Calculate max deviation across macros (per 100g)."""
    deviations = []
    for macro_key in OFF_MACROS:
        off_val = off_row.get(OFF_MACROS[macro_key], None)
        ref_val = ref_row.get(REF_MACROS[macro_key], None)
        
        if off_val is None or ref_val is None or pd.isna(off_val) or pd.isna(ref_val):
            continue
        
        off_val, ref_val = float(off_val), float(ref_val)
        if max(off_val, ref_val) < 1.0:
            deviations.append(abs(off_val - ref_val))
        else:
            deviations.append(abs(off_val - ref_val) / max(off_val, ref_val))
    
    if not deviations:
        return None  # Can't compare = unknown, not bad
    return max(deviations)


def main():
    # ===== Load data =====
    print("Loading data...")
    off_df = pd.read_parquet('data/processed/SILVER_OFF_semantic.parquet')
    ref_df = pd.read_parquet('data/processed/GOLD_REF_food_db.parquet')
    print(f"OFF products: {len(off_df)}")
    print(f"Reference foods: {len(ref_df)}")
    
    # Clean reference DB
    print("\nCleaning reference DB...")
    ref_df = clean_reference_db(ref_df)
    
    # ===== Encode names =====
    print(f"\nEncoding names with {MODEL_NAME}...")
    model = SentenceTransformer(MODEL_NAME)
    
    off_names = off_df['canonical_name'].tolist()
    ref_names = ref_df['description'].tolist()
    
    off_embeddings = model.encode(off_names, show_progress_bar=True, batch_size=128)
    ref_embeddings = model.encode(ref_names, show_progress_bar=True, batch_size=128)
    
    # Compute similarity matrix
    print(f"\nComputing similarity matrix ({len(off_names)} × {len(ref_names)})...")
    sim_matrix = cosine_similarity(off_embeddings, ref_embeddings)
    
    # ===== Matching: top-5 candidates, pick best with macro + state preference =====
    print("\nMatching with state-aware selection...")
    TOP_K = 5
    results = []
    
    for i in range(len(off_df)):
        off_row = off_df.iloc[i]
        
        # Get top-K similar reference foods
        top_k_idx = np.argsort(sim_matrix[i])[-TOP_K:][::-1]
        top_k_scores = sim_matrix[i][top_k_idx]
        
        best_match = None
        best_score = -1
        
        for rank, (ref_idx, sem_score) in enumerate(zip(top_k_idx, top_k_scores)):
            ref_row = ref_df.iloc[ref_idx]
            deviation = macro_deviation(off_row, ref_row)
            
            # Composite score: semantic similarity + macro agreement + state bonus
            composite = sem_score
            
            # Macro agreement bonus (if available)
            if deviation is not None:
                macro_bonus = max(0, (0.3 - deviation))  # Bonus if deviation < 30%
                composite += macro_bonus * 0.3
            
            # Raw/dried preference bonus (OFF products are as-sold)
            if ref_row.get('is_raw', False):
                composite += 0.05
            elif ref_row.get('is_cooked', False):
                composite -= 0.05
            
            if composite > best_score:
                best_score = composite
                best_match = {
                    'off_name': off_row['canonical_name'],
                    'ref_name': ref_row['description'],
                    'ref_food_category': ref_row.get('food_category', ''),
                    'semantic_score': round(float(sem_score), 4),
                    'macro_deviation': round(float(deviation), 4) if deviation is not None else None,
                    'is_raw_match': bool(ref_row.get('is_raw', False)),
                    'ref_idx': int(ref_idx),
                    'composite_score': round(float(composite), 4),
                }
        
        # Determine confidence tier
        sem = best_match['semantic_score']
        dev = best_match['macro_deviation']
        
        if sem >= 0.85:
            # Very high semantic → trust it even with moderate macro deviation
            if dev is None or dev <= 0.30:
                confidence = 'HIGH'
            elif dev <= 0.50:
                confidence = 'MEDIUM'
            else:
                confidence = 'LOW'
        elif sem >= 0.70:
            if dev is not None and dev <= 0.20:
                confidence = 'HIGH'
            elif dev is None or dev <= 0.35:
                confidence = 'MEDIUM'
            else:
                confidence = 'LOW'
        elif sem >= 0.55:
            if dev is not None and dev <= 0.15:
                confidence = 'MEDIUM'
            else:
                confidence = 'LOW'
        else:
            confidence = 'LOW'
        
        best_match['confidence'] = confidence
        results.append(best_match)
    
    results_df = pd.DataFrame(results)
    
    # ===== Report =====
    print(f"\n{'='*60}")
    print(f"MATCHING RESULTS")
    print(f"{'='*60}")
    for conf in ['HIGH', 'MEDIUM', 'LOW']:
        count = (results_df['confidence'] == conf).sum()
        pct = count / len(results_df) * 100
        print(f"  {conf}: {count:>6} ({pct:.1f}%)")
    
    for conf in ['HIGH', 'MEDIUM', 'LOW']:
        subset = results_df[results_df['confidence'] == conf]
        if len(subset) == 0:
            continue
        print(f"\n--- {conf} confidence examples (showing 15) ---")
        samples = subset.sample(min(15, len(subset)), random_state=42)
        for _, row in samples.iterrows():
            dev_str = f"{row['macro_deviation']:.2f}" if row['macro_deviation'] is not None else "N/A "
            raw_flag = "🥩" if row['is_raw_match'] else "  "
            print(f"  [{row['semantic_score']:.3f} sim, {dev_str} dev] {raw_flag} "
                  f"{row['off_name'][:35]:<35} → {row['ref_name'][:38]}")
    
    # ===== Save =====
    matched_mask = results_df['confidence'].isin(['HIGH', 'MEDIUM'])
    matched_results = results_df[matched_mask]
    unmatched_results = results_df[~matched_mask]
    
    # Build output with inherited micronutrients
    matched_products = off_df.loc[matched_mask].copy()
    for col in ['ref_name', 'ref_food_category', 
                'match_confidence', 'semantic_score', 'macro_deviation']:
        if col == 'match_confidence':
            matched_products[col] = matched_results['confidence'].values
        elif col in matched_results.columns:
            matched_products[col] = matched_results[col].values
    
    for col in MICRONUTRIENT_COLS:
        if col in ref_df.columns:
            matched_products[col] = ref_df.iloc[matched_results['ref_idx'].values][col].values
    
    matched_products.to_parquet('data/processed/GOLD_OFF_enriched.parquet', index=False)
    
    # Save unmatched
    unmatched_out = off_df.loc[~matched_mask][['canonical_name', 'calories', 'protein', 'carbohydrate', 'fat']].copy()
    unmatched_out['best_ref_name'] = unmatched_results['ref_name'].values
    unmatched_out['semantic_score'] = unmatched_results['semantic_score'].values
    unmatched_out['macro_deviation'] = unmatched_results['macro_deviation'].values
    unmatched_out.to_parquet('data/processed/GOLD_OFF_unmatched.parquet', index=False)
    unmatched_out.to_csv('data/processed/GOLD_OFF_unmatched.csv', index=False)
    
    print(f"\n{'='*60}")
    print(f"SAVED")
    print(f"{'='*60}")
    print(f"  Matched (HIGH+MEDIUM): {len(matched_products)} products")
    print(f"  Unmatched (LOW): {len(unmatched_out)} products")


if __name__ == '__main__':
    main()
