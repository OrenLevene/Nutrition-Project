import pandas as pd
import numpy as np
import sys
import warnings
from pathlib import Path

warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)

# Paths
DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_SILVER = DATA_PIPELINE_DIR / 'data' / 'reference' / 'silver'
DATA_GOLD = DATA_PIPELINE_DIR / 'data' / 'reference' / 'gold'


def merge_databases(foodb_df, cofid_df):
    """Merge FooDB and CoFID, avoiding duplicates."""
    print("\nMerging FooDB and CoFID databases...")
    all_columns = list(set(foodb_df.columns.tolist() + cofid_df.columns.tolist()))
    
    for col in all_columns:
        if col not in foodb_df.columns: foodb_df[col] = np.nan
        if col not in cofid_df.columns: cofid_df[col] = np.nan
            
    foodb_df = foodb_df[all_columns]
    cofid_df = cofid_df[all_columns]
    
    merged = pd.concat([foodb_df, cofid_df], ignore_index=True)
    merged['name_normalized'] = merged['description'].str.lower().str.strip()
    merged = merged.drop_duplicates(subset=['name_normalized'], keep='first')
    merged = merged.drop(columns=['name_normalized'])
    
    print(f"  Merged: {len(merged)} foods, {len(merged.columns)} nutrient columns")
    return merged


def consolidate_duplicate_columns(df):
    """Consolidate duplicate nutrient columns with different naming conventions."""
    print("\nConsolidating duplicate columns...")
    consolidation_map = {
        'Protein (g)': ['Proteins (MG_100G)', 'Protein', 'Protein (G)', 'Protein (g)'],
        'Fat (g)': ['Fat (MG_100G)', 'Fat', 'Fat (G)', 'Fatty acids (MG_100 G)', 'Fatty acids (MG_100G)', 'Total lipid (fat) (G)', 'Fat (g)'],
        'Carbohydrate (g)': ['Carbohydrate', 'Carbohydrate (G)', 'Carbohydrates (MG_100G)', 'Carbohydrate (MG_100G)', 'Carbohydrate, by difference (G)', 'Carbohydrate (g)'],
        'Fiber (g)': ['Fibre (G)', 'Fiber (MG_100G)', 'Dietary fibre', 'AOAC fibre', 'Fiber (dietary) (MG_100 G)', 'Fiber (dietary) (MG_100G)', 'Non-starch polysaccharide', 'Fiber, total dietary (G)', 'Fiber (g)'],
        'Sugars (g)': ['Total sugars', 'Total sugars (G)', 'Sugars, Total (G)', 'Sugars (g)'],
        'Starch (g)': ['Starch', 'Starch (G)', 'Starch (g)'],
        'Water (g)': ['Water', 'Water (G)', 'Water (g)'],
        'Ash (g)': ['Ash (MG_100 G)', 'Ash (G)', 'Ash (g)'],
        'Calories (kcal)': ['Energy (KCAL)', 'Energy (MG_100G)', 'Energy (MG_100 G)', 'kcal', 'Calories (kcal)'],
        'Energy (kJ)': ['kJ', 'Energy (kJ)'],
        'Calcium (mg)': ['Calcium, Ca (MG)', 'Ca (MG)', 'Calcium (MG)', 'Calcium', 'Calcium (mg)'],
        'Iron (mg)': ['Iron, Fe (MG)', 'Fe (MG)', 'Iron (MG)', 'Iron', 'Iron (mg)'],
        'Magnesium (mg)': ['Magnesium, Mg (MG)', 'Mg (MG)', 'Magnesium (MG)', 'Magnesium', 'Magnesium (mg)'],
        'Phosphorus (mg)': ['Phosphorus, P (MG)', 'P (MG)', 'Phosphorus (MG)', 'Phosphorus', 'Phosphorus (mg)'],
        'Potassium (mg)': ['Potassium, K (MG)', 'K (MG)', 'Potassium (MG)', 'Potassium', 'Potassium (mg)'],
        'Sodium (mg)': ['Sodium, Na (MG)', 'Na (MG)', 'Sodium (MG)', 'Sodium', 'Sodium (mg)'],
        'Zinc (mg)': ['Zinc, Zn (MG)', 'Zn (MG)', 'Zinc (MG)', 'Zinc', 'Zinc (mg)'],
        'Copper (mg)': ['Copper, Cu (MG)', 'Cu (MG)', 'Copper (MG)', 'Copper', 'Copper (mg)'],
        'Manganese (mg)': ['Manganese, Mn (MG)', 'Mn (MG)', 'Manganese (MG)', 'Manganese', 'Manganese (mg)'],
        'Selenium (ug)': ['Selenium, Se (UG)', 'Se (MG)', 'Selenium (MG)', 'Selenium', 'Selenium (ug)'],
        'Iodine (ug)': ['Iodine, I (UG)', 'I (MG)', 'Iodine (MG)', 'Iodine', 'Iodine (ug)'],
        'Chloride (mg)': ['Chloride (MG)', 'Cl (MG)', 'Chloride', 'Chloride (mg)'],
        'Vitamin A (ug)': ['Vitamin A, RAE (UG)', 'Retinol equivalent (UG)', 'Vitamin A (UG)', 'Total retinol equivalent', 'Vitamin A (ug)'],
        'Retinol (ug)': ['Retinol (UG)', 'All-trans-retinol', 'Retinol', 'Retinol (ug)'],
        'Vitamin C (mg)': ['Vitamin C, total ascorbic acid (MG)', 'Vitamin C (MG)', 'Vit C', 'Vitamin C (mg)'],
        'Vitamin D (ug)': ['Vitamin D (D2 + D3) (UG)', 'Vitamin D (UG)', 'Vitamin D', '25-hydroxy vitamin D3', 'Vitamin D (ug)'],
        'Vitamin E (mg)': ['Vitamin E (alpha-tocopherol) (MG)', 'Vitamin E (MG)', 'Vitamin E', 'Alpha-tocopherol', 'Vitamin E (mg)'],
        'Vitamin K (ug)': ['Vitamin K (phylloquinone) (UG)', 'Vitamin K1 (UG)', 'Vitamin K1', 'Phylloquinone', 'Vitamin K (ug)'],
        'Thiamin (mg)': ['Thiamin (MG)', 'Thiamin', 'Thiamin (mg)'],
        'Riboflavin (mg)': ['Riboflavin (MG)', 'Riboflavin', 'Riboflavin (mg)'],
        'Niacin (mg)': ['Niacin (MG)', 'Niacin', 'Niacin equivalent', 'Niacin (mg)'],
        'Vitamin B6 (mg)': ['Vitamin B-6 (MG)', 'Vitamin B6 (MG)', 'Vitamin B6', 'Vitamin B6 (mg)'],
        'Vitamin B12 (ug)': ['Vitamin B-12 (UG)', 'Vitamin B12 (UG)', 'Vitamin B12', 'Vitamin B12 (ug)'],
        'Folate (ug)': ['Folate, total (UG)', 'Folate (UG)', 'Folate', '5-methyl folate', 'Folate (ug)'],
        'Pantothenic acid (mg)': ['Pantothenic acid (MG)', 'Pantothenate (MG)', 'Pantothenic acid (mg)'],
        'Biotin (ug)': ['Biotin (UG)', 'Biotin', 'Biotin (ug)'],
        'Carotene, beta (ug)': ['Carotene, beta (UG)', 'Beta-carotene', 'Carotene', 'Carotene, beta (ug)'],
        'Carotene, alpha (ug)': ['Carotene, alpha (UG)', 'Alpha-carotene', 'Carotene, alpha (ug)'],
        'Lycopene (ug)': ['Lycopene (UG)', 'Lycopene', 'Lycopene (ug)'],
        'Lutein + zeaxanthin (ug)': ['Lutein + zeaxanthin (UG)', 'Lutein', 'Lutein + zeaxanthin (ug)'],
        'Cholesterol (mg)': ['Cholesterol (MG)', 'Cholesterol', 'Cholesterol (mg)'],
        'Alcohol (g)': ['Alcohol, ethyl (G)', 'Alcohol (G)', 'Alcohol', 'Alcohol (g)'],
        'Caffeine (mg)': ['Caffeine (MG)', 'Caffeine', 'Caffeine (mg)'],
        'Saturated Fat (g)': ['Fatty acids, total saturated (G)', 'Satd FA /100g FA', 'Saturated fatty acids (G)', 'Saturated fatty acids per 100g food', 'Saturated Fat (g)'],
        'Monounsaturated Fat (g)': ['Fatty acids, total monounsaturated (G)', 'Monounsaturated fatty acids (G)', 'Monounsaturated fatty acids per 100g food', 'cis-Monounsaturated fatty acids /100g Food', 'Monounsaturated Fat (g)'],
        'Polyunsaturated Fat (g)': ['Fatty acids, total polyunsaturated (G)', 'Polyunsaturated fatty acids (G)', 'Polyunsaturated fatty acids per 100g food', 'cis-Polyunsaturated fatty acids /100g Food', 'Polyunsaturated Fat (g)'],
        'Trans Fat (g)': ['Fatty acids, total trans (G)', 'Total Trans fatty acids per 100g food', 'trans monounsaturated fatty acids per 100g food (G)', 'trans polyunsaturated fatty acid per 100g food (G)', 'Trans Fat (g)'],
        'ALA (g)': ['PUFA 18:3 n-3 (ALA) (G)', 'cis n-3 Octadecatrienoic acid per 100g food (G)', 'ALA (g)'],
        'EPA (g)': ['PUFA 20:5 n-3 (EPA) (G)', 'cis n-3 Eicosapentaenoic acid per 100g food (G)', 'Eicosapentaenoic acid per 100g food (G)', 'EPA (g)'],
        'DHA (g)': ['PUFA 22:6 n-3 (DHA) (G)', 'cis n-3 Docosahexaenoic acid (DHA) per 100g food (G)', 'Docosahexaenoic acid (DHA) per 100g food (G)', 'DHA (g)'],
        'Omega-3 (g)': ['Total Omega-3 (G)', 'Total n-3 polyunsaturated fatty acids per 100g food', 'Omega-3 (g)'],
        'Omega-6 (g)': ['Total Omega-6 (G)', 'Total n-6 polyunsaturated fatty acids per 100g food', 'cis n-6 (G)', 'Omega-6 (g)'],
        'Phytosterols (mg)': ['Phytosterols (MG)', 'Total Phytosterols', 'Phytosterol', 'Phytosterols (mg)']
    }
    
    consolidated_count = 0
    for target_col, source_cols in consolidation_map.items():
        existing_sources = [c for c in source_cols if c in df.columns and c != target_col]
        if not existing_sources:
            continue
        if target_col not in df.columns:
            df[target_col] = np.nan
        for source_col in existing_sources:
            if 'MG_100G' in source_col:
                if '(g)' in target_col:
                    df[target_col] = df[target_col].fillna(df[source_col] / 1000)
                else:
                    df[target_col] = df[target_col].fillna(df[source_col])
            else:
                df[target_col] = df[target_col].fillna(df[source_col])
            df = df.drop(columns=[source_col])
            consolidated_count += 1
            
    print(f"  Consolidated {consolidated_count} duplicate columns")
    return df


def build_gold_reference():
    print("=" * 60)
    print("STAGE: GOLD — Build final Reference Database")
    print("=" * 60)
    
    # Load Silver sources
    try:
        silver_foodb = pd.read_csv(DATA_SILVER / 'foodb.csv')
        silver_cofid = pd.read_csv(DATA_SILVER / 'cofid.csv')
        silver_usda = pd.read_csv(DATA_SILVER / 'usda.csv')
    except FileNotFoundError as e:
        print(f"Error: Missing Silver source file. {e}")
        print("Please run pipeline/reference/silver/build_silver.py first.")
        return
        
    print(f"Loaded FooDB Silver: {len(silver_foodb)} foods")
    print(f"Loaded CoFID Silver: {len(silver_cofid)} foods")
    print(f"Loaded USDA Silver: {len(silver_usda)} foods")
    
    # Merge FooDB and CoFID first to resolve duplicate columns internally
    foodb_cofid = merge_databases(silver_foodb, silver_cofid)
    foodb_cofid = consolidate_duplicate_columns(foodb_cofid)
    
    # Extract CoFID entries only (FooDB entries overlap with USDA heavily)
    cofid = foodb_cofid[foodb_cofid['data_source'] == 'UK_CoFID'].copy()
    print(f"  CoFID subset extracted: {len(cofid)} foods")
    
    # Find common columns between USDA and CoFID
    usda_cols_lower = {c.lower(): c for c in silver_usda.columns}
    cofid_cols_lower = {c.lower(): c for c in cofid.columns}
    
    column_mapping = {}
    for cofid_lower, cofid_col in cofid_cols_lower.items():
        if cofid_lower in usda_cols_lower:
            usda_col = usda_cols_lower[cofid_lower]
            if cofid_col != usda_col:
                column_mapping[cofid_col] = usda_col
                
    cofid_renamed = cofid.rename(columns=column_mapping)
    
    # Key columns to keep (must exist in both)
    keep_cols = ['description'] + [c for c in silver_usda.columns if c != 'description' and c in cofid_renamed.columns]
    print(f"Shared nutrient columns: {len(keep_cols) - 1}")
    
    # Prepare subsets
    usda_subset = silver_usda[keep_cols].copy()
    usda_subset['data_source'] = 'USDA'
    
    cofid_subset = cofid_renamed[[c for c in keep_cols if c in cofid_renamed.columns]].copy()
    cofid_subset['data_source'] = 'UK_CoFID'
    
    # Combine and Deduplicate
    combined = pd.concat([usda_subset, cofid_subset], ignore_index=True)
    dupes = combined['description'].str.lower().duplicated(keep='first')
    print(f"Duplicate names removed (preferring USDA): {dupes.sum()}")
    combined = combined[~dupes].reset_index(drop=True)
    
    print(f"\nGOLD reference DB ready: {len(combined)} foods")
    print(f"  USDA: {(combined['data_source']=='USDA').sum()}")
    print(f"  CoFID: {(combined['data_source']=='UK_CoFID').sum()}")
    
    # Check Macro coverage
    print("\nMacro Coverage:")
    for col in ['Calories (kcal)', 'Protein (g)', 'Carbohydrate (g)', 'Fat (g)']:
        if col in combined.columns:
            pct = combined[col].notna().sum() / len(combined) * 100
            print(f"  {col:<45} {pct:.1f}%")
            
    # Save Gold
    DATA_GOLD.mkdir(parents=True, exist_ok=True)
    gold_path = DATA_GOLD / 'gold_ref_food_db.csv'
    combined.to_csv(gold_path, index=False)
    print(f"\nSUCCESS: Saved GOLD reference database: {gold_path} ({len(combined)} foods)")


if __name__ == '__main__':
    build_gold_reference()
