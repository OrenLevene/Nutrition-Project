"""Compare all processed nutrition data files to find the right reference DB."""
import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

files = [
    'data/processed/BRONZE_REF_foodb_cofid_raw.parquet',
    'data/processed/BRONZE_REF_usda.parquet',
    'data/processed/clean_nutrition.parquet',
    'data/processed/real_food_nutrition.parquet',
    'data/processed/refined_nutrition.parquet',
    'data/processed/nutrition_data.parquet',
]

for f in files:
    try:
        df = pd.read_parquet(f)
        print(f"\n{'='*60}")
        print(f"{f}")
        print(f"{'='*60}")
        print(f"Shape: {df.shape[0]} rows × {df.shape[1]} cols")
        
        # Find name/description column
        name_col = None
        for c in ['description', 'name', 'food_name', 'product_name', 'canonical_name']:
            if c in df.columns:
                name_col = c
                break
        if name_col:
            print(f"Name column: '{name_col}'")
            print(f"  Examples: {df[name_col].head(5).tolist()}")
        
        # Source info
        if 'data_source' in df.columns:
            print(f"Sources: {df['data_source'].value_counts().to_dict()}")
        if 'food_group' in df.columns:
            print(f"Food groups: {df['food_group'].nunique()} unique")
            print(f"  Top: {df['food_group'].value_counts().head(5).to_dict()}")
        
        # Key nutrient coverage
        key_nutrients = ['Energy (KCAL)', 'calories', 'Protein (G)', 'protein',
                        'Calcium, Ca (MG)', 'Iron, Fe (MG)', 'Vitamin C, total ascorbic acid (MG)',
                        'Total Vitamin D', 'Zinc, Zn (MG)']
        print(f"Key nutrient coverage:")
        for n in key_nutrients:
            if n in df.columns:
                pct = df[n].notna().sum() / len(df) * 100
                print(f"  {n:<45} {pct:5.1f}%")
    except Exception as e:
        print(f"\n{f}: CANNOT LOAD — {e}")
