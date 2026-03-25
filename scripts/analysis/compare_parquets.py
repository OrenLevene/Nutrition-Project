import pandas as pd

files = [
    'database_builder/real_food_nutrition.parquet',
    'database_builder/clean_nutrition.parquet',
    'database_builder/base_nutrition.parquet'
]

for f in files:
    try:
        print(f"\n--- Checking {f} ---")
        df = pd.read_parquet(f)
        print(f"Total rows: {len(df)}")
        
        if 'data_type' in df.columns:
            print("Data types:", df['data_type'].unique())
        
        # Check for restaurant foods
        restaurant_count = len(df[df['food_category'].astype(str).str.contains("Restaurant", case=False, na=False)])
        print(f"Restaurant foods count: {restaurant_count}")
        
    except Exception as e:
        print(f"Could not read {f}: {e}")
