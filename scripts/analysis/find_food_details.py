import pandas as pd

def find_food():
    df = pd.read_parquet("data/processed/clean_nutrition.parquet")
    
    # Broad search
    matches = df[
        df['description'].str.contains("soy", case=False, na=False) & 
        df['description'].str.contains("flour", case=False, na=False)
    ]
    
    print(f"Found {len(matches)} matches for 'soy' + 'flour':")
    
    if not matches.empty:
        for idx, row in matches.iterrows():
            print("\n--- Food Details ---")
            print(f"FDC ID: {row.get('fdc_id', 'N/A')}")
            print(f"Description: {row['description']}")
            print(f"Category: {row.get('food_category', 'N/A')}")
            print("\nNutrients (per 100g):")
            # Print non-zero nutrients
            for col in df.columns:
                if col not in ['fdc_id', 'description', 'food_category_id', 'food_category', 'publication_date']:
                    val = row[col]
                    if isinstance(val, (int, float)) and val > 0:
                        print(f"  {col}: {val}")

if __name__ == "__main__":
    find_food()
