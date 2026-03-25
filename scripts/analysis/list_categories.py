import pandas as pd

def list_categories():
    df = pd.read_parquet("data/processed/clean_nutrition.parquet")
    cats = df['food_category'].unique()
    print("Categories:")
    for c in sorted(cats):
        print(f" - {c}")

if __name__ == "__main__":
    list_categories()
