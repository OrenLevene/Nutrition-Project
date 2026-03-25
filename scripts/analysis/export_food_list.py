import pandas as pd

def export_list():
    df = pd.read_parquet("data/processed/clean_nutrition.parquet")
    
    # Select relevance columns
    subset = df[['fdc_id', 'description', 'food_category']]
    
    # Sort for easier reading
    subset = subset.sort_values(by=['food_category', 'description'])
    
    output_file = 'food_list_for_review.txt'
    with open(output_file, 'w', encoding='utf-8') as f:
        for idx, row in subset.iterrows():
            f.write(f"{row['fdc_id']}|{row['food_category']}|{row['description']}\n")
            
    print(f"Exported {len(subset)} items to {output_file}")

if __name__ == "__main__":
    export_list()
