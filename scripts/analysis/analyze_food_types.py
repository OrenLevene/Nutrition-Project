import pandas as pd

def analyze_types():
    parquet_path = "c:/Users/Oren Arie Levene/Nutrition Project/data/archive/UNUSED_base_nutrition_WITH_SAMPLES.parquet"
    print(f"Loading {parquet_path}...")
    df = pd.read_parquet(parquet_path)
    
    # 1. Filter Valid Foods
    valid_df = df[df['Energy (KCAL)'] > 0].copy()
    print(f"Total Valid Foods: {len(valid_df)}")
    
    # 2. Inspect Categories
    print("\n--- Top 20 Categories ---")
    counts = valid_df['food_category'].value_counts()
    print(counts.head(20))
    
    # 3. Simple Classification Heuristic
    # Note: This is an approximation. "Vegetables" category contains both raw carrots and canned carrots.
    
    # Broad classification mapping
    processed_keywords = [
        "Baked", "Sweets", "Snacks", "Fast Foods", "Meals", "Entrees", "Sausages", 
        "Luncheon Meats", "Breakfast Cereals", "Baby Foods", "Soups", "Sauces", "Beverages"
    ]
    
    raw_whole_keywords = [
        "Fruits", "Vegetables", "Beef", "Poultry", "Pork", "Lamb", "Finfish", "Shellfish",
        "Dairy", "Egg", "Nut", "Seed", "Cereal Grains", "Legumes"
    ]
    
    def classify(cat):
        if not isinstance(cat, str): return "Unknown"
        
        for k in processed_keywords:
            if k in cat: return "Processed/Prepared"
            
        for k in raw_whole_keywords:
            if k in cat: return "Raw/Whole/Minimally Processed"
            
        return "Other/Unknown"
        
    valid_df['type_group'] = valid_df['food_category'].apply(classify)
    
    print("\n--- Proportion by Group ---")
    group_counts = valid_df['type_group'].value_counts()
    total = len(valid_df)
    
    for group, count in group_counts.items():
        pct = (count / total) * 100
        print(f"{group}: {count} ({pct:.1f}%)")
        
    print("\n--- Breakdown of 'Other/Unknown' ---")
    unknowns = valid_df[valid_df['type_group'] == "Other/Unknown"]['food_category'].value_counts()
    print(unknowns.head(10))

if __name__ == "__main__":
    analyze_types()
