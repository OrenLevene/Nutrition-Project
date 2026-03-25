import pandas as pd

def analyze_keywords():
    df = pd.read_parquet("data/processed/clean_nutrition.parquet")
    
    keywords = [
        "nutrition bar", "protein bar", "energy bar", "snack bar",
        "shake", "powder", "formula", "beverage", "supplement", 
        "meal replacement", "baby food", "infant formula"
    ]
    
    print(f"Total Foods: {len(df)}")
    
    combined_mask = pd.Series([False] * len(df))
    
    for word in keywords:
        matches = df[df['description'].str.contains(word, case=False, na=False)]
        count = len(matches)
        print(f"'{word}': {count} matches")
        if count > 0:
            print(f"  Examples: {matches['description'].head(3).tolist()}")
            
        combined_mask = combined_mask | df['description'].str.contains(word, case=False, na=False)
        
    print(f"\nTotal items to be removed: {combined_mask.sum()}")
    print(f"Remaining items: {len(df) - combined_mask.sum()}")

if __name__ == "__main__":
    analyze_keywords()
