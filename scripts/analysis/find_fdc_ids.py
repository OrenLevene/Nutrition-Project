import pandas as pd

def find_ids():
    parquet_path = "c:/Users/Oren Arie Levene/Nutrition Project/data/archive/UNUSED_base_nutrition_WITH_SAMPLES.parquet"
    print(f"Loading {parquet_path}...")
    df = pd.read_parquet(parquet_path)
    
    # Problem foods from previous run:
    # OIL, SUNFLOWER
    # ground flaxseed meal
    # Sorghum bran, white, unenriched, dry, raw
    targets = ["Hummus"]
    
    print(f"\nSearching for {len(targets)} targets...\n")
    
    for target in targets:
        # Case insensitive partial match
        matches = df[df['description'].str.contains(target, case=False, na=False)]
        
        if not matches.empty:
            print(f"--- Matches for '{target}' ---")
            for idx, row in matches.iterrows():
                print(f"ID: {row['fdc_id']} | Name: {row['description']} | Kcal: {row.get('Energy (KCAL)', 'N/A')}")
        else:
            print(f"No match for '{target}'")

if __name__ == "__main__":
    find_ids()
