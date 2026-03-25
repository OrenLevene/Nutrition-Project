import pandas as pd

def check_columns():
    parquet_path = "c:/Users/Oren Arie Levene/Nutrition Project/data/archive/UNUSED_base_nutrition_WITH_SAMPLES.parquet"
    print(f"Loading {parquet_path}...")
    df = pd.read_parquet(parquet_path)
    
    print("\n--- All Columns ---")
    for c in df.columns:
        print(c)
        
    print("\n--- Sample Data for Hummus (319874) ---")
    hummus = df[df['fdc_id'] == 319874]
    if not hummus.empty:
        # Print all non-null columns
        row = hummus.iloc[0]
        for col in df.columns:
            val = row[col]
            if pd.notna(val) and val != 0:
                print(f"{col}: {val}")
    else:
        print("Hummus ID 319874 not found in parquet.")

if __name__ == "__main__":
    check_columns()
