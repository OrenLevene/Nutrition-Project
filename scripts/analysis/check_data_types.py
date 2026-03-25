import pandas as pd

def check_invalid_types():
    parquet_path = "c:/Users/Oren Arie Levene/Nutrition Project/data/archive/UNUSED_base_nutrition_WITH_SAMPLES.parquet"
    print(f"Loading {parquet_path}...")
    df = pd.read_parquet(parquet_path)
    
    # Identify 0-calorie foods
    zero_cal = df[ (df['Energy (KCAL)'].fillna(0) <= 0) & 
                   (df['Energy (Atwater General Factors) (KCAL)'].fillna(0) <= 0) &
                   (df['Energy (Atwater Specific Factors) (KCAL)'].fillna(0) <= 0) ]
                   
    print(f"\nTotal 0-Calorie Foods: {len(zero_cal)}")
    print("\nBreakdown by Data Type:")
    print(zero_cal['data_type'].value_counts())
    
    print("\n--- Valid Data Types (Foods with Calories) ---")
    valid = df[~df.index.isin(zero_cal.index)]
    print(valid['data_type'].value_counts())

if __name__ == "__main__":
    check_invalid_types()
