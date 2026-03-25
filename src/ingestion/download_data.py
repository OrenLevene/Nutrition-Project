import os
import requests
import zipfile
import io
import pandas as pd
import shutil

# Configuration
DATA_DIR = "usda_data"
OUTPUT_FILE = "base_nutrition.parquet"

# URLs for datasets (CSV format)
# Using the links identified from FDC documentation
DATASETS = {
    "Foundation": "https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_foundation_food_csv_2025-04-24.zip",
    "SR_Legacy": "https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_sr_legacy_food_csv_2018-04.zip"
}

def download_and_extract(url, extract_to):
    """Downloads a zip file and extracts it to the specific directory."""
    print(f"Downloading from {url}...")
    try:
        r = requests.get(url, stream=True)
        r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(extract_to)
        print(f"Extracted to {extract_to}")
    except Exception as e:
        print(f"Error downloading {url}: {e}")

def load_csv(base_path, filename, encoding='utf-8'):
    """Finds and loads a CSV file from a directory (searching recursively)."""
    for root, dirs, files in os.walk(base_path):
        if filename in files:
            path = os.path.join(root, filename)
            # USDA CSVs often have generic encoding issues, latin1 is safer fallback
            try:
                return pd.read_csv(path, encoding=encoding)
            except UnicodeDecodeError:
                return pd.read_csv(path, encoding='latin1')
    return None

def process_dataset(dataset_name, extract_path):
    print(f"Processing {dataset_name}...")
    
    # Load core tables
    foods = load_csv(extract_path, "food.csv")
    nutrients = load_csv(extract_path, "nutrient.csv")
    food_nutrients = load_csv(extract_path, "food_nutrient.csv")
    categories = load_csv(extract_path, "food_category.csv")
    
    if foods is None or nutrients is None or food_nutrients is None:
        print(f"Error: Missing CSV files for {dataset_name}")
        return None

    # Filter foods
    # fdc_id, data_type, description, food_category_id, publication_date
    if 'food_category_id' in foods.columns:
        foods = foods[['fdc_id', 'data_type', 'description', 'food_category_id', 'publication_date']]
        
        # Merge category names if available
        if categories is not None:
             # categories: id, code, description
             # rename description to category_name
             categories = categories[['id', 'description']]
             categories.columns = ['food_category_id', 'food_category']
             
             foods = foods.merge(categories, on='food_category_id', how='left')
    else:
        # Fallback if specific dataset lacks category
        foods = foods[['fdc_id', 'data_type', 'description', 'publication_date']]

    # Prepare nutrient metadata
    # id, name, unit_name, nutrient_nbr
    nutrients = nutrients[['id', 'name', 'unit_name']]
    nutrients.columns = ['nutrient_id', 'nutrient_name', 'unit_name']

    # Merge nutrient values with nutrient metadata
    # food_nutrient.csv has: id, fdc_id, nutrient_id, amount
    # Merge to get names:
    merged_nutrients = food_nutrients.merge(nutrients, on='nutrient_id', how='left')

    # Create a nice column name "Nutrient (Unit)"
    merged_nutrients['col_name'] = merged_nutrients['nutrient_name'] + " (" + merged_nutrients['unit_name'] + ")"

    # Pivot: Rows = fdc_id, Cols = nutrient name, Values = amount
    # This might be memory intensive but Base foods are < 50k rows, so it should be fine.
    print(f"  Pivoting nutrients for {len(foods)} foods...")
    nutrient_pivot = merged_nutrients.pivot_table(
        index='fdc_id', 
        columns='col_name', 
        values='amount', 
        aggfunc='first' # Duplicate nutrients shouldn't exist, but safe to pick first
    )

    # Join back to foods
    full_data = foods.merge(nutrient_pivot, on='fdc_id', how='left')
    
    return full_data

def main():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    all_data = []

    for name, url in DATASETS.items():
        extract_path = os.path.join(DATA_DIR, name)
        if not os.path.exists(extract_path):
            download_and_extract(url, extract_path)
            
        df = process_dataset(name, extract_path)
        if df is not None:
             all_data.append(df)

    if all_data:
        print("Merging datasets...")
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Save
        final_df.to_parquet(OUTPUT_FILE, index=False)
        print(f"Saved {len(final_df)} records to {OUTPUT_FILE}")
        
        # Save sample CSV
        final_df.head(100).to_csv("data/archive/base_nutrition_sample.csv", index=False)
        print("Saved sample to base_nutrition_sample.csv")
        
        # Clean up massive data dir if desired, but keeping for now.
    else:
        print("Failed to process data.")

if __name__ == "__main__":
    main()
