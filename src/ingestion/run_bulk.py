import os
import time
import pandas as pd
from usda_ingestion import USDAClient, process_food_item

# Configuration
API_KEY = os.getenv("USDA_API_KEY")
OUTPUT_FILE = "base_foods_db.parquet"
DATA_TYPES = ["Foundation", "SR Legacy"] # Focus on base foods
PAGES_PER_QUERY = 3 # Adjust to get more/less depth per category

# List of broad categories to query for "base" foods
CATEGORIES = [
    "Vegetables", "Fruits", "Beef", "Chicken", "Pork", "Fish", 
    "Legumes", "Nuts", "Seeds", "Dairy", "Egg", "Grains", 
    "Tofu", "Tempeh", "Pasta", "Tomato", "Butter", "Cheese", 
    "Milk", "Yogurt", "Bread", "Rice", "Oats", "Quinoa", 
    "Chickpeas", "Lentils", "Beans", "Potato", "Sweet Potato",
    "Onion", "Garlic", "Spinach", "Kale", "Broccoli", "Cauliflower",
    "Apple", "Banana", "Orange", "Berries", "Avocado",
]

def run_bulk_ingestion():
    if not API_KEY:
        print("Please set USDA_API_KEY in .env")
        return

    client = USDAClient(API_KEY)
    all_foods_data = []

    print(f"Starting bulk ingestion for {len(CATEGORIES)} categories...")
    print(f"Targeting Data Types: {DATA_TYPES}")
    print(f"Pages per category: {PAGES_PER_QUERY}")

    for category in CATEGORIES:
        print(f"\nProcessing category: '{category}'")
        
        for page in range(1, PAGES_PER_QUERY + 1):
            print(f"  Fetching page {page} for '{category}'...")
            search_result = client.search_foods(category, page_size=50, page_number=page, data_types=DATA_TYPES)
            
            if not search_result or "foods" not in search_result:
                print(f"    No results for '{category}' page {page}.")
                break
            
            search_items = search_result["foods"]
            if not search_items:
                print(f"    No more items for '{category}'.")
                break
                
            fdc_ids = [item["fdcId"] for item in search_items]
            
            # Batch fetch details
            chunk_size = 20
            for i in range(0, len(fdc_ids), chunk_size):
                batch_ids = fdc_ids[i:i + chunk_size]
                # print(f"    Fetching details for {len(batch_ids)} items...")
                try:
                    details = client.get_foods_batch(batch_ids)
                    for food in details:
                        # Append a category tag for easier querying later
                        flat_item = process_food_item(food)
                        flat_item["search_category"] = category
                        all_foods_data.append(flat_item)
                except Exception as e:
                    print(f"    Error processing batch: {e}")
                
                time.sleep(0.5) # Rate limit niceness

    if all_foods_data:
        df = pd.DataFrame(all_foods_data)
        
        # Save/Append to Parquet
        if os.path.exists(OUTPUT_FILE):
            print(f"Appending to existing {OUTPUT_FILE}...")
            try:
                existing_df = pd.read_parquet(OUTPUT_FILE)
                df = pd.concat([existing_df, df], ignore_index=True)
                # Deduplicate by FDC ID, keeping the latest one
                # Also deduplicate if same food found via different category search
                df = df.drop_duplicates(subset=["fdc_id"], keep="last")
            except Exception as e:
                print(f"Error reading existing file: {e}. Overwriting.")
        
        df.to_parquet(OUTPUT_FILE, index=False)
        
        # Also save a CSV for visual inspection if user wants
        csv_file = OUTPUT_FILE.replace(".parquet", ".csv")
        df.to_csv(csv_file, index=False)
        
        print(f"\nSuccess! Saved {len(df)} total records to {OUTPUT_FILE} and {csv_file}")
        print(df.head())
    else:
        print("\nNo data collected.")

if __name__ == "__main__":
    run_bulk_ingestion()
