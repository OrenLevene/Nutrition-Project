import os
import time
import requests
import pandas as pd
import argparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
API_KEY = os.getenv("USDA_API_KEY")
BASE_URL = "https://api.nal.usda.gov/fdc/v1"

class USDAClient:
    def __init__(self, api_key):
        self.api_key = api_key
        if not self.api_key:
            raise ValueError("API Key not found. Please set USDA_API_KEY in .env")

    def search_foods(self, query, page_size=50, page_number=1, data_types=None):
        """Search for foods by query string."""
        url = f"{BASE_URL}/foods/search"
        params = {
            "query": query,
            "pageSize": page_size,
            "pageNumber": page_number,
            "api_key": self.api_key
        }
        if data_types:
            # data_type is passed as a comma-separated string or multiple params
            # USDA API expects comma separated for dataType param
            params["dataType"] = ",".join(data_types)
            
        response = requests.get(url, params=params)
        if response.status_code != 200:
            print(f"Error searching foods: {response.status_code} - {response.text}")
            return None
        return response.json()

    def get_foods_batch(self, fdc_ids):
        """Fetch details for a list of FDC IDs using POST."""
        url = f"{BASE_URL}/foods"
        params = {"api_key": self.api_key}
        json_body = {
            "fdcIds": fdc_ids,
            "format": "full" # Request full details
        }
        response = requests.post(url, params=params, json=json_body)
        if response.status_code != 200:
            print(f"Error fetching batch details: {response.status_code} - {response.text}")
            return []
        return response.json()

def process_food_item(food):
    """Flatten a food item into a dictionary for DataFrame."""
    item = {
        "fdc_id": food.get("fdcId"),
        "description": food.get("description"),
        "data_type": food.get("dataType"),
        "brand_owner": food.get("brandOwner"),
        "gtin_upc": food.get("gtinUpc"),
        "ingredients": food.get("ingredients"),
        "serving_size": food.get("servingSize"),
        "serving_size_unit": food.get("servingSizeUnit"),
        "household_serving": food.get("householdServingFullText")
    }

    # Extract Nutrients
    # We will prefix nutrients with "nut_" to avoid collisions
    for nutrient in food.get("foodNutrients", []):
        # Some endpoints return 'nutrient' object nested, some return flat properties
        # In 'foods' (details) endpoint:
        # structure is usually: { "nutrient": { "name": ..., "unitName": ... }, "amount": ... }
        # OR flattened in search results. 
        # We are using the details endpoint so we should check the structure.
        
        name = ""
        amount = 0.0
        unit = ""

        if "nutrient" in nutrient:
            name = nutrient["nutrient"].get("name")
            unit = nutrient["nutrient"].get("unitName")
            amount = nutrient.get("amount")
        else:
            # Fallback for flat structure if any
            name = nutrient.get("nutrientName")
            unit = nutrient.get("unitName")
            amount = nutrient.get("value")

        if name:
            col_name = f"nut_{name}_{unit}"
            item[col_name] = amount

    return item

def main():
    parser = argparse.ArgumentParser(description="USDA Food Data Ingestion")
    parser.add_argument("--query", type=str, required=True, help="Search query (e.g., 'apple')")
    parser.add_argument("--pages", type=int, default=1, help="Number of pages to fetch")
    parser.add_argument("--output", type=str, default="data/processed/nutrition_data.parquet", help="Output file path")
    parser.add_argument("--format", type=str, choices=["parquet", "csv"], default="parquet", help="Output format")
    parser.add_argument("--data-types", type=str, default="Foundation,SR Legacy", help="Comma-separated data types (e.g. 'Foundation,SR Legacy')")
    
    args = parser.parse_args()
    
    client = USDAClient(API_KEY)
    
    # Parse data types
    data_types = [dt.strip() for dt in args.data_types.split(",")] if args.data_types else None
    
    all_foods_data = []
    
    print(f"Starting ingestion for query: '{args.query}' - fetching {args.pages} pages...")
    print(f"  Targeting Data Types: {data_types}")
    
    for page in range(1, args.pages + 1):
        print(f"Fetching page {page}...")
        search_result = client.search_foods(args.query, page_size=50, page_number=page, data_types=data_types)
        
        if not search_result or "foods" not in search_result:
            print("No foods found or error occurred.")
            break
            
        search_items = search_result["foods"]
        if not search_items:
            print("No more items found.")
            break
            
        # Extract FDC IDs for batch fetching details
        # Note: Search results (search_items) already contain some nutrients, 
        # but 'get_foods_batch' (details endpoint) provides the most comprehensive data (ingredients, full nutrient profile).
        # We will fetch details for better quality data.
        fdc_ids = [item["fdcId"] for item in search_items]
        
        # Batch fetch details (chunks of 20 to be safe with standard limits/timeouts)
        # USDA docs don't strictly specify POST body limit, but smaller batches are safer.
        chunk_size = 20
        for i in range(0, len(fdc_ids), chunk_size):
            batch_ids = fdc_ids[i:i + chunk_size]
            print(f"  Fetching details for {len(batch_ids)} items...")
            details = client.get_foods_batch(batch_ids)
            
            for food in details:
                flat_item = process_food_item(food)
                all_foods_data.append(flat_item)
            
            time.sleep(0.5) # Slight delay to be nice to the API

    if all_foods_data:
        df = pd.DataFrame(all_foods_data)
        
        # Save to file
        if args.format == "parquet":
            if os.path.exists(args.output):
                print(f"Appending to existing {args.output}...")
                try:
                    existing_df = pd.read_parquet(args.output)
                    df = pd.concat([existing_df, df], ignore_index=True)
                    df = df.drop_duplicates(subset=["fdc_id"], keep="last")
                except Exception as e:
                    print(f"Error reading existing file: {e}. Creating new one.")
            df.to_parquet(args.output, index=False)
        
        elif args.format == "csv":
            # For CSV we'll just append if exists, or write new.
            mode = 'a' if os.path.exists(args.output) else 'w'
            header = not os.path.exists(args.output)
            df.to_csv(args.output, mode=mode, header=header, index=False)

        print(f"Saved {len(df)} records to {args.output}")
        print("Sample data:")
        print(df.head())
    else:
        print("No data collected.")

if __name__ == "__main__":
    main()
