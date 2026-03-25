import requests
import os
import json
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("USDA_API_KEY")

def check_ids():
    # IDs from invalid_calorie_foods.csv (Branded?)
    # 319874: HUMMUS, SABRA CLASSIC
    # 320025: MILK, 2%
    
    ids_to_check = [319874, 320025]
    
    base_url = "https://api.nal.usda.gov/fdc/v1/foods"
    params = {
        "api_key": API_KEY,
        "fdcIds": ids_to_check,
        "format": "full"
    }
    
    print(f"Querying USDA API for IDs: {ids_to_check}...")
    response = requests.get(base_url, params=params)
    
    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        return

    data = response.json()
    
    for food in data:
        print(f"\n--- FDC ID: {food['fdcId']} ---")
        print(f"Description: {food['description']}")
        print("Nutrients:")
        found_energy = False
        for nut in food.get('foodNutrients', []):
            name = nut.get('nutrient', {}).get('name', 'Unknown')
            amount = nut.get('amount')
            unit = nut.get('nutrient', {}).get('unitName')
            
            if "Energy" in name:
                print(f"  > {name}: {amount} {unit}")
                found_energy = True
            elif "Vitamin" in name or "Fiber" in name:
                # Print a few others to verify data exists at all
                pass 
                
        if not found_energy:
            print("  > NO ENERGY/CALORIE DATA FOUND IN RAW API RESPONSE")

if __name__ == "__main__":
    if not API_KEY:
        print("Error: No API Key found in env")
    else:
        check_ids()
