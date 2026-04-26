from dataclasses import dataclass, field
from typing import Dict, List, Optional

@dataclass
class FoodItem:
    id: str
    name: str
    calories: float
    # Dictionary of nutrient name -> amount per 100g (or per item unit)
    nutrients: Dict[str, float] = field(default_factory=dict)
    
    # Optional metadata like serving size if needed later
    serving_size: float = 100.0
    serving_unit: str = "g"
    
    # Portion size for discrete optimization (grams per purchasable portion)
    portion_size: float = 100.0


class FoodDatabase:
    def __init__(self):
        self.foods: List[FoodItem] = []

    def load_mock_data(self):
        """Loads a small set of mock data for testing purposes."""
        self.foods = [
            FoodItem(
                id="1", 
                name="Chicken Breast", 
                calories=165, 
                nutrients={"protein": 31, "fat": 3.6, "carbohydrate": 0, "Vitamin A (mcg RAE)": 0, "Vitamin C (mg)": 0},
                portion_size=150.0  # 1 chicken breast
            ),
            FoodItem(
                id="2", 
                name="Brown Rice", 
                calories=111, 
                nutrients={"protein": 2.6, "fat": 0.9, "carbohydrate": 23, "Vitamin A (mcg RAE)": 0, "Vitamin C (mg)": 0},
                portion_size=150.0  # 1 cup cooked
            ),
            FoodItem(
                id="3", 
                name="Broccoli", 
                calories=34, 
                nutrients={"protein": 2.8, "fat": 0.4, "carbohydrate": 7, "Vitamin A (mcg RAE)": 623, "Vitamin C (mg)": 89},
                portion_size=100.0  # 1 cup florets
            ),
            FoodItem(
                id="4", 
                name="Almonds", 
                calories=579, 
                nutrients={"protein": 21, "fat": 49, "carbohydrate": 22, "Vitamin A (mcg RAE)": 1, "Vitamin C (mg)": 0},
                portion_size=30.0   # 1 handful
            ),
            FoodItem(
                id="5", 
                name="Olive Oil", 
                calories=884, 
                nutrients={"protein": 0, "fat": 100, "carbohydrate": 0, "Vitamin A (mcg RAE)": 0, "Vitamin C (mg)": 0},
                portion_size=15.0   # 1 tablespoon
            ),
             FoodItem(
                id="6", 
                name="Spinach", 
                calories=23, 
                nutrients={"protein": 2.9, "fat": 0.4, "carbohydrate": 3.6, "Vitamin A (mcg RAE)": 9377, "Vitamin C (mg)": 28},
                portion_size=100.0  # 1 large handful
            ),
        ]

    def load_from_csv(self, filepath: str):
        """Loads food data from a CSV file."""
        import csv
        
        # Mapping from CSV Headers (reference_unified_gold.csv) to our JSON/Internal keys
        header_mapping = {
            # Macros
            "Calories (kcal)": "calories",
            "Protein (g)": "protein",
            "Fat (g)": "fat",
            "Carbohydrate (g)": "carbohydrate",
            
            # Vitamins
            "Vitamin A (ug)": "Vitamin A (mcg RAE)", 
            "Vitamin C (mg)": "Vitamin C (mg)",
            "Vitamin D (ug)": "Vitamin D (mcg)",
            "Vitamin E (mg)": "Vitamin E (mg)",
            "Vitamin K (ug)": "Vitamin K (mcg)",
            "Thiamin (mg)": "Thiamin (B1) (mg)",
            "Riboflavin (mg)": "Riboflavin (B2) (mg)",
            "Niacin (mg)": "Niacin (B3) (mg NE)",
            "Vitamin B6 (mg)": "Vitamin B6 (mg)",
            "Folate (ug)": "Folate (mcg DFE)",
            "Vitamin B12 (ug)": "Vitamin B12 (mcg)",
            "Biotin (ug)": "Biotin (mcg)",
            "Pantothenic Acid (mg)": "Pantothenic Acid (mg)",
            
            # Minerals
            "Calcium (mg)": "Calcium (mg)",
            "Iron (mg)": "Iron (mg)",
            "Magnesium (mg)": "Magnesium (mg)",
            "Phosphorus (mg)": "Phosphorus (mg)",
            "Potassium (mg)": "Potassium (mg)",
            "Sodium (mg)": "Sodium (mg)",
            "Zinc (mg)": "Zinc (mg)",
            "Iodine (ug)": "Iodine (mcg)",
            "Selenium (ug)": "Selenium (mcg)",
            "Copper (mg)": "Copper (mg)", 
            "Manganese (mg)": "Manganese (mg)",
            
            # Other
            "Choline (mg)": "Choline (mg)",
            "Fiber (g)": "Fiber (g)",
            "Omega-3 (g)": "Omega-3 (ALA) (g)",
            "Omega-6 (g)": "Omega-6 (Linoleic) (g)",
            
            # Extended Macros (Sugar, Fats)
            "Sugars (g)": "Sugar (g)",
            "Saturated Fat (g)": "Saturated Fat (g)",
            "Monounsaturated Fat (g)": "Monounsaturated Fat (g)",
            "Polyunsaturated Fat (g)": "Polyunsaturated Fat (g)"
        }
        
        try:
            with open(filepath, mode='r', encoding='utf-8-sig') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Basic validation or skipping logic could go here
                    if not row.get("ref_name"): continue
                    
                    try:
                        # Extract basic info
                        fdc_id = row.get("source_id", row.get("ref_name"))
                        name = row.get("ref_name", "Unknown Food")
                        
                        # Calories (try different headers if needed, using generic priority)
                        calories = float(row.get("Calories (kcal)", 0) or 0)
                        
                        nutrients = {}
                        for csv_header, internal_key in header_mapping.items():
                             val_str = row.get(csv_header, "0")
                             try:
                                 val = float(val_str) if val_str else 0.0
                             except ValueError:
                                 val = 0.0
                             
                             nutrients[internal_key] = val
                        
                        food_item = FoodItem(
                            id=fdc_id,
                            name=name,
                            calories=calories,
                            nutrients=nutrients
                        )
                        self.foods.append(food_item)

                    except ValueError as e:
                        print(f"Skipping row {row.get('ref_name', '?')}: {e}")
                        
        except FileNotFoundError:
            print(f"Error: File not found at {filepath}")
        except Exception as e:
            print(f"Error loading CSV: {e}")

    def load_from_parquet(self, filepath: str):
        """Loads food data from a "data/processed/GOLD_REF_food_db.parquet" file."""
        import pandas as pd
        import numpy as np
        
        try:
            df = pd.read_parquet(filepath)
            
            # Mapping from base_nutrition.parquet columns to internal keys
            mapping = {
                'Energy (KCAL)': 'calories',
                'Protein (G)': 'protein',
                'Total lipid (fat) (G)': 'fat',
                'Carbohydrate, by difference (G)': 'carbohydrate',
                'Fiber, total dietary (G)': 'Fiber (g)',
                
                # Vitamins
                'Vitamin A, RAE (UG)': 'Vitamin A (mcg RAE)',
                'Vitamin C, total ascorbic acid (MG)': 'Vitamin C (mg)',
                'Vitamin D (D2 + D3) (UG)': 'Vitamin D (mcg)',
                'Vitamin E (alpha-tocopherol) (MG)': 'Vitamin E (mg)',
                'Vitamin K (phylloquinone) (UG)': 'Vitamin K (mcg)',
                'Thiamin (MG)': 'Thiamin (B1) (mg)',
                'Riboflavin (MG)': 'Riboflavin (B2) (mg)',
                'Niacin (MG)': 'Niacin (B3) (mg NE)',
                'Vitamin B-6 (MG)': 'Vitamin B6 (mg)',
                'Folate, DFE (UG)': 'Folate (mcg DFE)',
                'Vitamin B-12 (UG)': 'Vitamin B12 (mcg)',
                'Biotin (UG)': 'Biotin (mcg)',
                'Pantothenic acid (MG)': 'Pantothenic Acid (mg)',
                'Choline, total (MG)': 'Choline (mg)',
                
                # Minerals
                'Calcium, Ca (MG)': 'Calcium (mg)',
                'Iron, Fe (MG)': 'Iron (mg)',
                'Magnesium, Mg (MG)': 'Magnesium (mg)',
                'Phosphorus, P (MG)': 'Phosphorus (mg)',
                'Potassium, K (MG)': 'Potassium (mg)',
                'Sodium, Na (MG)': 'Sodium (mg)',
                'Zinc, Zn (MG)': 'Zinc (mg)',
                'Copper, Cu (MG)': 'Copper (mg)',
                'Manganese, Mn (MG)': 'Manganese (mg)',
                'Selenium, Se (UG)': 'Selenium (mcg)',
                'Molybdenum, Mo (UG)': 'Molybdenum (mcg)',
                'Iodine, I (UG)': 'Iodine (mcg)',
                
                # Others
                'PUFA 18:3 n-3 c,c,c (ALA) (G)': 'Omega-3 (ALA) (g)',
                'PUFA 18:2 n-6 c,c (G)': 'Omega-6 (Linoleic) (g)',

                # Finer Grained Macros
                'Sugars, Total (G)': 'Sugar (g)',
                'Fatty acids, total saturated (G)': 'Saturated Fat (g)',
                'Fatty acids, total monounsaturated (G)': 'Monounsaturated Fat (g)',
                'Fatty acids, total polyunsaturated (G)': 'Polyunsaturated Fat (g)',
            }
            
            for index, row in df.iterrows():
                try:
                    fdc_id = str(row.get('fdc_id', ''))
                    name = row.get('description', 'Unknown')
                    
                    # Calories
                    calories = row.get('Energy (KCAL)', 0)
                    if pd.isna(calories): calories = 0.0
                    
                    nutrients = {}
                    for col, key in mapping.items():
                        val = row.get(col, 0.0)
                        if pd.isna(val): val = 0.0
                        nutrients[key] = float(val)

                    self.foods.append(FoodItem(fdc_id, name, float(calories), nutrients))
                    
                except Exception as e:
                    continue
                    
        except Exception as e:
            print(f"Error loading Parquet: {e}")

    def get_all_foods(self) -> List[FoodItem]:
        return self.foods
