from calculator.nutrition_calculator import calculate_bmr, calculate_tdee_simple, get_micronutrients, load_data
from calculator.food_database import FoodDatabase
from calculator.optimizer import NutritionOptimizer
import json

def test_integration():
    print("--- 1. Loading Data & Calculating Requirements ---")
    data = load_data("calculator/nutrition_data.json") # Loads nutrition_data.json from subdir
    
    # User Profile: Male, 23, Active (Moderately Active)
    # Assumptions: 75kg, 180cm
    weight = 75
    height = 180
    age = 23
    gender = 'male'
    activity = 'moderately_active'
    
    bmr = calculate_bmr(weight, height, age, gender)
    tdee_result = calculate_tdee_simple(bmr, activity, data)
    tdee = tdee_result[0] # Extract calories from (calories, multiplier, activity_string)
    
    print(f"BMR: {bmr}")
    print(f"TDEE (Calories): {tdee}")
    
    # Get Micronutrients (and Macros from TDEE/Data)
    # The calculator's get_micronutrients likely returns just parsing the JSON for that age/gender
    micros = get_micronutrients(age, gender, data)
    
    # We need to construct the constraints dict: {Nutrient: (min, max)}
    constraints = {}
    
    # Add Macros (AMDR) - calculating grams based on TDEE
    # ranges: Protein 10-35%, Fat 20-35%, Carbs 45-65% (typical)
    # The calculator might have a function for this, or we do it manually based on JSON data
    # Let's check what 'micros' contains:
    # It probably contains 'vitamins', 'minerals' from JSON.
    
    # Let's inspect keys
    # print("Micros keys:", micros.keys()) 
    # Example: 'Vitamin A (mcg RAE)': 900
    
    # Flatten micros dictionary
    for category in ['vitamins', 'minerals', 'other']:
        if category in micros:
            for nutrient, amount in micros[category].items():
                if isinstance(amount, (int, float)):
                    constraints[nutrient] = (amount, None)
    
    # Add Macros manually
    # TDEE ~2735. Min Protein 10% or 0.8g/kg.
    # User is 'moderately_active'.
    # Let's set some reasonable ranges based on standard guidelines:
    # Protein: 1.2g/kg to 2.2g/kg (active)
    # Fat: 20-35% TDEE
    # Carbs: Remainder (45-65% TDEE)
    
    # 1. Protein
    p_min_g = weight * 1.2
    p_max_g = weight * 2.2
    constraints["protein"] = (p_min_g, p_max_g)
    
    # 2. Fat (9 cal/g)
    f_min_cal = tdee * 0.20
    f_max_cal = tdee * 0.35
    constraints["fat"] = (f_min_cal / 9.0, f_max_cal / 9.0)
    
    # 3. Carbs (4 cal/g)
    c_min_cal = tdee * 0.45
    c_max_cal = tdee * 0.65
    constraints["carbohydrate"] = (c_min_cal / 4.0, c_max_cal / 4.0)
    
    print("\n--- Constraints (Sample) ---")
    for k, v in list(constraints.items())[:5]:
        print(f"{k}: {v}")
    
    print("\n--- 2. Loading Food Database (Full) ---")
    db = FoodDatabase()
    # db.load_mock_data()
    db.load_from_parquet("data/processed/base_nutrition.parquet")
    print(f"Loaded {len(db.get_all_foods())} foods.")
    
    # Collect all unique nutrient keys available across all foods
    available_nutrients = set()
    for food in db.get_all_foods():
        available_nutrients.update(food.nutrients.keys())
    
    # print(f"Available Nutrients in DB: {available_nutrients}")
    
    # Use ALL constraints that are available in DB
    final_constraints = {k: v for k, v in constraints.items() if k in available_nutrients}
    
    # print(f"Filtered Constraints keys: {list(final_constraints.keys())}")
    
    print("\n--- 3. Running Optimizer ---")
    
    optimizer = NutritionOptimizer(db.get_all_foods())
    result = optimizer.optimize_diet(tdee, final_constraints)
    
    print(f"Status: {result['status']}")
    if result["status"] == "Optimal":
        print("  - " + "\n  - ".join([f"{k}: {v:.2f}g" for k, v in result['selected_foods'].items()]))
        
        # Debug: Print stats of selected foods
        print("\n--- Selected Food Details ---")
        for FoodName, grams in result['selected_foods'].items():
            # Find food object
            f = next((x for x in db.get_all_foods() if x.name == FoodName), None)
            if f:
                print(f"{f.name} (ID: {f.id}):")
                print(f"  Calories/100g: {f.calories}")
                print(f"  Protein/100g: {f.nutrients.get('protein', 0)}")
                print(f"  Fat/100g: {f.nutrients.get('fat', 0)}")
                print(f"  Carbs/100g: {f.nutrients.get('carbohydrate', 0)}")
    
        if result.get("warnings"):
            print("Warnings:", result["warnings"])
    else:
        print("Infeasible. Likely due to missing nutrient data in mock foods vs constraints.")
        print("Constraint Keys (Sample):", list(constraints.keys())[:5])

if __name__ == "__main__":
    test_integration()
