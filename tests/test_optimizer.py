import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from src.calculator.db_interface import FoodDatabase
from src.optimizer.optimizer import NutritionOptimizer

def test_optimization():
    print("Initializing Database...")
    db = FoodDatabase()
    db.load_mock_data()
    print(f"Loaded {len(db.get_all_foods())} foods.")

    optimizer = NutritionOptimizer(db.get_all_foods())
    
    # Test Case 1: Simple Calorie Goal
    print("\n--- Test Case 1: 2000 Calories, Min 100g Protein ---")
    target_cals = 2000
    constraints = {
        "protein": (100, None), # Min 100g, No max
        "fat": (0, None),
        "carbs": (0, None)
    }
    
    result = optimizer.optimize_diet(target_cals, constraints)
    
    print(f"Status: {result['status']}")
    if result["status"] == "Optimal":
        print("Selected Foods:")
        for name, amount in result["selected_foods"].items():
            print(f"  - {name}: {amount:.2f}g")
        
        print("Totals:")
        print(f"  Calories: {result['totals']['calories']:.2f}")
        for n, val in result["totals"]["nutrients"].items():
            print(f"  {n}: {val:.2f}")
            
    # Test Case 2: Constraint that requires mixing (High Vitamin A but low calories?)
    # Spinach has high Vit A (9377) and low cals (23). 
    # Let's ask for 2000 cals but HUGE Vitamin A, forcing Spinach inclusion?
    # Or actually, let's just inspect if it minimizes ingredients.
    # If we just ask for 2000 cals, it should pick the single food that best fits or combination.
    # Almonds (579 cals) -> ~345g of Almonds = 2000 cals. 1 ingredient.
    
    print("\n--- Test Case 2: Minimize Ingredients Check (Just Calories) ---")
    result_simple = optimizer.optimize_diet(2000, {}) # No nutrient constraints
    print(f"Status: {result_simple['status']}")
    print(f"Selected Foods: {list(result_simple['selected_foods'].keys())}")
    
    # Test Case 3: Infeasible
    print("\n--- Test Case 3: Infeasible (5000 cals but max 10g fat) ---")
    result_fail = optimizer.optimize_diet(5000, {"fat": (0, 10)})
    print(f"Status: {result_fail['status']}")

    print("\n--- Test Case 4: High Calorie Warning Check ---")
    # Force high volume by restricting DB to only Spinach (23 cal/100g)
    # Target 500 cals -> 500/0.23 = ~2173g > 2000g
    spinach = [f for f in db.get_all_foods() if f.name == "Spinach"]
    optimizer_spinach = NutritionOptimizer(spinach)
    
    result_high = optimizer_spinach.optimize_diet(600, {}) # 600 cals -> ~2600g
    print(f"Status: {result_high['status']}")
    if result_high.get("warnings"):
        print("Warnings:", result_high["warnings"])
    else:
        print("No warnings triggered.")

if __name__ == "__main__":
    test_optimization()
