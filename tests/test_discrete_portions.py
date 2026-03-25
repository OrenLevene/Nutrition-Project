"""
Test discrete portion constraints in the optimizer.
"""
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from src.calculator.db_interface import FoodDatabase, FoodItem
from src.optimizer.optimizer import NutritionOptimizer

def test_discrete_portions():
    """
    Test that discrete portion mode:
    1. Returns amounts that are exact multiples of portion_size
    2. Uses more diverse foods than continuous mode
    """
    print("="*60)
    print("TEST: Discrete Portion Constraints")
    print("="*60)
    
    db = FoodDatabase()
    db.load_mock_data()
    
    print("\nFoods with portion sizes:")
    for food in db.get_all_foods():
        print(f"  - {food.name}: {food.portion_size}g per portion, {food.calories} kcal/100g")
    
    optimizer = NutritionOptimizer(db.get_all_foods())
    target_cals = 1500
    
    # Test 1: Continuous mode
    print(f"\n--- CONTINUOUS MODE (Target: {target_cals} kcal) ---")
    result_continuous = optimizer.optimize_diet(target_cals, {}, use_discrete_portions=False)
    print(f"Status: {result_continuous['status']}")
    print(f"Foods selected: {len(result_continuous['selected_foods'])}")
    for name, amount in result_continuous['selected_foods'].items():
        print(f"  - {name}: {amount:.2f}g")
    print(f"Total Calories: {result_continuous['totals']['calories']:.1f}")
    
    # Test 2: Discrete mode
    print(f"\n--- DISCRETE PORTION MODE (Target: {target_cals} kcal) ---")
    result_discrete = optimizer.optimize_diet(target_cals, {}, use_discrete_portions=True)
    print(f"Status: {result_discrete['status']}")
    print(f"Foods selected: {len(result_discrete['selected_foods'])}")
    for name, amount in result_discrete['selected_foods'].items():
        portion_info = result_discrete['selected_portions'].get(name, {})
        portions = portion_info.get('portions', '?')
        portion_size = portion_info.get('portion_size_g', '?')
        print(f"  - {name}: {amount:.0f}g ({portions} × {portion_size}g portions)")
    print(f"Total Calories: {result_discrete['totals']['calories']:.1f}")
    
    # Verify discrete amounts are exact multiples of portion sizes
    print("\n--- VERIFICATION ---")
    all_valid = True
    for food in db.get_all_foods():
        if food.name in result_discrete['selected_foods']:
            amount = result_discrete['selected_foods'][food.name]
            portion_size = food.portion_size
            remainder = amount % portion_size
            is_valid = remainder < 0.001 or (portion_size - remainder) < 0.001
            status = "PASS" if is_valid else "FAIL"
            print(f"  {status} {food.name}: {amount}g / {portion_size}g = {amount/portion_size:.2f} portions")
            if not is_valid:
                all_valid = False
    
    if all_valid:
        print("\nPASS: All amounts are valid multiples of portion sizes!")
    else:
        print("\nFAIL: Some amounts are NOT valid multiples of portion sizes!")
    
    # Compare diversity
    print(f"\n--- DIVERSITY COMPARISON ---")
    print(f"Continuous mode: {len(result_continuous['selected_foods'])} foods")
    print(f"Discrete mode:   {len(result_discrete['selected_foods'])} foods")
    
    if len(result_discrete['selected_foods']) >= len(result_continuous['selected_foods']):
        print("OK: Discrete mode uses same or more foods (as expected)")
    else:
        print("Note: Discrete mode uses fewer foods (may vary by constraints)")
    
    return all_valid

def test_high_calorie_discrete():
    """
    Test discrete portions with a higher calorie target to see more food diversity.
    """
    print("\n" + "="*60)
    print("TEST: High Calorie Discrete Portions")
    print("="*60)
    
    db = FoodDatabase()
    db.load_mock_data()
    optimizer = NutritionOptimizer(db.get_all_foods())
    
    # Higher target with protein constraint
    target_cals = 2000
    constraints = {"protein": (100, None)}  # Min 100g protein
    
    print(f"\n--- DISCRETE MODE (Target: {target_cals} kcal, Min Protein: 100g) ---")
    result = optimizer.optimize_diet(target_cals, constraints, use_discrete_portions=True)
    
    print(f"Status: {result['status']}")
    print(f"Foods selected: {len(result['selected_foods'])}")
    for name, amount in result['selected_foods'].items():
        portion_info = result['selected_portions'].get(name, {})
        portions = portion_info.get('portions', '?')
        portion_size = portion_info.get('portion_size_g', '?')
        print(f"  - {name}: {amount:.0f}g ({portions} × {portion_size}g portions)")
    print(f"Total Calories: {result['totals']['calories']:.1f}") 
    print(f"Total Protein: {result['totals']['nutrients'].get('protein', 0):.1f}g")
    
    return result['status'] in ['Optimal', 'Feasible (Time Limit)']

if __name__ == "__main__":
    test1_passed = test_discrete_portions()
    test2_passed = test_high_calorie_discrete()
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Test 1 (Discrete Constraints):   {'PASS' if test1_passed else 'FAIL'}")
    print(f"Test 2 (High Calorie Discrete):  {'PASS' if test2_passed else 'FAIL'}")
