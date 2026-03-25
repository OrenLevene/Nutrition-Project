"""
Simple test: Discrete portions with MACROS ONLY
Verifies the integer portion math is correct.
"""
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from src.calculator.db_interface import FoodDatabase, FoodItem
from src.optimizer.optimizer import NutritionOptimizer

def test_macros_only():
    """Test discrete portions with only macronutrients - fast and focused."""
    
    print("="*70)
    print("DISCRETE PORTION TEST: MACROS ONLY")
    print("="*70)
    
    # Load mock data (6 foods with known portion sizes)
    db = FoodDatabase()
    db.load_mock_data()
    
    print("\n--- FOOD DATABASE ---")
    for f in db.get_all_foods():
        print(f"  {f.name}: {f.calories}kcal/100g, P:{f.nutrients.get('protein',0)}g, "
              f"F:{f.nutrients.get('fat',0)}g, C:{f.nutrients.get('carbohydrate',0)}g | "
              f"Portion: {f.portion_size}g")
    
    optimizer = NutritionOptimizer(db.get_all_foods())
    
    # Simple macros-only constraints
    target_cals = 2000
    constraints = {
        "protein": {"min": 100, "max": 200},  # 100-200g protein
        "fat": {"min": 50, "max": 100},       # 50-100g fat
        "carbohydrate": {"min": 150, "max": 300}  # 150-300g carbs
    }
    
    print(f"\n--- TARGET: {target_cals} kcal ---")
    print("Constraints:")
    for name, c in constraints.items():
        print(f"  {name}: {c['min']} - {c['max']}g")
    
    # Test CONTINUOUS mode
    print("\n" + "="*70)
    print("MODE: CONTINUOUS")
    print("="*70)
    result_cont = optimizer.optimize_diet(
        target_calories=target_cals,
        nutrient_constraints=constraints,
        use_discrete_portions=False
    )
    print(f"Status: {result_cont['status']}")
    print("\nSelected Foods:")
    for name, amount in result_cont['selected_foods'].items():
        print(f"  {name}: {amount:.2f}g")
    print(f"\nTotals:")
    print(f"  Calories: {result_cont['totals']['calories']:.1f}")
    for n, v in result_cont['totals']['nutrients'].items():
        print(f"  {n}: {v:.1f}g")
    
    # Test DISCRETE mode
    print("\n" + "="*70)
    print("MODE: DISCRETE PORTIONS")
    print("="*70)
    result_disc = optimizer.optimize_diet(
        target_calories=target_cals,
        nutrient_constraints=constraints,
        use_discrete_portions=True
    )
    print(f"Status: {result_disc['status']}")
    print("\nSelected Foods (with portion counts):")
    for name, amount in result_disc['selected_foods'].items():
        portion_info = result_disc['selected_portions'].get(name, {})
        portions = portion_info.get('portions', '?')
        portion_size = portion_info.get('portion_size_g', '?')
        print(f"  {name}: {portions} x {portion_size}g = {amount:.0f}g")
    
    print(f"\nTotals:")
    print(f"  Calories: {result_disc['totals']['calories']:.1f}")
    for n, v in result_disc['totals']['nutrients'].items():
        print(f"  {n}: {v:.1f}g")
    
    # Verify integer portions
    print("\n" + "="*70)
    print("VERIFICATION: Are portions integers?")
    print("="*70)
    all_integer = True
    for name, info in result_disc['selected_portions'].items():
        portions = info['portions']
        portion_size = info['portion_size_g']
        total = info['total_g']
        is_int = portions == int(portions)
        matches = abs(total - (portions * portion_size)) < 0.01
        status = "OK" if (is_int and matches) else "FAIL"
        print(f"  {status}: {name} = {portions} portions x {portion_size}g = {total}g")
        if not (is_int and matches):
            all_integer = False
    
    if all_integer:
        print("\n*** SUCCESS: All portions are integers! ***")
    else:
        print("\n*** FAILURE: Some portions are not integers! ***")
    
    return all_integer

if __name__ == "__main__":
    test_macros_only()
