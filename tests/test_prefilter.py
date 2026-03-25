"""
Test the pre-filter optimization for faster discrete portions.
"""
import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from src.calculator.db_interface import FoodDatabase
from src.optimizer.optimizer import NutritionOptimizer
from src.utils.food_filter import filter_foods_by_nutrient_density

def test_prefilter_optimization():
    """Test that pre-filtering dramatically speeds up discrete portion optimization."""
    
    print("="*70)
    print("PRE-FILTER OPTIMIZATION TEST")
    print("="*70)
    
    # Load real database
    db = FoodDatabase()
    parquet_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__), 
        '../data/processed/real_food_nutrition.parquet'
    ))
    
    if os.path.exists(parquet_path):
        print(f"Loading database from {parquet_path}...")
        db.load_from_parquet(parquet_path)
    else:
        print("Real database not found, using mock data")
        db.load_mock_data()
    
    all_foods = db.get_all_foods()
    print(f"Total foods loaded: {len(all_foods)}")
    
    # Simple macro-only constraints for speed
    constraints = {
        "protein": {"min": 100, "max": 200},
        "fat": {"min": 50, "max": 100},
        "carbohydrate": {"min": 200, "max": 400}
    }
    target_cals = 2000
    
    print(f"\nTarget: {target_cals} kcal")
    print(f"Constraints: protein (100-200g), fat (50-100g), carbs (200-400g)")
    
    # Test 1: Pre-filter to 100 foods, then discrete MILP
    print("\n" + "="*70)
    print("TEST 1: Pre-filter (100 foods) + Discrete MILP")
    print("="*70)
    
    start = time.time()
    filtered_foods = filter_foods_by_nutrient_density(
        all_foods,
        max_foods=100,
        min_calories=10.0,
        target_nutrients=["protein", "fat", "carbohydrate"]
    )
    filter_time = time.time() - start
    print(f"Filtered to {len(filtered_foods)} foods in {filter_time:.3f}s")
    
    # Show top 5 filtered foods
    print("\nTop 5 filtered foods (by nutrient density):")
    for f in filtered_foods[:5]:
        print(f"  - {f.name}: {f.calories}kcal, P:{f.nutrients.get('protein',0):.1f}g")
    
    optimizer = NutritionOptimizer(filtered_foods)
    
    start = time.time()
    result = optimizer.optimize_diet(
        target_calories=target_cals,
        nutrient_constraints=constraints,
        use_discrete_portions=True,
        time_limit=30
    )
    solve_time = time.time() - start
    
    print(f"\nSolver completed in {solve_time:.2f}s")
    print(f"Status: {result['status']}")
    print(f"Foods selected: {len(result['selected_foods'])}")
    
    if result['selected_foods']:
        print("\nMeal Plan:")
        for name, amount in result['selected_foods'].items():
            portions = result['selected_portions'].get(name, {})
            p_count = portions.get('portions', '?')
            p_size = portions.get('portion_size_g', '?')
            print(f"  - {name}: {p_count} x {p_size}g = {amount:.0f}g")
        
        print(f"\nTotals:")
        print(f"  Calories: {result['totals']['calories']:.0f}")
        for n, v in result['totals']['nutrients'].items():
            print(f"  {n}: {v:.1f}g")
    
    total_time = filter_time + solve_time
    print(f"\n*** TOTAL TIME: {total_time:.2f}s ***")
    
    success = result['status'] in ['Optimal', 'Feasible (Time Limit)'] and solve_time < 15
    print(f"\nTest {'PASSED' if success else 'FAILED'} (target: < 15s solve time)")
    
    return success

if __name__ == "__main__":
    test_prefilter_optimization()
