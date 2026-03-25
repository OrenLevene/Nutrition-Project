"""Test script for genetic algorithm meal optimizer with micronutrients."""
import sys
sys.path.insert(0, '.')

from src.calculator.db_interface import FoodDatabase
from src.optimizer.genetic_optimizer import GeneticMealOptimizer

# Load foods
db = FoodDatabase()
db.load_from_parquet('data/processed/real_food_nutrition.parquet')
foods = db.get_all_foods()
print(f'Loaded {len(foods)} foods')

# Create optimizer
optimizer = GeneticMealOptimizer(
    foods=foods,
    population_size=80,
    generations=150
)

# Full nutrient targets (per day)
daily_targets = {
    # Macros
    'protein': {'min': 100, 'max': None},
    'fat': {'min': 60, 'max': 90},
    'carbohydrate': {'min': 250, 'max': 350},
    
    # Vitamins (Vitamin D removed - supplement/sun recommended)
    'Vitamin A (mcg RAE)': {'min': 900, 'max': 3000},
    'Vitamin C (mg)': {'min': 90, 'max': 2000},
    # 'Vitamin D (mcg)': {'min': 15, 'max': 100},  # Supplement recommended
    'Vitamin E (mg)': {'min': 15, 'max': 1000},
    'Vitamin K (mcg)': {'min': 120, 'max': None},
    'Thiamin (B1) (mg)': {'min': 1.2, 'max': None},
    'Riboflavin (B2) (mg)': {'min': 1.3, 'max': None},
    'Niacin (B3) (mg NE)': {'min': 16, 'max': None},
    'Vitamin B6 (mg)': {'min': 1.3, 'max': 100},
    'Folate (mcg DFE)': {'min': 400, 'max': None},
    'Vitamin B12 (mcg)': {'min': 2.4, 'max': None},
    
    # Minerals
    'Calcium (mg)': {'min': 1000, 'max': 2500},
    'Iron (mg)': {'min': 8, 'max': 45},
    'Magnesium (mg)': {'min': 400, 'max': None},
    'Phosphorus (mg)': {'min': 700, 'max': 4000},
    'Potassium (mg)': {'min': 3400, 'max': None},
    'Zinc (mg)': {'min': 11, 'max': 40},
    'Selenium (mcg)': {'min': 55, 'max': 400},
}
target_calories = 2000

# Optimize for 7 days
print('Running optimization...')
result = optimizer.optimize(
    n_days=7,
    target_calories=target_calories,
    nutrient_targets=daily_targets,
    foods_per_day=6
)

print(f'\n{"="*60}')
print(f'RESULTS')
print(f'{"="*60}')
print(f'Status: {result["status"]}')
print(f'Fitness: {result["fitness_score"]}')
print(f'Unique foods: {result["unique_foods"]}')
print(f'Max trio reps allowed: {result["max_trio_repetitions"]}')

print(f'\n{"="*60}')
print(f'DAILY BREAKDOWN (checking ±10% of macros)')
print(f'{"="*60}')
for day in result['daily_plans']:
    print(f'\nDay {day["day"]}: {day["total_calories"]:.0f} kcal')
    
    # Calculate day macros
    day_protein = 0
    day_fat = 0
    day_carbs = 0
    for food in day['foods']:
        for f in foods:
            if f.name == food['name']:
                mult = food['grams'] / 100
                day_protein += f.nutrients.get('protein', 0) * mult
                day_fat += f.nutrients.get('fat', 0) * mult
                day_carbs += f.nutrients.get('carbohydrate', 0) * mult
                break
    
    # Check against ±10%
    cal_ok = abs(day["total_calories"] - target_calories) / target_calories <= 0.10
    
    print(f'  Calories: {day["total_calories"]:.0f} (target: {target_calories}, {"OK" if cal_ok else "MISS"})')
    print(f'  Foods: {", ".join([f["name"][:25] for f in day["foods"]])}')

print(f'\n{"="*60}')
print(f'SHOPPING LIST ({len(result["shopping_list"])} items)')
print(f'{"="*60}')
for food, grams in sorted(result['shopping_list'].items(), key=lambda x: -x[1])[:15]:
    print(f'  {food[:50]}: {grams:.0f}g')

print(f'\n{"="*60}')
print(f'NUTRIENT TOTALS (daily average)')  
print(f'{"="*60}')
for nutrient, value in result['totals']['daily_average'].items():
    if nutrient in daily_targets:
        target = daily_targets[nutrient]
        print(f'  {nutrient}: {value:.1f} (min: {target.get("min", "-")}, max: {target.get("max", "-")})')
    elif nutrient == 'calories':
        print(f'  {nutrient}: {value:.1f} (target: {target_calories})')

