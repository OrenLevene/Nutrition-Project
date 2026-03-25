"""
Nutrition Optimizer Web API.

FastAPI application providing endpoints for nutrient calculation and diet optimization.
"""
import sys
import os
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from typing import Dict, Any

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.calculator.engine import (
    load_data, calculate_bmr, calculate_tdee_simple, get_micronutrients
)
from src.calculator.db_interface import FoodDatabase
from src.optimizer.optimizer import NutritionOptimizer
from src.optimizer.genetic_optimizer import GeneticMealOptimizer

from src.web_app.models import UserProfile, CalculationResult, NutrientRange, OptimizationResult

app = FastAPI(title="Nutrition Optimizer API")

app.mount("/static", StaticFiles(directory="src/web_app/static"), name="static")
templates = Jinja2Templates(directory="src/web_app/templates")

# Global data loaded at startup
try:
    NUTRITION_DATA = load_data(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/config/nutrition_data.json')))
    FOOD_DB = FoodDatabase()
    parquet_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/real_food_nutrition.parquet'))
    if os.path.exists(parquet_path):
        print(f"Loading database from {parquet_path}...")
        FOOD_DB.load_from_parquet(parquet_path)
    else:
        print("Warning: Real database not found, falling back to mock.")
        FOOD_DB.load_mock_data()
        
    OPTIMIZER = NutritionOptimizer(FOOD_DB.get_all_foods())
    
    from src.utils.store_lookup import StoreProductLookup
    STORE_LOOKUP = StoreProductLookup()
    print(f"Store lookup initialized with {len(STORE_LOOKUP.get_covered_canonical_ids())} canonical items coverage")
except Exception as e:
    print(f"Startup Error: {e}")
    FOOD_DB = FoodDatabase()
    FOOD_DB.load_mock_data()
    OPTIMIZER = NutritionOptimizer(FOOD_DB.get_all_foods())
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Serve the landing page."""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/calculator", response_class=HTMLResponse)
async def read_calculator(request: Request):
    """Serve the calculator page."""
    return templates.TemplateResponse("calculator.html", {"request": request})

@app.get("/science", response_class=HTMLResponse)
async def read_science(request: Request):
    """Serve the science/methodology page."""
    return templates.TemplateResponse("science.html", {"request": request})

def _calculate_needs_internal(profile: UserProfile):
    """Calculate nutrition needs from user profile."""
    bmr = calculate_bmr(profile.weight_kg, profile.height_cm, profile.age, profile.gender)
    tdee, multiplier, _ = calculate_tdee_simple(bmr, profile.activity_level, NUTRITION_DATA)
    
    if profile.goal == 'muscle_gain':
        tdee += 250
    elif profile.goal == 'loss':
        tdee -= 500
    
    protein_g = profile.weight_kg * 2.0
    fats_g = (tdee * 0.25) / 9
    carbs_g = (tdee - (protein_g * 4) - (fats_g * 9)) / 4
    
    # Map frontend goal values to backend expectations
    backend_goal = 'maintenance'
    if profile.goal == 'gain': backend_goal = 'muscle_gain'
    elif profile.goal == 'loss': backend_goal = 'fat_loss'
    
    micros = get_micronutrients(profile.age, profile.gender, NUTRITION_DATA, backend_goal)
    
    return tdee, protein_g, fats_g, carbs_g, micros

@app.post("/api/calculate-ranges", response_model=CalculationResult)
async def calculate_nutrient_ranges(profile: UserProfile):
    """
    Calculate nutrient ranges using REAL logic from src.calculator.engine.py
    """
    tdee, protein, fats, carbs, micros = _calculate_needs_internal(profile)
    
    # Flatten micros for response
    nutrient_ranges = []
    
    # Flatten Vitamins and Minerals
    for category in ['vitamins', 'minerals']:
        if category in micros:
            for name, value in micros[category].items():
                min_v = 0
                max_v = None
                
                if isinstance(value, dict):
                    min_v = value.get('min', 0)
                    max_v = value.get('max')
                elif isinstance(value, (int, float)):
                    min_v = value
                
                nutrient_ranges.append(NutrientRange(
                    nutrient_name=name,
                    min_value=round(float(min_v), 1),
                    max_value=round(float(max_v), 1) if max_v else None,
                    unit="mg/mcg" # Simplification: unit is not in the flattened map key in python code
                ))
    
    extended_macros = ["Sugar (g)", "Saturated Fat (g)", "Monounsaturated Fat (g)", "Polyunsaturated Fat (g)"]
    macro_data = NUTRITION_DATA.get('rda', {}).get('macronutrients', {})
    
    for name in extended_macros:
        if name in macro_data:
            info = macro_data[name]['values']['all_ages']
            div = 4.0 if "Sugar" in name else 9.0
            
            min_g = info.get('min', 0)
            min_pct = info.get('min_percent_calories')
            if min_pct is not None:
                min_g = (tdee * (min_pct / 100.0)) / div
            
            max_g = info.get('max')
            max_pct = info.get('max_percent_calories')
            if max_pct is not None:
                max_g = (tdee * (max_pct / 100.0)) / div
            
            nutrient_ranges.append(NutrientRange(
                nutrient_name=name,
                min_value=round(min_g) if min_g else 0,
                max_value=round(max_g) if max_g else None,
                unit="g"
            ))

    cal_tol = 0.05
    macro_tol = 0.10
    
    return CalculationResult(
        calories=tdee,
        calories_min=round(tdee * (1 - cal_tol), 0),
        calories_max=round(tdee * (1 + cal_tol), 0),
        protein_g=protein,
        protein_min=round(protein * (1 - macro_tol), 1),
        protein_max=round(protein * (1 + macro_tol), 1),
        carbs_g=carbs,
        carbs_min=round(carbs * (1 - macro_tol), 1),
        carbs_max=round(carbs * (1 + macro_tol), 1),
        fats_g=fats,
        fats_min=round(fats * (1 - macro_tol), 1),
        fats_max=round(fats * (1 + macro_tol), 1),
        micronutrients=nutrient_ranges
    )

@app.post("/api/optimize", response_model=OptimizationResult)
async def optimize_diet(profile: UserProfile):
    """
    Optimize weekly diet based on calculated needs.
    Returns weekly shopping list with daily averages for nutrient compliance.
    """
    tdee, protein, fat, carbs, micros = _calculate_needs_internal(profile)
    days = 7
    
    constraints = {}
    tolerance = 0.10
    constraints["protein"] = {'min': protein * (1 - tolerance), 'max': protein * (1 + tolerance)}
    constraints["fat"] = {'min': fat * (1 - tolerance), 'max': fat * (1 + tolerance)}
    constraints["carbohydrate"] = {'min': carbs * (1 - tolerance), 'max': carbs * (1 + tolerance)}
    
    # Nutrients with <5% data coverage - recommend supplements instead
    LOW_COVERAGE_NUTRIENTS = {
        'Biotin (mcg)': 'Biotin - only 0.1% of foods have data',
        'Iodine (mcg)': 'Iodine - only 0.2% of foods have data',
        'Molybdenum (mcg)': 'Molybdenum - only 0.2% of foods have data',
        'Creatine (g)': 'Creatine - not typically in food databases',
    }
    
    supplement_recommended = []
    
    # 2. Micronutrients: Dynamic Loading (skip low-coverage nutrients)
    for category in ['vitamins', 'minerals']:
        if category in micros:
            for name, val in micros[category].items():
                # Skip nutrients with poor data coverage
                if name in LOW_COVERAGE_NUTRIENTS:
                    supplement_recommended.append({
                        'nutrient': name,
                        'reason': LOW_COVERAGE_NUTRIENTS[name],
                        'daily_target': val.get('min') if isinstance(val, dict) else val
                    })
                    continue
                    
                min_v = val.get('min') if isinstance(val, dict) else val
                max_v = val.get('max') if isinstance(val, dict) else None
                
                # Filter out "Not specified" or invalid
                if isinstance(min_v, (int, float)):
                    constraints[name] = {'min': min_v, 'max': max_v}

    # 3. Extended Macronutrients (Sugar, Sat Fat, Unsat Fat)
    extended_macros = ["Sugar (g)", "Saturated Fat (g)", "Monounsaturated Fat (g)", "Polyunsaturated Fat (g)"]
    macro_data = NUTRITION_DATA.get('rda', {}).get('macronutrients', {})
    
    for name in extended_macros:
        if name in macro_data:
            info = macro_data[name]['values']['all_ages']
            
            # Divisor: 4 kcal/g for sugars, 9 kcal/g for fats
            div = 4.0 if "Sugar" in name else 9.0
            
            # Calculate min (from min or min_percent_calories)
            min_g = info.get('min', 0)
            min_pct = info.get('min_percent_calories')
            if min_pct is not None:
                min_g = (tdee * (min_pct / 100.0)) / div
            
            # Calculate max (from max or max_percent_calories)
            max_g = info.get('max')
            max_pct = info.get('max_percent_calories')
            if max_pct is not None:
                max_g = (tdee * (max_pct / 100.0)) / div
                 
            constraints[name] = {'min': min_g or 0, 'max': max_g}

    # Use Genetic Algorithm Optimizer for N-day meal planning
    all_foods = FOOD_DB.get_all_foods()
    print(f"Total foods in database: {len(all_foods)}")
    
    # Create GA optimizer (handles food filtering and weighting internally)
    ga_optimizer = GeneticMealOptimizer(
        foods=all_foods,
        population_size=60,   # Reduced for speed
        generations=100,      # Reduced (early exit at fitness ≥95)
        mutation_rate=0.15,
        crossover_rate=0.7
    )
    
    # DEBUG: Check if filtering worked
    print(f"DEBUG: GA initialized with {len(ga_optimizer.foods)} foods (from {len(all_foods)} total)")
    guar_check = [f.name for f in ga_optimizer.foods if "guar" in f.name.lower()]
    if guar_check:
        print(f"CRITICAL WARNING: 'Guar gum' still in food list! Found: {guar_check[:3]}")
    else:
        print("DEBUG: No 'guar' foods found in optimizer (Filtering correct)")
    
    # Run GA Optimizer for N-day meal planning
    result = ga_optimizer.optimize(
        n_days=days,
        target_calories=tdee,            # Daily target
        nutrient_targets=constraints,    # Daily constraints (min/max)
        daily_tolerance=0.10,            # ±10% daily balance
        foods_per_day=10,                # 10 ingredients per day
        max_grams_per_food=300.0         # 300g max per food item
    )
    
    # Build nutrient analysis with range compliance (using DAILY averages)
    from src.web_app.models import NutrientAnalysis
    
    nutrient_analysis = []
    # GA optimizer returns flat dict in totals.daily_average
    daily_avg = result.get("totals", {}).get("daily_average", {})
    # Handle both formats: GA returns flat dict, old optimizer returns nested
    daily_avg_nutrients = daily_avg if isinstance(daily_avg, dict) else {}
    daily_avg_calories = daily_avg.get("calories", daily_avg_nutrients.get("calories", 0))
    
    # Helper function to determine status and error
    def analyze_nutrient(name: str, actual_daily: float, daily_min, daily_max, unit: str = "") -> NutrientAnalysis:
        status = "in_range"
        error_percent = None
        nutrient_tolerance = 0.20
        
        # Apply tolerance for comparison (internal only, not displayed)
        adjusted_min = daily_min * (1 - nutrient_tolerance) if daily_min else None
        adjusted_max = daily_max * (1 + nutrient_tolerance) if daily_max else None
        
        if adjusted_min is not None and actual_daily < adjusted_min:
            status = "below_min"
            if daily_min > 0:  # Use original value for error calculation
                error_percent = round(((daily_min - actual_daily) / daily_min) * 100, 1)
        elif adjusted_max is not None and actual_daily > adjusted_max:
            status = "above_max"
            if daily_max > 0:  # Use original value for error calculation
                error_percent = round(((actual_daily - daily_max) / daily_max) * 100, 1)
        
        # For nutrients with only a max limit (like sugar), display the max as target
        # For nutrients with a min target, display the min
        display_target = daily_min if daily_min else daily_max
        
        return NutrientAnalysis(
            nutrient_name=name,
            actual=round(actual_daily, 2),
            daily_target=round(display_target, 2) if display_target is not None else None,
            weekly_target=round(display_target * days, 2) if display_target is not None else None,
            min_target=round(daily_min, 2) if daily_min is not None else None,  # True optimal, no tolerance
            max_target=round(daily_max, 2) if daily_max is not None else None,  # True optimal, no tolerance
            unit=unit,
            status=status,
            error_percent=error_percent
        )
    
    # Add calories analysis (±5% tolerance)
    cal_tolerance = 0.05
    cal_min = tdee * (1 - cal_tolerance)
    cal_max = tdee * (1 + cal_tolerance)
    
    if daily_avg_calories < cal_min:
        cal_status = "below_min"
        cal_error = round(((cal_min - daily_avg_calories) / cal_min) * 100, 1)
    elif daily_avg_calories > cal_max:
        cal_status = "above_max"
        cal_error = round(((daily_avg_calories - cal_max) / cal_max) * 100, 1)
    else:
        cal_status = "in_range"
        cal_error = None
    
    nutrient_analysis.append(NutrientAnalysis(
        nutrient_name="Calories",
        actual=round(daily_avg_calories, 2),
        daily_target=round(tdee, 2),
        weekly_target=round(tdee * days, 2),
        min_target=round(cal_min, 2),
        max_target=round(cal_max, 2),
        unit="kcal",
        status=cal_status,
        error_percent=cal_error
    ))
    
    # Add all constrained nutrients
    unit_map = {
        "protein": "g", "fat": "g", "carbohydrate": "g",
        "Sugar (g)": "g", "Saturated Fat (g)": "g",
        "Monounsaturated Fat (g)": "g", "Polyunsaturated Fat (g)": "g"
    }
    
    for nutrient_name, constraint in constraints.items():
        actual = daily_avg_nutrients.get(nutrient_name, 0)
        min_val = constraint.get('min') if isinstance(constraint, dict) else constraint
        max_val = constraint.get('max') if isinstance(constraint, dict) else None
        
        # Infer unit from nutrient name
        if nutrient_name in unit_map:
            unit = unit_map[nutrient_name]
        elif "(mg)" in nutrient_name or nutrient_name.endswith("(mg)"):
            unit = "mg"
        elif "(mcg)" in nutrient_name or nutrient_name.endswith("(mcg)"):
            unit = "mcg"
        elif "(g)" in nutrient_name:
            unit = "g"
        else:
            unit = ""
        
        nutrient_analysis.append(analyze_nutrient(
            nutrient_name,
            actual,
            min_val,
            max_val,
            unit
        ))
    
    # Log skipped nutrients
    if supplement_recommended:
        print(f"Skipped {len(supplement_recommended)} nutrients (insufficient data, supplement recommended):")
        for s in supplement_recommended:
            print(f"  - {s['nutrient']}: {s['reason']}")
    
    # Handle GA optimizer format (shopping_list dict) vs old format (selected_foods list)
    if "shopping_list" in result:
        # Convert GA shopping_list to selected_foods format and enrich with store options
        selected_foods = []
        for name, grams in result["shopping_list"].items():
            food_item = {
                "name": name, 
                "weekly_grams": grams,
                "store_options": []
            }
            
            # Lookup store options
            matches = STORE_LOOKUP.get_store_products_by_name(name)
            if matches:
                # Add top matches (limited to 5)
                food_item["store_options"] = [
                    {
                        "brand": m.get("brand", ""),
                        "name": m.get("off_name", ""),
                        "size": m.get("size", ""),
                        "match_quality": m.get("keywords_matched", 0)
                    }
                    for m in matches[:5]
                ]
            
            selected_foods.append(food_item)
            
        selected_portions = {}  # GA doesn't use portions
    else:
        selected_foods = result.get("selected_foods", [])
        selected_portions = result.get("selected_portions", {})
    
    return OptimizationResult(
        status=result.get("status", "Optimal"),
        days=days,
        selected_foods=selected_foods,
        selected_portions=selected_portions,
        totals=result.get("totals", {}),
        nutrient_analysis=nutrient_analysis,
        warnings=result.get("warnings", []),
        mode=result.get("mode", "genetic_algorithm"),
        infeasibility_analysis=result.get("infeasibility_analysis"),
        supplement_recommended=supplement_recommended,
        # Pass GA specific fields
        daily_plans=result.get("daily_plans", []),
        max_trio_repetitions=result.get("max_trio_repetitions"),
        unique_foods=result.get("unique_foods")
    )

if __name__ == "__main__":
    import uvicorn
    # Important: reload works best when running from the directory or module correctly
    uvicorn.run("src.web_app.main:app", host="0.0.0.0", port=8000, reload=True)
