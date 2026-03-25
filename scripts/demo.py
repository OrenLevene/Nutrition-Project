import json
import pandas as pd
import os
from src.calculator.engine import load_data, calculate_bmr, calculate_tdee_simple, get_micronutrients
from src.calculator.db_interface import FoodDatabase
from src.optimizer.optimizer import NutritionOptimizer

def print_grid(df):
    if df.empty:
        print("(Empty Table)")
        return
        
    # Convert all to string
    df_str = df.astype(str)
    
    # Calculate column widths
    widths = [max(len(col), df_str[col].str.len().max()) for col in df_str.columns]
    
    # Create separator line
    sep = "+" + "+".join(["-" * (w + 2) for w in widths]) + "+"
    
    print(sep)
    # Header
    header = "|" + "|".join([f" {col.ljust(w)} " for col, w in zip(df_str.columns, widths)]) + "|"
    print(header)
    print(sep)
    
    # Rows
    for _, row in df_str.iterrows():
        line = "|" + "|".join([f" {val.ljust(w)} " for val, w in zip(row, widths)]) + "|"
        print(line)
        
    print(sep)

def run_demo():
    # Configure pandas for wide display
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 2000)
    pd.set_option('display.max_colwidth', None)

    print("--- 1. LOADING DATA ---")
    data = load_data()
    # ... (rest of setup code is fine, skipping lines 9-89 in this huge block is hard with replace, 
    # so I will target the imports first, then the specific print block at the end)

# Actually, I need to do this in chunks or carefully targeted replacements.
# First, imports.

    print("--- 1. LOADING DATA ---")
    data = load_data()
    if not data:
        print("Failed to load nutrition_data.json")
        return

    # Profile: Male, 30y, 80kg, 180cm, Moderately Active
    age = 30
    gender = 'male'
    weight = 80
    height = 180
    activity = 'moderately_active'
    
    print(f"Profile: {gender}, {age}y, {weight}kg, {height}cm, {activity}")

    print("\n--- 2. CALCULATING CONSTRAINTS ---")
    bmr = calculate_bmr(weight, height, age, gender)
    tdee, _, _ = calculate_tdee_simple(bmr, activity, data)
    
    print(f"Target Calories: {tdee:.0f} kcal")
    
    # Get Micros
    micros = get_micronutrients(age, gender, data, goal='maintenance')
    
    # Flatten constraints for optimizer
    constraints = {}
    
    # 1. Macros (using simple standard split for demo)
    # Protein ~20% (1.6g/kg approx for health/active)
    # Carbs ~50%
    # Fat ~30%
    
    # Let's define constraints based on grams
    # 20% of 2800 kcal = 560 kcal = 140g protein
    p_g = (tdee * 0.20) / 4
    c_g = (tdee * 0.50) / 4
    f_g = (tdee * 0.30) / 9
    
    # Add some flexibility (+/- 10%)
    constraints["protein"] = {'min': p_g * 0.9, 'max': p_g * 1.1}
    constraints["carbohydrate"] = {'min': c_g * 0.9, 'max': c_g * 1.1}
    constraints["fat"] = {'min': f_g * 0.9, 'max': f_g * 1.1}
    
    print("\n--- 3. LOAD FOODS ---")
    db = FoodDatabase()
    # Use the clean database
    parquet_path = "c:/Users/Oren Arie Levene/Nutrition Project/database_builder/real_food_nutrition.parquet"
    print(f"Loading data from {parquet_path}...")
    db.load_from_parquet(parquet_path)
    print(f"Loaded {len(db.foods)} REAL FOODS from parquet.")
    
    # 2. Micros - Add first 5 alphabetical
    print("\nAdding first 5 alphabetical micronutrients...")
    # Get all available nutrient keys from the first few foods to ensure we pick existing ones,
    # OR rely on the `micros` keys which come from requirements.
    # Let's rely on `micros` requirements keys, but filter by what exists in food data if needed.
    # Actually, `constraints` dict needs keys that match FoodItem.nutrients keys.
    # In `usda_ingestion` / `download_process`, we named cols as "Nutrient Name (Unit)".
    # `nutrition_calculator.py` `get_micronutrients` returns standardized keys.
    # We need to map Requirement Keys -> Parquet Column Keys.
    # For this demo, let's look at the implementation of `load_from_parquet`.
    # It likely standardizes names or keeps them as is. 
    # Let's check `db.foods[0].nutrients` keys if possible. 
    # BUT, since we can't inspect runtime objects easily, we'll try to match the keys from `get_micronutrients`.
    
    # Sort all micro keys
    all_micro_keys = []
    for cat in micros:
        for nutrient in micros[cat]:
             all_micro_keys.append((nutrient, micros[cat][nutrient]))
    
    all_micro_keys.sort(key=lambda x: x[0])
    selected_micros = all_micro_keys[:5] # Revert to 5 for speed/stability

    
    for nutrient, constraint in selected_micros:
        print(f"  Adding constraint for: {nutrient}")
        constraints[nutrient] = constraint

    # No filtering needed for clean DB
    
    print("\n--- 4. OPTIMIZING ---")
    optimizer = NutritionOptimizer(db.foods)
    result = optimizer.optimize_diet(tdee, constraints)
    
    print(f"\nStatus: {result['status']}")
    
    if result['status'] == 'Optimal':
        # --- 1. PREPARE DATA ---
        selected_items = result['selected_foods'] # name -> grams
        totals = result['totals']['nutrients']
        
        # Identify all constrained nutrients for columns
        # Macros
        constrained_keys = ["calories", "protein", "carbohydrate", "fat"]
        # Micros
        for nutrient, _ in selected_micros:
            constrained_keys.append(nutrient)
            
        # --- 2. SUMMARY TABLE ---
        summary_rows = []
        for key in constrained_keys:
            # Fetch actual value
            if key == "calories":
                actual = result['totals']['calories']
                target = tdee
            else:
                actual = totals.get(key, 0)
                target = constraints.get(key, {})
            
            # Parse target
            min_val = 0
            max_val = float('inf')
            
            if isinstance(target, dict):
                min_val = target.get('min', 0)
                max_val = target.get('max')
                if max_val is None: max_val = float('inf')
            elif isinstance(target, (int, float)): # Simple value (like Calories often handled)
                # Special case for calories target range logic if strictly tdee
                if key == "calories":
                     min_val = tdee * 0.95
                     max_val = tdee * 1.05
                else: 
                     min_val = target
            elif isinstance(target, (tuple, list)):
                min_val = target[0] if len(target)>0 else 0
                max_val = target[1] if len(target)>1 else float('inf')
                if max_val is None: max_val = float('inf')
            
            status = "OK"
            if actual < min_val - 0.1: status = "LOW"
            if actual > max_val + 0.1: status = "HIGH"
            
            max_str = f"{max_val:.1f}" if max_val != float('inf') else "Inf"
            
            summary_rows.append({
                "Nutrient": key,
                "Actual": f"{actual:.1f}",
                "Min": f"{min_val:.1f}",
                "Max": max_str,
                "Status": status
            })
            
        print("\n=== OPTIMIZATION SUMMARY ===")
        df_summary = pd.DataFrame(summary_rows)
        print_grid(df_summary)

        # --- 3. FOOD DETAILS TABLE ---
        # Need lookup for per-100g values. 
        # Create map name -> FoodItem
        food_map = {f.name: f for f in db.foods}
        
        food_rows = []
        
        for name, amount in selected_items.items():
            food_item = food_map.get(name)
            if not food_item: continue
            
            row = {"Food": name, "Weight (g)": f"{amount:.1f}"}
            
            for key in constrained_keys:
                # Value per 100g
                per_100 = 0
                if key == "calories":
                    per_100 = food_item.calories
                else:
                    per_100 = food_item.nutrients.get(key, 0)
                    
                # Value in diet
                in_diet = (per_100 / 100.0) * amount
                
                # Add columns
                row[f"{key} (100g)"] = f"{per_100:.1f}"
                row[f"{key} (Total)"] = f"{in_diet:.1f}"
            
            food_rows.append(row)
            
        print("\n=== SELECTED FOOD DETAILS ===")
        if food_rows:
            df_foods = pd.DataFrame(food_rows)
            
            # HTML Report Generation
            html_content = """
            <html>
            <head>
                <style>
                    body { font-family: Arial, sans-serif; margin: 20px; }
                    table { border-collapse: collapse; width: 100%; margin-bottom: 20px; display: block; overflow-x: auto; }
                    th, td { border: 1px solid #ddd; padding: 8px; text-align: left; white-space: nowrap; }
                    th { background-color: #f2f2f2; }
                    tr:nth-child(even) { background-color: #f9f9f9; }
                    h2 { color: #333; }
                </style>
            </head>
            <body>
            <h1>Optimization Report</h1>
            <h2>Summary</h2>
            """
            html_content += df_summary.to_html(index=False)
            
            html_content += "<h2>Food Details</h2>"
            html_content += df_foods.to_html(index=False)
            
            html_content += "</body></html>"
            
            report_path = "optimization_report.html"
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(html_content)
                
            print(f"\n[SUCCESS] Report saved to: {os.path.abspath(report_path)}")
            print("Open this file in your browser to view the scrollable tables.")
        
        if 'warnings' in result and result['warnings']:
             print("\nWARNINGS:")
             for w in result['warnings']:
                 print(f"  ! {w}")
    else:
        print("Could not find a solution. Constraints might be too strict.")

if __name__ == "__main__":
    run_demo()
