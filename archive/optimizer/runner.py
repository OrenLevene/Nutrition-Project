import json
import pandas as pd
import os
import webbrowser
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

def run_task():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 2000)
    
    print("--- 1. CONFIGURATION ---")
    # Fix path: dataset is in same directory as this script
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.abspath(os.path.join(base_dir, "../../data/config/nutrition_data.json"))
    data = load_data(data_path)
    if not data:
        print("Failed to load nutrition_data.json")
        return

    # Profile: Male, 30y, 80kg, 180cm, Moderately Active (Using standards as before)
    age = 30
    gender = 'male'
    weight = 80
    height = 180
    activity = 'moderately_active'
    
    # Calculate Constraints
    bmr = calculate_bmr(weight, height, age, gender)
    tdee, _, _ = calculate_tdee_simple(bmr, activity, data)
    
    print(f"Goal: {tdee:.0f} kcal (Maintenance)")
    
    constraints = {}
    
    # 1. Macronutrients (Protein, Fat, Carbs)
    # Using roughly 2000-2500kcal standard distribution converted to grams
    # Protein: ~25%
    # Fat: ~30%
    # Carbs: ~45%
    
    # Let's use the TDEE to derive grams
    p_g = (tdee * 0.25) / 4
    c_g = (tdee * 0.45) / 4
    f_g = (tdee * 0.30) / 9
    
    constraints["protein"] = {'min': p_g * 0.9, 'max': p_g * 1.1}
    constraints["carbohydrate"] = {'min': c_g * 0.9, 'max': c_g * 1.1}
    constraints["fat"] = {'min': f_g * 0.9, 'max': f_g * 1.1}
    
    # 2. Micronutrients: Dynamic Loading from Science Data
    print("Fetching scientific micronutrient ranges...")
    micros = get_micronutrients(age, gender, data)
    
    # micros structure is {category: {nutrient_name: {min: x, max: y}}}
    # We flatten this into our constraints dict
    
    for category, nutrients in micros.items():
        for nutrient_name, limits in nutrients.items():
            # Limits in JSON are usually {min: x, max: y} or {min: x, max: null}
            # Our optimizer expects {min: x, max: y/None}
            
            # Use all available nutrients or filter if needed. 
            # For this test, we accept all that have a defined min or max.
            
            min_val = limits.get('min')
            max_val = limits.get('max')
            
            if min_val is not None:
                 constraints[nutrient_name] = {'min': min_val, 'max': max_val}

    # 3. Extended Macronutrients (Sugar, Sat Fat, Unsat Fat)
    # These are stored in data['rda']['macronutrients'] but have 'max_percent_calories'
    print("Fetching extended macronutrient limits (Sugar, Fats)...")
    extended_macros = ["Sugar (g)", "Saturated Fat (g)", "Monounsaturated Fat (g)", "Polyunsaturated Fat (g)"]
    macro_data = data.get('rda', {}).get('macronutrients', {})
    
    for name in extended_macros:
        if name in macro_data:
            info = macro_data[name]['values']['all_ages']
            
            min_g = info.get('min', 0)
            max_pct = info.get('max_percent_calories')
            max_g = None
            
            if max_pct is not None:
                # Convert % calories to grams. 
                # Energy density: Fats = 9, Sugar = 4
                divisor = 9.0
                if "Sugar" in name: divisor = 4.0
                
                cals_from_nutrient = tdee * (max_pct / 100.0)
                max_g = cals_from_nutrient / divisor
            
            # Also check for explicit max if any (values.get('max'))
            if max_g is None and info.get('max'):
                 max_g = info.get('max')
                 
            constraints[name] = {'min': min_g or 0, 'max': max_g}
            
    print("\n--- 2. LOADING DATABASE ---")
    db = FoodDatabase()
    parquet_path = os.path.abspath(os.path.join(base_dir, "../../data/processed/real_food_nutrition.parquet"))
    if not os.path.exists(parquet_path):
        print(f"Error: Database not found at {parquet_path}")
        return
        
    print(f"Loading {parquet_path}...")
    db.load_from_parquet(parquet_path)
    print(f"Loaded {len(db.foods)} items.")

    print("\n--- 3. OPTIMIZING ---")
    print("Constraints:")
    for k, v in constraints.items():
        print(f"  {k}: {v}")

    optimizer = NutritionOptimizer(db.foods)
    result = optimizer.optimize_diet(tdee, constraints, max_food_weight=1000.0, time_limit=30, slack_penalty=1000.0)
    
    print(f"\nStatus: {result['status']}")
    
    if result['status'] == 'Optimal':
        selected_items = result['selected_foods']
        totals = result['totals']['nutrients']
        
        # Prepare DataFrames for Report
        
        # 1. Summary
        constrained_keys = ["calories", "protein", "carbohydrate", "fat"]
        micros = [k for k in constraints.keys() if k not in constrained_keys]
        micros.sort()
        constrained_keys.extend(micros)
        
        summary_rows = []
        for key in constrained_keys:
            if key == "calories":
                actual = result['totals']['calories']
                target = tdee
                min_val = tdee * 0.95
                max_val = tdee * 1.05
            else:
                actual = totals.get(key, 0)
                target = constraints.get(key, {})
                if isinstance(target, dict):
                    min_val = target.get('min', 0)
                    max_val = target.get('max', float('inf'))
                else: 
                     min_val = 0
                     max_val = float('inf')
            
            if max_val is None: max_val = float('inf')
            
            status = "OK"
            if actual < min_val - 0.1: status = "LOW"
            if actual > max_val + 0.1 and max_val != float('inf'): status = "HIGH"
            
            max_str = f"{max_val:.1f}" if max_val != float('inf') else "Inf"
            
            summary_rows.append({
                "Nutrient": key,
                "Actual": f"{actual:.1f}",
                "Min": f"{min_val:.1f}",
                "Max": max_str,
                "Status": status
            })

        # Add Combined Unsaturated Fat Row for convenience
        mono = totals.get("Monounsaturated Fat (g)", 0)
        poly = totals.get("Polyunsaturated Fat (g)", 0)
        if mono > 0 or poly > 0:
             summary_rows.append({
                "Nutrient": "Unsaturated Fat (Total)",
                "Actual": f"{(mono + poly):.1f}",
                "Min": "0.0",
                "Max": "Inf",
                "Status": "OK"
             })
            
        df_summary = pd.DataFrame(summary_rows)
        print("\n=== SUMMARY ===")
        print_grid(df_summary)
        
        # 2. Food Details
        food_map = {f.name: f for f in db.foods}
        food_rows = []
        
        for name, amount in selected_items.items():
            food_item = food_map.get(name)
            if not food_item: continue
            
            row = {"Food": name, "Weight (g)": f"{amount:.1f}"}
            for key in constrained_keys:
                if key == "calories":
                    per_100 = food_item.calories
                else:
                    per_100 = food_item.nutrients.get(key, 0)
                
                in_diet = (per_100 / 100.0) * amount
                row[f"{key} (100g)"] = f"{per_100:.1f}"
                # row[f"{key} (Total)"] = f"{in_diet:.1f}"  <-- User requested removal
                
            food_rows.append(row)
            
        df_foods = pd.DataFrame(food_rows)
        
        # HTML Generation
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 40px; background-color: #f4f4f9; color: #333; }}
                h1 {{ color: #2c3e50; }}
                h2 {{ color: #34495e; margin-top: 40px; }}
                table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border-radius: 8px; overflow: hidden; }}
                th, td {{ padding: 12px 15px; text-align: left; }}
                th {{ background-color: #3498db; color: white; font-weight: 600; text-transform: uppercase; font-size: 0.85rem; letter-spacing: 0.05em; }}
                tr {{ border-bottom: 1px solid #eeeeee; }}
                tr:nth-child(even) {{ background-color: #f8fbfd; }}
                tr:last-child {{ border-bottom: none; }}
                tr:hover {{ background-color: #f1f1f1; }}
                .status-ok {{ color: green; font-weight: bold; }}
                .status-low {{ color: orange; font-weight: bold; }}
                .status-high {{ color: red; font-weight: bold; }}
            </style>
        </head>
        <body>
        <h1>Optimization Results</h1>
        <p><strong>Goal:</strong> {tdee:.0f} kcal | <strong>Status:</strong> {result['status']}</p>
        
        <h2>Nutrient Summary</h2>
        {df_summary.to_html(index=False, classes='summary-table')}
        
        <h2>Food Composition</h2>
        {df_foods.to_html(index=False, classes='foods-table')}
        
        </body></html>
        """
        
        report_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/logs/optimization_report.html"))
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(html_content)
            
        print(f"\nReport saved to: {report_path}")
        webbrowser.open('file://' + os.path.realpath(report_path))
        
    else:
        print("Optimization failed to find a solution.")

if __name__ == "__main__":
    run_task()
