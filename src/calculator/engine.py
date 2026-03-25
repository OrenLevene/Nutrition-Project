"""
Nutrition requirements calculator engine.

Calculates BMR, TDEE, and micronutrient needs based on user profile.
"""
import json
import argparse
import os
from typing import Dict, Any, Optional, Tuple, List


def in_range(age_val: float, range_str: str) -> bool:
    """
    Check if an age value falls within a range string.
    
    Parses range strings like "0-6 months", "1-3 years", "19+ years".
    Returns False for special keys like "athlete_strength".
    """
    
    parts = range_str.split(' ')
    if len(parts) < 2:
        return False
        
    unit = parts[1]
    range_part = parts[0]
    
    start_age, end_age = 0, float('inf')
    
    if '+' in range_part:
        start_age = float(range_part.replace('+', ''))
    elif '-' in range_part:
        s, e = range_part.split('-')
        start_age = float(s)
        end_age = float(e)
    else:
        return False # Should not happen with current data
        
    if unit == 'months':
        start_age /= 12.0
        end_age /= 12.0
    
    return start_age <= age_val <= end_age


def load_data(filepath: str = "data/config/nutrition_data.json") -> Optional[Dict[str, Any]]:
    """Load nutrition configuration data from JSON file."""
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Data file not found at {filepath}")
        return None

def calculate_bmr(weight_kg: float, height_cm: float, age_years: float, gender: str) -> float:
    """Calculate Basal Metabolic Rate using Mifflin-St Jeor Equation."""
    if gender.lower() == 'male':
        return (10 * weight_kg) + (6.25 * height_cm) - (5 * age_years) + 5
    elif gender.lower() == 'female':
        return (10 * weight_kg) + (6.25 * height_cm) - (5 * age_years) - 161
    else:
        raise ValueError("Gender must be 'male' or 'female'")

def calculate_tdee_simple(
    bmr: float, activity_level: str, data: Dict[str, Any]
) -> Tuple[float, float, str]:
    """Calculate TDEE using activity level multiplier."""
    levels = data['constants']['activity_levels']
    if activity_level not in levels:
        raise ValueError(f"Activity level must be one of: {', '.join(levels.keys())}")
    
    multiplier = levels[activity_level]['multiplier']
    return bmr * multiplier, multiplier, activity_level

def calculate_tdee_advanced(
    bmr: float, weight_kg: float, schedule_path: str, data: Dict[str, Any]
) -> Tuple[float, float, List[str]]:
    """Calculate TDEE using detailed activity schedule with MET values."""
    try:
        with open(schedule_path, 'r') as f:
            schedule = json.load(f)
    except FileNotFoundError:
        raise ValueError(f"Schedule file not found: {schedule_path}")

    met_values = data['constants'].get('met_values', {})
    total_weekly_calories = 0
    activity_breakdown = []

    for item in schedule:
        activity = item['activity']
        duration = item['duration_minutes']
        intensity = item.get('intensity', 'medium')
        freq = item.get('frequency_per_week', 1) # Default to once if not specified, but typically schedule is a list of all activities in a week. Or list of types.
        # Let's assume the schedule list IS the weekly schedule. 
        # But if user put 'frequency_per_week', we honor it.
        
        if activity not in met_values:
            print(f"Warning: Activity '{activity}' not found in database. Skipping.")
            continue
            
        met = met_values[activity].get(intensity, met_values[activity]['medium'])
        
        # Formula: Calories = MET * Weight(kg) * (Duration(min)/60)
        cals_per_session = met * weight_kg * (duration / 60.0)
        weekly_cals = cals_per_session * freq
        
        total_weekly_calories += weekly_cals
        activity_breakdown.append(f"{activity} ({intensity}): {freq}x {duration}min = {weekly_cals:.0f} kcal/week")

    daily_exercise_calories = total_weekly_calories / 7.0
    
    # Base TDEE = Sedentary (BMR * 1.2) + Daily Exercise Activity
    # 1.2 accounts for TEF and NEAT (sedentary baseline)
    base_tdee = bmr * 1.2
    total_tdee = base_tdee + daily_exercise_calories
    
    effective_multiplier = total_tdee / bmr
    
    return total_tdee, effective_multiplier, activity_breakdown

def get_micronutrients(
    age: float, gender: str, data: Dict[str, Any], goal: str = 'maintenance'
) -> Dict[str, Any]:
    """Get recommended micronutrient intakes based on age, gender, and goal."""
    requirements = {}
    
    goal_key_map = {
        'muscle_gain': 'athlete_strength',
        'athletic_performance': 'athlete_endurance' # Simplification
    }
    
    special_key = goal_key_map.get(goal)
    
    for category in ['vitamins', 'minerals', 'other']:
        requirements[category] = {}
        for nutrient, ranges in data['rda'][category].items():
            found = False
            # Goal-specific override takes priority
            if special_key and special_key in ranges:
                values = ranges[special_key].get(gender.lower(), ranges[special_key].get('all'))
                requirements[category][nutrient] = values
                found = True
            
            # Fallback to age/gender-based lookup
            if not found:
                for range_str, values in ranges.items():
                    if in_range(age, range_str):
                        val = values.get(gender.lower(), values.get('all'))
                        requirements[category][nutrient] = val
                        found = True
                        break
            
            if not found:
                requirements[category][nutrient] = "Not specified"
                
    return requirements

def main() -> None:
    """CLI entry point for nutrition requirements calculator."""
    parser = argparse.ArgumentParser(description="Nutrition Requirements Calculator")
    parser.add_argument('--age', type=float, required=True, help="Age in years")
    parser.add_argument('--gender', type=str, required=True, choices=['male', 'female'], help="Gender")
    parser.add_argument('--weight', type=float, required=True, help="Weight in kg")
    parser.add_argument('--height', type=float, required=True, help="Height in cm")
    parser.add_argument('--mode', choices=['simple', 'advanced'], default='simple', help="Calculation mode")
    parser.add_argument('--goal', choices=['maintenance', 'muscle_gain', 'fat_loss', 'athletic_performance'], default='maintenance', help="Goal")
    parser.add_argument('--activity', type=str, 
                        choices=['sedentary', 'lightly_active', 'moderately_active', 'very_active', 'super_active'],
                        help="Activity level (required for simple mode)")
    parser.add_argument('--schedule', type=str, help="Path to JSON activity schedule (required for advanced mode)")
    
    args = parser.parse_args()
    
    data = load_data()
    if not data:
        return

    try:
        bmr = calculate_bmr(args.weight, args.height, args.age, args.gender)
        
        if args.mode == 'simple':
            if not args.activity:
                print("Error: --activity is required for simple mode")
                return
            tdee, multiplier, label = calculate_tdee_simple(bmr, args.activity, data)
            calc_note = f"Activity Level: {label}"
        else:
            if not args.schedule:
                print("Error: --schedule is required for advanced mode")
                return
            tdee, multiplier, breakdown = calculate_tdee_advanced(bmr, args.weight, args.schedule, data)
            calc_note = f"Custom Schedule (Effective Multiplier: {multiplier:.2f})"
        
        goal_note = ""
        if args.goal == 'muscle_gain':
            tdee += 250
            goal_note = "(+250 kcal surplus for muscle gain)"
        elif args.goal == 'fat_loss':
            tdee -= 500
            goal_note = "(-500 kcal deficit for fat loss)"

    except ValueError as e:
        print(f"Calculation Error: {e}")
        return

    micros = get_micronutrients(args.age, args.gender, data, args.goal)
    
    from src.utils.nutrient_limits import NutrientLimitResolver
    resolver = NutrientLimitResolver()
    
    limits = resolver.get_limits(
        gender=args.gender, 
        age=args.age, 
        weight_kg=args.weight, 
        target_calories=tdee,
        activity_level=args.activity if args.mode == 'simple' else 'sedentary', # Advanced mode TDEE is custom, but AMDR logic needs a base.
        goal=args.goal
    )

    # Output
    print("\n--- NUTRITION REPORT ---")
    print(f"Profile: {args.age}y, {args.gender}, {args.weight}kg, {args.height}cm")
    print(f"Goal: {args.goal} {goal_note}")
    print(f"{calc_note}")
    if args.mode == 'advanced':
         print("Activity Breakdown:")
         for line in breakdown:
             print(f"  - {line}")
         print(f"  > Avg Daily Exercise Calories: {(tdee - (bmr*1.2) - (250 if args.goal=='muscle_gain' else 0) + (500 if args.goal=='fat_loss' else 0)):.0f} kcal")

    print("-" * 30)
    print(f"BMR (Basal Metabolic Rate): {bmr:.0f} kcal/day")
    print(f"TDEE (Total Daily Energy Expenditure): {tdee:.0f} kcal/day")
    print("-" * 30)
    
    print("Estimated Macronutrient Needs (Science-Backed Ranges):")
    # Display Macros from the resolver output
    macros = ["protein", "fat", "carbohydrate", "fiber"]
    
    # Helper to print range
    def print_range(name, val):
        min_v, max_v = val
        s = f"{min_v}"
        if max_v:
             s += f" - {max_v}"
        else:
             s += "+"
        return s

    for m in macros:
        if m in limits:
            # For macros resolver returns g or %?. It returns grams based on the logic I implemented (percent_calories converted to g)
            # wait, I should verify NutrientLimitResolver logic.
            # Yes, logic converts % to grams using target_calories.
            
            unit = "g"
            if m == "fiber": unit = "g"
            
            val = limits[m]
            
            # Print label
            label = m.capitalize()
            if m == "carbohydrate": label = "Carbs"
            
            print(f"{label}: {print_range(m, val)} {unit}")
            
    print("-" * 30)
    print("Micronutrient Recommendations (RDA/AI - UL):")
    
    for nutrient, val in limits.items():
        if nutrient in macros: continue
        # Formatting
        print(f"  {nutrient}: {print_range(nutrient, val)}")

if __name__ == "__main__":
    main()
