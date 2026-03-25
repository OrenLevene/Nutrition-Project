
import json
import os
from typing import Dict, Any, Optional, Tuple, Union

DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "config", "nutrient_limits.json")

class NutrientLimitResolver:
    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH):
        self.config = self._load_config(config_path)

    def _load_config(self, path: str) -> Dict[str, Any]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Config file not found: {path}")
        with open(path, 'r') as f:
            return json.load(f)

    def _parse_age_range(self, age_range_str: str) -> Tuple[int, int]:
        if "-" in age_range_str:
            start, end = map(int, age_range_str.split("-"))
            return start, end
        elif "+" in age_range_str:
            start = int(age_range_str.replace("+", ""))
            return start, 150 # Upper bound default
        return 0, 150

    def _match_profile(self, profiles: list, gender: str, age: int) -> Optional[Dict[str, Any]]:
        for profile in profiles:
            # Check Gender
            p_genders = profile.get("gender", "").split("|")
            if gender.lower() not in [g.lower() for g in p_genders]:
                continue
            
            # Check Age
            age_range = profile.get("age", "0-150")
            min_age, max_age = self._parse_age_range(age_range)
            if not (min_age <= age <= max_age):
                continue
                
            return profile
        return None

    def calculate_calories(self, 
                          gender: str, 
                          age: int, 
                          weight_kg: float, 
                          height_cm: float, 
                          activity_level: str = "sedentary", 
                          goal: str = "general") -> float:
        """
        Calculates TDEE using Mifflin-St Jeor Equation and Activity Multipliers.
        """
        # 1. BMR Calculation (Mifflin-St Jeor)
        if gender.lower() == "male":
            bmr = (10 * weight_kg) + (6.25 * height_cm) - (5 * age) + 5
        else:
            bmr = (10 * weight_kg) + (6.25 * height_cm) - (5 * age) - 161
            
        # 2. Activity Multiplier
        # Standard FAO/WHO/UNU multipliers
        multipliers = {
            "sedentary": 1.2,
            "lightly_active": 1.375,
            "moderately_active": 1.55,
            "active": 1.55, # Alias
            "very_active": 1.725,
            "super_active": 1.9,
            "athlete": 1.9 # Alias
        }
        mult = multipliers.get(activity_level, 1.2)
        tdee = bmr * mult
        
        # 3. Goal Adjustment
        if goal in ["muscle_gain", "build", "bulk"]:
            tdee += 300 # mild surplus
        elif goal in ["fat_loss", "cut", "lose_weight"]:
            tdee -= 500 # standard deficit
            
        return round(tdee, 0)

    def get_limits(self, 
                   gender: str, 
                   age: int, 
                   weight_kg: float, 
                   target_calories: float,
                   activity_level: str = "sedentary",
                   goal: str = "general") -> Dict[str, Tuple[float, Optional[float]]]:
        
        limits = {}
        
        for nutrient, data in self.config.get("nutrients", {}).items():
            profiles = data.get("profiles", [])
            profile = self._match_profile(profiles, gender, age)
            
            if not profile:
                continue
                
            strategy = data.get("strategy", "absolute")
            
            min_val = profile.get("min")
            max_val = profile.get("max")
            
            # Handle Strategies
            if strategy == "bodyweight":
                # Special logic for protein mainly
                # If optimal values exist and goal matches, use them
                if "protein" in nutrient.lower() and goal in ["muscle_gain", "build"]:
                    optimal = profile.get("optimal_build")
                    if optimal:
                        min_val = optimal
                elif "protein" in nutrient.lower() and activity_level in ["active", "very_active"]:
                     optimal = profile.get("optimal_active")
                     if optimal:
                        min_val = optimal
                
                if min_val: min_val = min_val * weight_kg
                if max_val: max_val = max_val * weight_kg
                
                # Check for max_safe if explicit max not set
                if max_val is None and profile.get("max_safe"):
                    max_val = profile.get("max_safe") * weight_kg

            elif strategy == "percent_calories":
                # Convert % to grams
                # Calories per gram: Fat=9, Carbs=4, Protein=4
                cals_per_g = 4
                if "fat" in nutrient.lower(): cals_per_g = 9
                elif "carb" in nutrient.lower(): cals_per_g = 4
                
                if min_val: min_val = (target_calories * (min_val / 100)) / cals_per_g
                if max_val: max_val = (target_calories * (max_val / 100)) / cals_per_g
                
                # Absolute min check (e.g. 130g carbs)
                abs_min = profile.get("absolute_min_g")
                if abs_min and min_val is not None:
                     min_val = max(min_val, abs_min)

            # Round values
            if min_val is not None: min_val = round(min_val, 2)
            if max_val is not None: max_val = round(max_val, 2)
            
            limits[nutrient] = (min_val, max_val)
            
        return limits
