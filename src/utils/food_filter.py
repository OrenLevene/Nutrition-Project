"""
Utility for pre-filtering foods by nutrient coverage to improve optimization scalability.
Uses a "best sources" approach: for each nutrient, include the top foods that provide
the most of that nutrient per calorie (relative to RDA).
"""
from typing import List, Dict, Set
from src.calculator.db_interface import FoodItem


# Patterns to exclude from optimization (industrial/impractical foods)
# Note: Sweets/desserts are ALLOWED - only truly impractical items excluded
# Note: Spices/herbs are ALLOWED - quantity limits handled in optimizer
EXCLUDE_PATTERNS = [
    # Industrial / non-food items
    'industrial',
    'leavening agent',
    'seed gum',
    'gums, seed',
    'guar gum',
    'gum',
    'infant formula',
    'medical food',
    'meat extender',
    'formulated bar',
    'protein isolate',
    'whey protein',
    'soy protein',
    'casein',
    'oil, industrial',
    'shortening',
    'margarine-like',
    'butter replacement',
    'egg substitute',
    'imitation',
    'analogue',
    'test sample',
    'experimental',
    
    # Fortified drinks and breakfast products (artificially high in nutrients)
    'breakfast type, low calorie',
    'breakfast type, powder',
    'fruit-flavored drink, powder',
    'orange-flavor drink',
    'instant breakfast',
    'drink mix, powder',
    'beverages, fruit punch-flavor drink',
    'beverages, grape drink',
    'sports drink',
    'energy drink',
    'meal replacement',
    'nutritional supplement',
    'fortified',
    
    # Liver and organ meats (extremely nutrient-dense but not commonly purchased)
    'liver',
    'giblets',
    'gizzard',
    
    # Ready meals / Canned processed foods
    'canned, condensed',
    'canned, prepared',
    'canned, ready-to-serve',
    'soup, canned',
    'soup, instant',
    'frozen dinner',
    'frozen meal',
    'frozen entree',
    'tv dinner',
    'microwave meal',
    'ready-to-eat',
    'heat and serve',
    'meal kit',
    
    # Exotic/game meats (not in regular supermarkets)
    'game meat',
    'squirrel',
    'bison',
    'elk',
    'deer',
    'venison',
    'rabbit',
    'moose',
    'bear',
    'caribou',
    'antelope',
    'wild boar',
    'horse',
    'frog legs',
    'turtle',
    'alligator',
    'ostrich',
    'emu',
    'quail',
    'pheasant',
    'chiton',
    'alaska native',
    'new zealand',
    
    # Variety meats / offal
    'variety meats',
    'by-products',
    'blood',
    'brain',
    'tongue',
    'heart',
    'kidney',
    'tripe',
    'sweetbreads',
    'intestine',
    'chitlins',
    'bone marrow',
    'spleen',
    'lungs',
    
    # Exotic/unusual plants and leaves
    'acerola',
    'drumstick leaves',
    'amaranth leaves',
    'jute, potherb',
    'new zealand spinach',
    'vinespinach',
    'pumpkin leaves',
    'sweet potato leaves',
    'cress, garden',
    'dock, raw',
    'prairie turnip',
    'agave, raw',
    'nopales',
    
    # Unusual seafood
    'mollusks, snail',
    'sea cucumber',
    
    # Baby food / specialty
    'baby food',
    'gerber',
    'stage 1',
    'stage 2',
    'toddler',
    
    # Restaurant / prepared foods
    'restaurant',
    'fast food',
    'school lunch',
    'hospital',
    
    # Regional/ethnic specialty (may not be available everywhere)
    'pastelitos',
    'apache',
    'luxury loaf',
    
    # Yeast extract (extremely high B vitamins but not a staple)
    'yeast extract',
    
    # Industrial/Ingredient type foods
    r"dried.*egg", r"egg.*dried",
    r"powder", r"isolate", r"concentrate",
    r"defatted", r"low fat.*flour",
    r"industrial", r"shortening",
    r"beverage", r"drink",  # Catch fortified drinks unless specific
    r"shake", r"supplement",
    r"rennet", r"vital wheat gluten",
    r"pectin", r"lecithin", r"gelatin, dry",
    
    # User feedback specific
    r"dried whole egg", r"egg yolk, dried",
    r"flour, soy", # Usually industrial
    r"oil, flaxseed, contains added", # Very specific blend
]


def is_excluded_food(food: FoodItem, user_exclusions: List[str] = None) -> bool:
    """
    Check if food matches any exclusion pattern.
    
    Args:
        food: The food item to check
        user_exclusions: Optional list of additional patterns to exclude (from user preferences)
    """
    name_lower = food.name.lower()
    
    # Check default exclusion patterns
    # We use regex matching to support patterns like "dried.*egg"
    import re
    for pattern in EXCLUDE_PATTERNS:
        if re.search(pattern, name_lower):
            return True
    
    # Check user-defined exclusion patterns
    if user_exclusions:
        for pattern in user_exclusions:
            if pattern.lower() in name_lower:
                return True
    
    return False


def filter_excluded_foods(foods: List[FoodItem], user_exclusions: List[str] = None) -> List[FoodItem]:
    """
    Remove foods matching exclusion patterns.
    
    Args:
        foods: List of FoodItem objects
        user_exclusions: Optional list of additional patterns to exclude (from user preferences)
    """
    filtered = [f for f in foods if not is_excluded_food(f, user_exclusions)]
    excluded_count = len(foods) - len(filtered)
    if excluded_count > 0:
        print(f"Excluded {excluded_count} industrial/impractical foods")
    return filtered


# User preference defaults - foods that can be toggled on/off via UI
# These are high-nutrient foods that some users may want to exclude
TOGGLEABLE_EXCLUSIONS = {
    'liver': {
        'patterns': ['liver'],
        'description': 'Liver and organ meats',
        'default_excluded': True,  # Excluded by default in EXCLUDE_PATTERNS
    },
    'yeast_extract': {
        'patterns': ['yeast extract', 'marmite', 'vegemite'],
        'description': 'Yeast extract spreads (Marmite, Vegemite)',
        'default_excluded': True,
    },
    'fortified_drinks': {
        'patterns': ['instant breakfast', 'drink mix, powder', 'meal replacement'],
        'description': 'Fortified drinks and meal replacements',
        'default_excluded': True,
    },
    'dried_herbs': {
        'patterns': ['dried', 'freeze-dried'],
        'description': 'Dried and freeze-dried herbs/vegetables',
        'default_excluded': False,
    },
    'fish_oil': {
        'patterns': ['fish oil', 'cod liver oil'],
        'description': 'Fish oils and cod liver oil',
        'default_excluded': False,
    },
}


# RDA values for scoring - use INTERNAL KEY NAMES from db_interface.py mapping
RDA_VALUES = {
    # Macros
    "protein": 50,
    "carbohydrate": 275,
    "fat": 65,
    "Fiber (g)": 28,
    "Sugar (g)": 50,  # Max limit, not RDA
    "Saturated Fat (g)": 20,  # Max limit
    "Monounsaturated Fat (g)": 20,
    "Polyunsaturated Fat (g)": 15,
    
    # Vitamins - use internal key names
    "Vitamin A (mcg RAE)": 900,
    "Vitamin C (mg)": 90,
    "Vitamin D (mcg)": 15,
    "Vitamin E (mg)": 15,
    "Vitamin K (mcg)": 120,
    "Thiamin (B1) (mg)": 1.2,
    "Riboflavin (B2) (mg)": 1.3,
    "Niacin (B3) (mg NE)": 16,
    "Vitamin B6 (mg)": 1.7,
    "Folate (mcg DFE)": 400,
    "Vitamin B12 (mcg)": 2.4,
    "Pantothenic Acid (mg)": 5,
    "Choline (mg)": 550,
    
    # Minerals - use internal key names
    "Calcium (mg)": 1000,
    "Iron (mg)": 8,
    "Magnesium (mg)": 420,
    "Phosphorus (mg)": 700,
    "Potassium (mg)": 4700,
    "Sodium (mg)": 2300,  # Max limit
    "Zinc (mg)": 11,
    "Copper (mg)": 0.9,
    "Manganese (mg)": 2.3,
    "Selenium (mcg)": 55,
}


def get_nutrient_value(food: FoodItem, nutrient_key: str) -> float:
    """
    Get nutrient value from food, handling different naming conventions.
    """
    # Direct match
    if nutrient_key in food.nutrients:
        return food.nutrients[nutrient_key]
    
    # Try lowercase match
    nutrient_lower = nutrient_key.lower()
    for key, value in food.nutrients.items():
        if key.lower() == nutrient_lower:
            return value
    
    # Try partial match (for simpler keys like "protein")
    for key, value in food.nutrients.items():
        if nutrient_lower in key.lower():
            return value
    
    return 0.0


def filter_foods_by_nutrient_coverage(
    foods: List[FoodItem],
    max_foods: int = 500,
    min_calories: float = 1.0,
    top_per_nutrient: int = 30
) -> List[FoodItem]:
    """
    Filter foods to ensure coverage of all nutrients.
    
    Strategy:
    1. For each nutrient, find top N foods that provide the most per 100 kcal (relative to RDA)
    2. Combine all these "best source" foods
    3. Fill remaining slots with overall nutrient-dense foods
    
    This ensures we have foods that can satisfy each individual nutrient constraint.
    """
    # Filter out very low calorie foods
    valid_foods = [f for f in foods if f.calories >= min_calories]
    
    # Track selected foods
    selected_ids: Set[str] = set()
    selected_foods: List[FoodItem] = []
    
    # For each nutrient, find the best sources per calorie
    for nutrient, rda in RDA_VALUES.items():
        # Calculate "RDA% per 100 kcal" for each food
        scored = []
        for food in valid_foods:
            if food.calories > 0:
                nutrient_value = get_nutrient_value(food, nutrient)
                # Score = (nutrient / RDA) / (calories / 100) = % of RDA per 100 kcal
                rda_pct_per_100kcal = (nutrient_value / rda) / (food.calories / 100) * 100
                scored.append((food, rda_pct_per_100kcal))
        
        # Sort by score (descending) and take top N
        scored.sort(key=lambda x: x[1], reverse=True)
        
        for food, score in scored[:top_per_nutrient]:
            if food.id not in selected_ids and score > 0:
                selected_ids.add(food.id)
                selected_foods.append(food)
    
    # If we haven't reached max_foods, fill with overall nutrient-dense foods
    if len(selected_foods) < max_foods:
        # Calculate overall density score for remaining foods
        remaining_foods = [f for f in valid_foods if f.id not in selected_ids]
        
        for food in remaining_foods:
            if food.calories > 0:
                total_score = 0
                for nutrient, rda in RDA_VALUES.items():
                    value = get_nutrient_value(food, nutrient)
                    total_score += (value / rda) / (food.calories / 100)
                food._density_score = total_score
            else:
                food._density_score = 0
        
        remaining_foods.sort(key=lambda f: getattr(f, '_density_score', 0), reverse=True)
        
        for food in remaining_foods:
            if len(selected_foods) >= max_foods:
                break
            selected_foods.append(food)
    
    print(f"Selected {len(selected_foods)} foods covering {len(RDA_VALUES)} nutrients")
    return selected_foods[:max_foods]


# Keep old function for backward compatibility
def filter_foods_by_nutrient_density(
    foods: List[FoodItem],
    max_foods: int = 500,
    min_calories: float = 1.0,
    target_nutrients: List[str] = None
) -> List[FoodItem]:
    """
    Filter foods by nutrient coverage (wrapper for backward compatibility).
    Now uses the improved coverage-based algorithm.
    """
    return filter_foods_by_nutrient_coverage(
        foods=foods,
        max_foods=max_foods,
        min_calories=min_calories
    )


def calculate_nutrient_density(food: FoodItem, target_nutrients: List[str] = None) -> float:
    """
    Calculate nutrient density score for a food.
    Score = Sum of (nutrient / RDA) / calories
    """
    if food.calories <= 0:
        return 0.0
    
    total_score = 0.0
    for nutrient, rda in RDA_VALUES.items():
        value = get_nutrient_value(food, nutrient)
        if rda > 0:
            contribution = value / rda
            total_score += contribution
    
    density = (total_score / food.calories) * 100 if food.calories > 0 else 0
    return density
