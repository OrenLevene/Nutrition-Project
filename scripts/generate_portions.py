"""
Generate exact portion sizes for ALL foods in the database.
Each food gets its own entry with specific pack size and perishable status.
"""
import pandas as pd
import json
import re

# Load the database
df = pd.read_parquet('data/processed/real_food_nutrition.parquet')
foods = df['description'].tolist()

print(f"Processing {len(foods)} foods...")

def get_portion_info(food_name: str) -> dict:
    """
    Determine pack size and perishable status for a specific food.
    Uses knowledge of UK supermarket pack sizes.
    """
    name = food_name.lower()
    
    # Default values
    pack_size = 100
    is_perishable = False
    
    # ========== CANNED GOODS (discrete, use whole can) ==========
    if 'canned' in name or 'tinned' in name:
        is_perishable = True  # Must use whole can
        if 'bean' in name or 'pea' in name or 'lentil' in name:
            pack_size = 400
        elif 'tuna' in name:
            pack_size = 145
        elif 'salmon' in name:
            pack_size = 213
        elif 'sardine' in name:
            pack_size = 120
        elif 'mackerel' in name:
            pack_size = 125
        elif 'tomato' in name:
            pack_size = 400
        elif 'corn' in name or 'sweetcorn' in name:
            pack_size = 340
        elif 'soup' in name:
            pack_size = 400
        elif 'fruit' in name or 'peach' in name or 'pear' in name or 'pineapple' in name:
            pack_size = 410
        else:
            pack_size = 400
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== FRESH MEATS (perishable, typical pack sizes) ==========
    if any(m in name for m in ['beef', 'pork', 'lamb', 'veal', 'venison', 'bison', 'goat']):
        is_perishable = True
        if 'mince' in name or 'ground' in name:
            pack_size = 500
        elif 'steak' in name:
            pack_size = 400 if 'ribeye' in name or 'sirloin' in name else 350
        elif 'roast' in name or 'joint' in name:
            pack_size = 1000
        elif 'chop' in name:
            pack_size = 500
        elif 'rib' in name:
            pack_size = 700
        elif 'liver' in name or 'kidney' in name or 'heart' in name:
            pack_size = 400
        elif 'bacon' in name:
            pack_size = 300
        elif 'sausage' in name:
            pack_size = 400
        else:
            pack_size = 500
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== POULTRY (perishable) ==========
    if any(p in name for p in ['chicken', 'turkey', 'duck', 'goose', 'pheasant', 'quail']):
        is_perishable = True
        if 'breast' in name:
            pack_size = 500
        elif 'thigh' in name or 'drumstick' in name or 'leg' in name:
            pack_size = 500
        elif 'wing' in name:
            pack_size = 500
        elif 'whole' in name:
            pack_size = 1500
        elif 'mince' in name or 'ground' in name:
            pack_size = 500
        elif 'liver' in name:
            pack_size = 300
        else:
            pack_size = 500
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== PROCESSED MEATS ==========
    if any(m in name for m in ['ham', 'bacon', 'sausage', 'frankfurter', 'hot dog', 'bologna', 'salami', 'pepperoni', 'chorizo', 'prosciutto']):
        is_perishable = True
        if 'bacon' in name:
            pack_size = 300
        elif 'ham' in name:
            pack_size = 200
        elif 'sausage' in name or 'frankfurter' in name or 'hot dog' in name:
            pack_size = 400
        elif 'salami' in name or 'pepperoni' in name:
            pack_size = 100
        else:
            pack_size = 200
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== FRESH FISH & SEAFOOD ==========
    if any(f in name for f in ['fish', 'salmon', 'cod', 'haddock', 'trout', 'bass', 'tilapia', 'pollock', 'halibut', 'sole', 'plaice', 'herring', 'carp', 'catfish', 'perch', 'pike', 'snapper', 'swordfish', 'tuna steak', 'mahi']):
        is_perishable = True
        if 'fillet' in name:
            pack_size = 280
        elif 'steak' in name:
            pack_size = 300
        elif 'whole' in name:
            pack_size = 400
        elif 'smoked' in name:
            pack_size = 200
        else:
            pack_size = 300
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== SHELLFISH ==========
    if any(s in name for s in ['shrimp', 'prawn', 'crab', 'lobster', 'clam', 'mussel', 'oyster', 'scallop', 'squid', 'octopus', 'mollusk', 'crustacean']):
        is_perishable = True
        if 'prawn' in name or 'shrimp' in name:
            pack_size = 200
        elif 'mussel' in name:
            pack_size = 500
        else:
            pack_size = 200
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== EGGS ==========
    if 'egg' in name:
        is_perishable = True
        if 'dried' in name or 'powder' in name:
            pack_size = 250
            is_perishable = False
        else:
            pack_size = 360  # 6 eggs x 60g
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== DAIRY ==========
    if any(d in name for d in ['milk', 'cream', 'yogurt', 'yoghurt', 'cheese', 'butter', 'margarine']):
        is_perishable = True
        if 'milk' in name:
            if 'powder' in name or 'dried' in name or 'evaporated' in name or 'condensed' in name:
                pack_size = 400
                is_perishable = 'condensed' not in name  # Condensed cans are discrete
            else:
                pack_size = 2000  # 2L carton
        elif 'cream' in name:
            pack_size = 300
        elif 'yogurt' in name or 'yoghurt' in name:
            pack_size = 500
        elif 'cheese' in name:
            if 'cottage' in name:
                pack_size = 300
            elif 'cream cheese' in name:
                pack_size = 200
            elif 'parmesan' in name or 'grated' in name:
                pack_size = 100
            elif 'ricotta' in name:
                pack_size = 250
            else:
                pack_size = 400
        elif 'butter' in name:
            pack_size = 250
        elif 'margarine' in name:
            pack_size = 500
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== FRESH VEGETABLES ==========
    veg_fresh = ['tomato', 'potato', 'carrot', 'onion', 'broccoli', 'cauliflower', 'cabbage', 'spinach', 'lettuce', 'kale', 'pepper', 'mushroom', 'cucumber', 'celery', 'asparagus', 'pea', 'squash', 'courgette', 'zucchini', 'aubergine', 'eggplant', 'sweet potato', 'parsnip', 'beetroot', 'turnip', 'radish', 'leek', 'garlic', 'ginger', 'artichoke', 'avocado', 'brussels', 'chard', 'endive', 'fennel', 'kohlrabi', 'okra', 'rutabaga', 'shallot', 'watercress', 'arugula', 'rocket', 'romaine', 'iceberg']
    if any(v in name for v in veg_fresh) and 'frozen' not in name and 'canned' not in name and 'dried' not in name:
        is_perishable = True
        if 'potato' in name:
            pack_size = 2500
        elif 'carrot' in name or 'onion' in name:
            pack_size = 1000
        elif 'sweet potato' in name:
            pack_size = 1000
        elif 'tomato' in name:
            pack_size = 400
        elif 'broccoli' in name:
            pack_size = 350
        elif 'cauliflower' in name or 'cabbage' in name:
            pack_size = 500
        elif 'spinach' in name or 'lettuce' in name or 'kale' in name:
            pack_size = 200
        elif 'pepper' in name:
            pack_size = 500
        elif 'mushroom' in name:
            pack_size = 250
        elif 'cucumber' in name:
            pack_size = 400
        elif 'celery' in name:
            pack_size = 400
        elif 'asparagus' in name:
            pack_size = 250
        elif 'avocado' in name:
            pack_size = 320
        elif 'garlic' in name:
            pack_size = 50
        elif 'ginger' in name:
            pack_size = 100
        else:
            pack_size = 400
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== FROZEN VEGETABLES ==========
    if 'frozen' in name and any(v in name for v in ['vegetable', 'pea', 'corn', 'broccoli', 'spinach', 'carrot', 'green bean', 'mixed']):
        is_perishable = True
        pack_size = 900  # Typical frozen veg bag
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== FRESH FRUITS ==========
    fruits = ['apple', 'banana', 'orange', 'lemon', 'lime', 'grapefruit', 'pear', 'peach', 'plum', 'apricot', 'cherry', 'grape', 'strawberry', 'blueberry', 'raspberry', 'blackberry', 'melon', 'watermelon', 'pineapple', 'mango', 'kiwi', 'nectarine', 'papaya', 'passion', 'pomegranate', 'tangerine', 'clementine', 'mandarin', 'cantaloupe', 'honeydew', 'fig', 'date', 'cranberry', 'currant', 'gooseberry', 'guava', 'lychee', 'persimmon', 'quince', 'starfruit']
    if any(f in name for f in fruits) and 'dried' not in name and 'canned' not in name and 'juice' not in name:
        is_perishable = True
        if 'apple' in name or 'orange' in name or 'pear' in name:
            pack_size = 1000
        elif 'banana' in name:
            pack_size = 750
        elif 'grape' in name:
            pack_size = 500
        elif 'strawberry' in name:
            pack_size = 400
        elif 'blueberry' in name or 'raspberry' in name or 'blackberry' in name:
            pack_size = 150
        elif 'melon' in name or 'watermelon' in name:
            pack_size = 2000
        elif 'pineapple' in name:
            pack_size = 1000
        elif 'mango' in name:
            pack_size = 400
        elif 'lemon' in name or 'lime' in name:
            pack_size = 250
        elif 'grapefruit' in name:
            pack_size = 600
        else:
            pack_size = 500
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== DRIED FRUITS ==========
    if 'dried' in name or 'raisin' in name or 'prune' in name or 'date' in name or ('fig' in name and 'raw' not in name):
        pack_size = 250
        is_perishable = False
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== BEANS & LEGUMES (dried = continuous, canned handled above) ==========
    if any(b in name for b in ['bean', 'lentil', 'chickpea', 'pea', 'cowpea', 'soybean']):
        if 'dried' in name or 'raw' in name or 'mature' in name:
            pack_size = 500
            is_perishable = False
        else:
            pack_size = 400
            is_perishable = True
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== TOFU & SOY PRODUCTS ==========
    if 'tofu' in name or 'tempeh' in name or 'soy' in name:
        is_perishable = True
        pack_size = 280
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== GRAINS (non-perishable, continuous) ==========
    if any(g in name for g in ['rice', 'pasta', 'noodle', 'couscous', 'quinoa', 'bulgur', 'barley', 'millet', 'polenta', 'farro', 'spelt']):
        pack_size = 500 if 'noodle' in name else 1000
        is_perishable = False
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== CEREALS & BREAKFAST ==========
    if 'cereal' in name or 'oat' in name or 'granola' in name or 'muesli' in name:
        pack_size = 500
        is_perishable = False
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== FLOUR & BAKING ==========
    if 'flour' in name or 'cornmeal' in name or 'baking' in name:
        pack_size = 1500
        is_perishable = False
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== BREAD & BAKERY ==========
    if any(b in name for b in ['bread', 'roll', 'bagel', 'muffin', 'croissant', 'biscuit', 'cracker', 'tortilla', 'pita', 'naan', 'flatbread']):
        if 'cracker' in name or 'crisp' in name:
            pack_size = 200
            is_perishable = False
        else:
            pack_size = 400
            is_perishable = True
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== COOKIES & CAKES ==========
    if 'cookie' in name or 'cake' in name or 'pie' in name or 'pastry' in name or 'brownie' in name or 'donut' in name:
        pack_size = 300
        is_perishable = True
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== NUTS & SEEDS ==========
    if any(n in name for n in ['nut', 'almond', 'walnut', 'cashew', 'peanut', 'pistachio', 'hazelnut', 'pecan', 'macadamia', 'brazil', 'chestnut']):
        pack_size = 200
        is_perishable = False
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    if 'seed' in name or 'sunflower' in name or 'pumpkin' in name or 'chia' in name or 'flax' in name or 'sesame' in name:
        pack_size = 200
        is_perishable = False
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== OILS ==========
    if 'oil' in name:
        pack_size = 500
        is_perishable = False
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== SAUCES & CONDIMENTS ==========
    if any(s in name for s in ['sauce', 'salsa', 'dressing', 'mayonnaise', 'ketchup', 'mustard', 'gravy', 'marinade', 'vinegar']):
        if 'ready-to-serve' in name or 'jar' in name:
            pack_size = 500
            is_perishable = True
        else:
            pack_size = 300
            is_perishable = False
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== SOUPS (not canned) ==========
    if 'soup' in name:
        pack_size = 500
        is_perishable = True
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== BEVERAGES ==========
    if 'juice' in name:
        pack_size = 1000
        is_perishable = True
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    if any(b in name for b in ['beverage', 'drink', 'soda', 'cola', 'tea', 'coffee']):
        if 'alcoholic' in name or 'wine' in name or 'beer' in name or 'spirit' in name:
            pack_size = 750
            is_perishable = False
        elif 'tea' in name or 'coffee' in name:
            pack_size = 250
            is_perishable = False
        else:
            pack_size = 500
            is_perishable = True
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== SWEETS & SNACKS ==========
    if any(s in name for s in ['candy', 'chocolate', 'snack', 'chip', 'crisp', 'popcorn', 'pretzel']):
        pack_size = 150
        is_perishable = False
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    if 'pudding' in name or 'custard' in name or 'mousse' in name:
        pack_size = 150
        is_perishable = True
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    if 'ice cream' in name or 'frozen' in name:
        pack_size = 500
        is_perishable = True
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== SPREADS ==========
    if 'peanut butter' in name or 'almond butter' in name:
        pack_size = 340
        is_perishable = False
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    if 'jam' in name or 'jelly' in name or 'marmalade' in name or 'preserve' in name:
        pack_size = 454
        is_perishable = False
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    if 'honey' in name:
        pack_size = 340
        is_perishable = False
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    if 'hummus' in name:
        pack_size = 300
        is_perishable = True
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== SPICES & SEASONINGS ==========
    if 'spice' in name or 'seasoning' in name or 'herb' in name or 'pepper' in name and 'bell' not in name:
        pack_size = 50
        is_perishable = False
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== SUGAR & SWEETENERS ==========
    if 'sugar' in name or 'syrup' in name or 'molasses' in name:
        pack_size = 1000 if 'sugar' in name else 340
        is_perishable = False
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # ========== PROTEIN BARS & SUPPLEMENTS ==========
    if 'bar' in name or 'protein' in name:
        pack_size = 60
        is_perishable = False
        return {"pack_size": pack_size, "is_perishable": is_perishable}
    
    # Default fallback
    return {"pack_size": pack_size, "is_perishable": is_perishable}


# Process all foods
food_portions = {}
for food in foods:
    info = get_portion_info(food)
    food_portions[food] = info

# Build the config
config = {
    "_comment": "Individual portion sizes for each food in the database. is_perishable=true means discrete mode.",
    "default_portion_size": 100,
    "default_is_perishable": False,
    "foods": food_portions
}

# Save
with open('data/config/portion_sizes.json', 'w') as f:
    json.dump(config, f, indent=2)

# Stats
perishable_count = sum(1 for v in food_portions.values() if v['is_perishable'])
continuous_count = len(food_portions) - perishable_count

print(f"\nGenerated portion_sizes.json:")
print(f"  Total foods: {len(food_portions)}")
print(f"  Discrete (perishable/canned): {perishable_count}")
print(f"  Continuous (non-perishable): {continuous_count}")
