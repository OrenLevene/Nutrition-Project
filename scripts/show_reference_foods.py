"""
Show what's in our solid reference food data — focus on foods 
you'd actually find in a British supermarket.
"""
import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

# Load combined reference DB
ref = pd.read_parquet('data/processed/GOLD_REF_food_db.parquet')
print(f"Total reference foods: {len(ref)}")
print(f"  USDA: {(ref['data_source']=='USDA').sum()}")
print(f"  CoFID: {(ref['data_source']=='UK_CoFID').sum()}")

# What food categories exist?
if 'food_category' in ref.columns:
    print(f"\n{'='*60}")
    print("FOOD CATEGORIES (from USDA entries)")
    print(f"{'='*60}")
    cats = ref[ref['data_source']=='USDA']['food_category'].value_counts()
    for cat, count in cats.items():
        print(f"  {cat:<45} {count:>4}")

# CoFID doesn't have food_category — show by name patterns
cofid = ref[ref['data_source'] == 'UK_CoFID']
print(f"\n{'='*60}")
print(f"CoFID UK FOODS ({len(cofid)} entries) — sample by type")
print(f"{'='*60}")

categories = {
    'Bread & Bakery': ['bread', 'roll', 'baguette', 'croissant', 'scone', 'muffin', 'cake', 'biscuit'],
    'Dairy': ['milk', 'cheese', 'yogurt', 'yoghurt', 'cream', 'butter'],
    'Meat': ['beef', 'lamb', 'pork', 'chicken', 'turkey', 'sausage', 'bacon', 'ham'],
    'Fish': ['cod', 'salmon', 'tuna', 'haddock', 'prawn', 'fish'],
    'Fruit': ['apple', 'banana', 'orange', 'strawberry', 'grape', 'pear'],
    'Vegetables': ['potato', 'carrot', 'onion', 'tomato', 'broccoli', 'peas'],
    'Pasta/Rice/Grains': ['pasta', 'rice', 'noodle', 'couscous', 'oat', 'cereal'],
    'Drinks': ['juice', 'tea', 'coffee', 'cola', 'lemonade', 'beer', 'wine'],
    'Snacks': ['crisps', 'popcorn', 'nuts', 'chocolate', 'sweet'],
    'Ready meals': ['pie', 'pizza', 'curry', 'soup', 'sandwich', 'quiche'],
    'Condiments': ['sauce', 'ketchup', 'mustard', 'mayo', 'pickle', 'chutney'],
}

for cat_name, keywords in categories.items():
    pattern = '|'.join(keywords)
    matches = cofid[cofid['description'].str.lower().str.contains(pattern, regex=True)]
    if len(matches) > 0:
        print(f"\n  {cat_name} ({len(matches)} entries):")
        for _, row in matches.head(5).iterrows():
            cal = row.get('Energy (KCAL)', '?')
            print(f"    {row['description'][:55]:<55} {cal} kcal")
        if len(matches) > 5:
            print(f"    ... and {len(matches)-5} more")

# Nutrient coverage for the full combined DB
print(f"\n{'='*60}")
print("NUTRIENT COVERAGE (combined DB)")
print(f"{'='*60}")
key_nutrients = [
    ('Energy (KCAL)', 'Calories'),
    ('Protein (G)', 'Protein'),
    ('Carbohydrate, by difference (G)', 'Carbs'),
    ('Total lipid (fat) (G)', 'Fat'),
    ('Fiber, total dietary (G)', 'Fibre'),
    ('Calcium, Ca (MG)', 'Calcium'),
    ('Iron, Fe (MG)', 'Iron'),
    ('Vitamin C, total ascorbic acid (MG)', 'Vitamin C'),
    ('Zinc, Zn (MG)', 'Zinc'),
    ('Sodium, Na (MG)', 'Sodium'),
    ('Potassium, K (MG)', 'Potassium'),
    ('Folate, total (UG)', 'Folate'),
    ('Vitamin B-12 (UG)', 'Vitamin B12'),
]
for col, label in key_nutrients:
    if col in ref.columns:
        pct = ref[col].notna().sum() / len(ref) * 100
        bar = '█' * int(pct / 5) + '░' * (20 - int(pct / 5))
        print(f"  {label:<15} {bar} {pct:5.1f}%")
