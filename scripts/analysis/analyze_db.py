import pandas as pd
import os

# Load database
db_path = r"c:\Users\Oren Arie Levene\Nutrition Project\data\processed\real_food_nutrition.parquet"
df = pd.read_parquet(db_path)

print(f"Original items: {len(df)}")

# Define categories
exclude_keywords = [
    'protein bar', 'meal replacement', 'supplement', 'powder', 
    'isolate', 'industrial', 'shortening', 'starch', 'flour', 
    'formula', 'shake', 'beverage mix', 'whey', 'casein', 'soy protein',
    'nutrition bar', 'energy bar', 'power bar', 'breakfast bar'
]

def categorizer(desc):
    desc_lower = desc.lower()
    
    # 1. Strong Exclusions
    for kw in exclude_keywords:
        # Exception for "flour" if it's "cauliflower" or similar? No, usually separate word.
        # But 'flour' keyword might catch 'flower'. 
        # User explicitly said "flour is not a food".
        if kw in desc_lower:
             # Check context for powder
             if kw == 'powder':
                 if 'baking' in desc_lower: return "Exclude (Ingredient)"
                 if 'garlic' in desc_lower or 'onion' in desc_lower or 'chili' in desc_lower or 'spice' in desc_lower or 'curry' in desc_lower:
                     return "Keep (Spice)"
                 return f"Exclude ({kw})"
             
             return f"Exclude ({kw})"
    
    # 2. Ingredients / Non-Food
    if 'industrial' in desc_lower: return "Exclude (Industrial)"
    if 'yeast' in desc_lower and 'baker' in desc_lower: return "Exclude (Ingredient)"
    if 'baking' in desc_lower and ('soda' in desc_lower or 'chocolate' in desc_lower and 'unsweetened' in desc_lower): return "Exclude (Ingredient)"
    
    # 3. Dry Goods Logic
    if 'dry' in desc_lower:
        # Keep list
        if any(x in desc_lower for x in ['pasta', 'rice', 'noodle', 'spaghetti', 'macaroni', 'couscous', 'bulgur', 'farro', 'barley', 'oat', 'quinoa', 'wheat', 'bean', 'lentil', 'pea', 'chickpea', 'nut', 'almond', 'cashew', 'walnut', 'pecan', 'seed', 'fruit', 'tomatoes', 'apricot', 'prune', 'raisin', 'date', 'fig']):
             if 'mix' in desc_lower and 'seasoning' in desc_lower:
                 pass # Fall through to exclude mix? "Yellow rice with seasoning, dry packet mix" -> Exclude
             else:
                 return "Keep"
        
        # Exclude list
        if any(x in desc_lower for x in ['mix', 'pudding', 'beverage', 'drink', 'custard', 'gelatin', 'powder']):
            return "Exclude (Dry Mix)"
            
    # 4. Bar Logic
    if ' bar' in desc_lower or 'bar,' in desc_lower:
        # Already filtered "protein bar" etc above.
        # Check for granola/cereal bars that might be junk but are "food".
        # User wants "normal food someone would buy".
        # Granola bars are normal food. Protein bars are supplements.
        if 'granola' in desc_lower or 'cereal' in desc_lower or 'fruit' in desc_lower:
             return "Keep"
        # If it's something else, maybe review? 
        return "Review (Bar)"

    return "Keep"

df['Action'] = df['description'].apply(categorizer)

# Post-processing refining
# Remove "Yellow rice with seasoning, dry packet mix" if it wasn't caught
mask_mix = (df['description'].str.contains('mix', case=False)) & (df['description'].str.contains('dry', case=False)) & (df['description'].str.contains('rice|pasta', case=False))
df.loc[mask_mix, 'Action'] = 'Exclude (Dry Mix)'

print("\nSummary of Actions:")
print(df['Action'].value_counts())

# Filter
df_refined = df[df['Action'].str.contains('Keep')]
print(f"\nRefined items: {len(df_refined)}")

# Save
output_path = r"c:\Users\Oren Arie Levene\Nutrition Project\data\processed\refined_nutrition.parquet"
df_refined.to_parquet(output_path)
print(f"Saved to {output_path}")

# Verification Samples
print("\n--- Removed Items Sample ---")
print(df[~df['Action'].str.contains('Keep')]['description'].sample(20, replace=True).drop_duplicates().to_string())
