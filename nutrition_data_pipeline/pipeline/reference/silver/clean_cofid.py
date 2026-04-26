import sys
import pandas as pd
from pathlib import Path

# Setup paths to import shared utils
DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(DATA_PIPELINE_DIR / 'pipeline' / 'utils'))
from reference_cleaning import clean_nutrient_columns, standardize_column_names, sort_columns, enforce_uniform_schema

import numpy as np
import re

# CoFID has NO category column, so we classify from description keywords.
COFID_KEYWORD_RULES = [
    # (keywords_list, descriptor)
    (['beef', 'steak', 'mince', 'veal', 'lamb', 'pork', 'bacon', 'ham', 'sausage', 'gammon'],
     'animal | variable | variable'),
    (['chicken', 'turkey', 'duck', 'goose', 'poultry'],
     'animal | variable | variable'),
    (['fish', 'cod', 'salmon', 'tuna', 'mackerel', 'haddock', 'prawn', 'crab', 'lobster', 'mussel', 'oyster', 'shellfish', 'sardine', 'anchov'],
     'animal | variable | variable'),
    (['milk', 'cheese', 'yoghurt', 'yogurt', 'cream', 'butter', 'curd', 'whey'],
     'animal | variable | variable'),
    (['egg'],
     'animal | raw | whole'),
    (['bread', 'flour', 'pasta', 'rice', 'noodle', 'cereal', 'oat', 'wheat', 'barley', 'rye', 'corn', 'maize'],
     'plant | processed | milled'),
    (['cake', 'biscuit', 'pastry', 'scone', 'muffin', 'crumpet', 'croissant', 'pie'],
     'plant | processed | baked'),
    (['apple', 'banana', 'orange', 'grape', 'pear', 'berry', 'melon', 'mango', 'peach', 'plum', 'cherry', 'fruit', 'lemon', 'lime', 'kiwi', 'apricot', 'fig', 'date', 'raisin', 'sultana', 'prune', 'rhubarb'],
     'plant | raw | whole'),
    (['potato', 'carrot', 'onion', 'tomato', 'pepper', 'broccoli', 'cabbage', 'spinach', 'lettuce', 'pea', 'bean', 'lentil', 'chickpea', 'vegetable', 'mushroom', 'courgette', 'aubergine', 'cauliflower', 'sweetcorn', 'asparagus', 'celery', 'cucumber', 'leek', 'turnip', 'swede', 'parsnip', 'beetroot', 'sprout'],
     'plant | variable | variable'),
    (['nut', 'almond', 'walnut', 'cashew', 'peanut', 'hazelnut', 'pistachio', 'seed', 'sesame', 'sunflower'],
     'plant | raw | whole'),
    (['oil', 'margarine', 'lard', 'dripping', 'shortening', 'ghee'],
     'mixed | processed | extracted'),
    (['sugar', 'chocolate', 'sweet', 'toffee', 'candy', 'honey', 'jam', 'marmalade', 'syrup', 'treacle', 'fudge'],
     'mixed | processed | manufactured'),
    (['beer', 'wine', 'spirit', 'lager', 'cider', 'ale', 'whisky', 'gin', 'vodka', 'rum', 'brandy', 'liqueur', 'port', 'sherry', 'stout'],
     'mixed | liquid | alcoholic'),
    (['juice', 'squash', 'cordial', 'water', 'tea', 'coffee', 'cocoa', 'drink', 'cola', 'lemonade', 'tonic'],
     'mixed | liquid | variable'),
    (['sauce', 'gravy', 'soup', 'stock', 'ketchup', 'mayonnaise', 'dressing', 'pickle', 'chutney', 'vinegar', 'mustard'],
     'mixed | processed | cooked'),
    (['herb', 'spice', 'pepper', 'salt', 'cinnamon', 'ginger', 'garlic', 'curry', 'chilli', 'thyme', 'rosemary', 'basil', 'mint', 'parsley', 'oregano', 'cumin', 'nutmeg', 'coriander', 'dill', 'sage', 'paprika', 'turmeric'],
     'plant | dried | ground'),
]

def classify_cofid_description(desc):
    """Keyword-match CoFID description to a semantic descriptor."""
    if pd.isna(desc):
        return 'unknown | unknown | unknown'
        
    # Strip cooking suffixes to prevent oils/butter from hijacking the taxonomy
    # Strip cooking suffixes to prevent oils/butter from hijacking the taxonomy
    try:
        import re
        match = re.search(r',?\s*([^,]*?(?:fried|roasted|boiled|cooked|steamed|poached|microwaved|baked|canned|flesh|skin|raw|fresh|dry|dried).*?$)', str(desc), re.IGNORECASE)
        if match:
            core_food = str(desc)[:match.start()].strip()
            if not core_food: core_food = desc
        else:
            core_food = desc
    except:
        core_food = desc
        
    desc_lower = str(core_food).lower()
    for keywords, descriptor in COFID_KEYWORD_RULES:
        if any(kw in desc_lower for kw in keywords):
            return descriptor
            
    # Fallback to original string if core food matched nothing
    desc_full_lower = str(desc).lower()
    for keywords, descriptor in COFID_KEYWORD_RULES:
        if any(kw in desc_full_lower for kw in keywords):
            return descriptor
            
    return 'unknown | unknown | unknown'

from base_cleaner import BaseNutritionCleaner

class CoFIDCleaner(BaseNutritionCleaner):
    def __init__(self):
        super().__init__(source_name="CoFID")

    def apply_taxonomy(self, df):
        if 'description' in df.columns:
            df['semantic_descriptor'] = df['description'].apply(classify_cofid_description)
        elif 'name' in df.columns:
            df['semantic_descriptor'] = df['name'].apply(classify_cofid_description)
        else:
            df['semantic_descriptor'] = 'unknown | unknown | unknown'
        return df

    def extract_weights(self, df):
        if 'serving_size' in df.columns and 'serving_size_unit' in df.columns:
            df['Total Weight (g)'] = np.nan
            
            mask_g = df['serving_size_unit'].astype(str).str.lower().isin(['g', 'ml'])
            df.loc[mask_g, 'Total Weight (g)'] = pd.to_numeric(df.loc[mask_g, 'serving_size'], errors='coerce')
            
            mask_oz = df['serving_size_unit'].astype(str).str.lower().isin(['oz', 'fl oz'])
            df.loc[mask_oz, 'Total Weight (g)'] = pd.to_numeric(df.loc[mask_oz, 'serving_size'], errors='coerce') * 28.35
        return df

def process_silver_cofid(df):
    """Entry point for legacy build script integration."""
    cleaner = CoFIDCleaner()
    return cleaner.execute(df)
