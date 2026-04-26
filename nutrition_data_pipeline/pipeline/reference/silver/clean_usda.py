import sys
import pandas as pd
from pathlib import Path

# Setup paths to import shared utils
DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(DATA_PIPELINE_DIR / 'pipeline' / 'utils'))
from reference_cleaning import clean_nutrient_columns, standardize_column_names, sort_columns, enforce_uniform_schema

import numpy as np

# USDA food_category → semantic_descriptor mapping
# Format: "origin | state | process"
USDA_TAXONOMY = {
    'Legumes and Legume Products':      'plant | variable | variable',
    'Vegetables and Vegetable Products':'plant | variable | variable',
    'Fruits and Fruit Juices':          'plant | variable | variable',
    'Cereal Grains and Pasta':          'plant | processed | milled',
    'Baked Products':                   'plant | processed | baked',
    'Breakfast Cereals':                'plant | processed | manufactured',
    'Nut and Seed Products':            'plant | raw | whole',
    'Spices and Herbs':                 'plant | dried | ground',
    'Fats and Oils':                    'mixed | processed | extracted',
    'Dairy and Egg Products':           'animal | variable | variable',
    'Beef Products':                    'animal | raw | whole',
    'Pork Products':                    'animal | raw | whole',
    'Poultry Products':                 'animal | raw | whole',
    'Lamb, Veal, and Game Products':    'animal | raw | whole',
    'Sausages and Luncheon Meats':      'animal | processed | cured',
    'Finfish and Shellfish Products':   'animal | raw | whole',
    'Soups, Sauces, and Gravies':       'mixed | processed | cooked',
    'Sweets':                           'mixed | processed | manufactured',
    'Beverages':                        'mixed | liquid | variable',
    'Snacks':                           'mixed | processed | manufactured',
    'Baby Foods':                       'mixed | processed | manufactured',
    'Meals, Entrees, and Side Dishes':  'mixed | processed | cooked',
    'American Indian/Alaska Native Foods':'mixed | variable | variable',
}

from base_cleaner import BaseNutritionCleaner

class USDACleaner(BaseNutritionCleaner):
    def __init__(self):
        super().__init__(source_name="USDA")

    def apply_taxonomy(self, df):
        if 'food_category' in df.columns:
            df['semantic_descriptor'] = df['food_category'].map(USDA_TAXONOMY).fillna('unknown | unknown | unknown')
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

def process_silver_usda(df):
    """Entry point for legacy build script integration."""
    cleaner = USDACleaner()
    return cleaner.execute(df)
