import sys
import pandas as pd
from pathlib import Path

# Setup paths to import shared utils
DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(DATA_PIPELINE_DIR / 'pipeline' / 'utils'))
from reference_cleaning import clean_nutrient_columns, standardize_column_names, sort_columns, enforce_uniform_schema

import numpy as np

# FooDB food_group → semantic_descriptor mapping
FOODB_TAXONOMY = {
    'Vegetables':                   'plant | variable | variable',
    'Fruits':                       'plant | raw | whole',
    'Herbs and Spices':             'plant | dried | ground',
    'Nuts':                         'plant | raw | whole',
    'Cereals and cereal products':  'plant | processed | milled',
    'Pulses':                       'plant | variable | variable',
    'Soy':                          'plant | variable | variable',
    'Teas':                         'plant | processed | brewed',
    'Coffee':                       'plant | processed | brewed',
    'Cocoa':                        'plant | processed | manufactured',
    'Gourds':                       'plant | raw | whole',
    'Baking goods':                 'plant | processed | manufactured',
    'Animal foods':                 'animal | variable | variable',
    'Milk and milk products':       'animal | variable | variable',
    'Eggs':                         'animal | raw | whole',
    'Aquatic foods':                'animal | raw | whole',
    'Fats and oils':                'mixed | processed | extracted',
    'Beverages':                    'mixed | liquid | variable',
    'Snack foods':                  'mixed | processed | manufactured',
    'Confectioneries':              'mixed | processed | manufactured',
    'Dishes':                       'mixed | processed | cooked',
    'Baby foods':                   'mixed | processed | manufactured',
}

def process_silver_foodb(df):
    """Clean FooDB: standardize, enforce types, round. Map food_group taxonomy."""
    
    # Semantic Descriptor from food_group
    if 'food_group' in df.columns:
        df['semantic_descriptor'] = df['food_group'].map(FOODB_TAXONOMY).fillna('unknown | unknown | unknown')
    else:
        df['semantic_descriptor'] = 'unknown | unknown | unknown'
    
    # Extract weight from Serving Size
    if 'serving_size' in df.columns and 'serving_size_unit' in df.columns:
        df['Total Weight (g)'] = np.nan
        
        mask_g = df['serving_size_unit'].astype(str).str.lower().isin(['g', 'ml'])
        df.loc[mask_g, 'Total Weight (g)'] = pd.to_numeric(df.loc[mask_g, 'serving_size'], errors='coerce')
        
        mask_oz = df['serving_size_unit'].astype(str).str.lower().isin(['oz', 'fl oz'])
        df.loc[mask_oz, 'Total Weight (g)'] = pd.to_numeric(df.loc[mask_oz, 'serving_size'], errors='coerce') * 28.35

    df = standardize_column_names(df)
    df = enforce_uniform_schema(df)
    clean_nutrient_columns(df)
    df = sort_columns(df)
    return df

