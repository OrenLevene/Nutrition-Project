import sys
from pathlib import Path

# Setup paths to import shared utils
DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(DATA_PIPELINE_DIR / 'pipeline' / 'utils'))
from reference_cleaning import clean_nutrient_columns, standardize_column_names, sort_columns

import numpy as np

def process_silver_foodb(df):
    """Clean FooDB: standardize, enforce types, round. Extract weight if available."""
    
    # Extract weight from Serving Size
    if 'serving_size' in df.columns and 'serving_size_unit' in df.columns:
        df['Total Weight (g)'] = np.nan
        
        mask_g = df['serving_size_unit'].astype(str).str.lower().isin(['g', 'ml'])
        df.loc[mask_g, 'Total Weight (g)'] = pd.to_numeric(df.loc[mask_g, 'serving_size'], errors='coerce')
        
        mask_oz = df['serving_size_unit'].astype(str).str.lower().isin(['oz', 'fl oz'])
        df.loc[mask_oz, 'Total Weight (g)'] = pd.to_numeric(df.loc[mask_oz, 'serving_size'], errors='coerce') * 28.35

    df = standardize_column_names(df)
    clean_nutrient_columns(df)
    df = sort_columns(df)
    return df
