# Build SILVER tier for the Reference Stream.
# Reads Bronze data, applies standard cleaning logic, validates physics, and saves to data/reference/silver/.
# NOTE: FooDB was dropped — see physics_validation_results.md for rationale.

import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Paths
DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_BRONZE = DATA_PIPELINE_DIR / 'data' / 'reference' / 'bronze'
DATA_SILVER = DATA_PIPELINE_DIR / 'data' / 'reference' / 'silver'
DATA_FLAGGED = DATA_PIPELINE_DIR / 'data' / 'reference' / 'flagged'

# Import utils
from clean_cofid import process_silver_cofid
from clean_usda import process_silver_usda

sys.path.insert(0, str(DATA_PIPELINE_DIR / 'pipeline' / 'utils'))
from reference_cleaning import validate_physics

def run_pipeline():
    print("=" * 60)
    print("STAGE: SILVER — Standardise, classify, validate")
    print("=" * 60)
    
    DATA_SILVER.mkdir(parents=True, exist_ok=True)
    DATA_FLAGGED.mkdir(parents=True, exist_ok=True)
    
    sources = [
        ('CoFID', 'cofid.csv', process_silver_cofid),
        ('USDA',  'usda.csv',  process_silver_usda),
    ]
    
    for name, filename, processor in sources:
        try:
            print(f"\nProcessing {name} Silver...")
            df = processor(pd.read_csv(DATA_BRONZE / filename))
            
            # Run physics validation BEFORE stripping Alcohol — so alcoholic drinks
            # don't get wrongly flagged (Alcohol calories contribute ~7 kcal/g).
            clean_df, flagged_df = validate_physics(df, source_name=name)
            
            # Drop Alcohol (g) after validation — not a shopping/meal-planning nutrient
            for frame in [clean_df, flagged_df]:
                if 'Alcohol (g)' in frame.columns:
                    frame.drop(columns=['Alcohol (g)'], inplace=True)
            
            # Save clean data
            out_path = DATA_SILVER / filename
            clean_df.to_csv(out_path, index=False)
            print(f"  Saved {out_path} ({len(clean_df)} foods)")
            
            # Save flagged data for review
            if len(flagged_df) > 0:
                flag_path = DATA_FLAGGED / f'{name.lower()}_flagged.csv'
                flagged_df.to_csv(flag_path, index=False)
                print(f"  Flagged {len(flagged_df)} rows -> {flag_path}")
                
        except FileNotFoundError:
            print(f"  Warning: {filename} not found in Bronze!")

if __name__ == '__main__':
    run_pipeline()
