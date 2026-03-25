# Build SILVER tier for the Reference Stream.
# Reads Bronze data, applies standard cleaning logic, and saves to data/reference/silver/.

import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Paths
DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_BRONZE = DATA_PIPELINE_DIR / 'data' / 'reference' / 'bronze'
DATA_SILVER = DATA_PIPELINE_DIR / 'data' / 'reference' / 'silver'

# Import utils
from clean_foodb import process_silver_foodb
from clean_cofid import process_silver_cofid
from clean_usda import process_silver_usda

def run_pipeline():
    print("=" * 60)
    print("STAGE: SILVER — Standardise and clean uniformly")
    print("=" * 60)
    
    DATA_SILVER.mkdir(parents=True, exist_ok=True)
    
    # FooDB
    try:
        print("\nProcessing FooDB Silver...")
        foodb_df = process_silver_foodb(pd.read_csv(DATA_BRONZE / 'foodb.csv'))
        out_path = DATA_SILVER / 'foodb.csv'
        foodb_df.to_csv(out_path, index=False)
        print(f"  Saved {out_path} ({len(foodb_df)} foods)")
    except FileNotFoundError:
        print("  Warning: foodb.csv not found in Bronze!")
        
    # CoFID
    try:
        print("\nProcessing CoFID Silver...")
        cofid_df = process_silver_cofid(pd.read_csv(DATA_BRONZE / 'cofid.csv'))
        out_path = DATA_SILVER / 'cofid.csv'
        cofid_df.to_csv(out_path, index=False)
        print(f"  Saved {out_path} ({len(cofid_df)} foods)")
    except FileNotFoundError:
        print("  Warning: cofid.csv not found in Bronze!")
        
    # USDA
    try:
        print("\nProcessing USDA Silver...")
        usda_df = process_silver_usda(pd.read_csv(DATA_BRONZE / 'usda.csv'))
        out_path = DATA_SILVER / 'usda.csv'
        usda_df.to_csv(out_path, index=False)
        print(f"  Saved {out_path} ({len(usda_df)} foods)")
    except FileNotFoundError:
        print("  Warning: usda.csv not found in Bronze!")

if __name__ == '__main__':
    run_pipeline()
