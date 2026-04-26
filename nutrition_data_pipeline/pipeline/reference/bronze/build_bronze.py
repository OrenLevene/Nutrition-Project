"""
Build BRONZE tier for the Reference Stream.
Extracts raw FooDB and CoFID data and saves to data/reference/bronze/.
"""
import pandas as pd
import numpy as np
import warnings
from pathlib import Path
import sys

warnings.filterwarnings('ignore')

# Paths — script lives in pipeline/reference/bronze/
DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_RAW = DATA_PIPELINE_DIR / 'data' / 'reference' / 'raw'
DATA_BRONZE = DATA_PIPELINE_DIR / 'data' / 'reference' / 'bronze'

FOODB_DIR = DATA_RAW / 'foodb' / 'foodb_2020_04_07_csv'
COFID_FILE = DATA_RAW / 'UK_CoFID_2021.xlsx'

# Import utils
sys.path.insert(0, str(DATA_PIPELINE_DIR / 'pipeline' / 'utils'))
from reference_cleaning import clean_nutrient_columns


def load_foodb():
    """Load and pivot FooDB data into food-per-row, nutrient-per-column format."""
    print("Loading FooDB...")
    
    foods = pd.read_csv(FOODB_DIR / 'Food.csv')
    # Keep all useful metadata — drop only picture/timestamp/internal columns
    drop_cols = [c for c in foods.columns if c.startswith('picture_') or c in [
        'created_at', 'updated_at', 'creator_id', 'updater_id', 
        'legacy_id', 'export_to_afcdb', 'export_to_foodb',
        'itis_id', 'ncbi_taxonomy_id', 'public_id',
    ]]
    foods = foods.drop(columns=drop_cols, errors='ignore')
    foods = foods.rename(columns={'id': 'food_id', 'name': 'description'})
    
    nutrients = pd.read_csv(FOODB_DIR / 'Nutrient.csv')
    nutrients = nutrients[['id', 'public_id', 'name']]
    nutrients = nutrients.rename(columns={'id': 'source_id', 'name': 'nutrient_name'})
    
    print("  Loading Content.csv (this may take a minute)...")
    content = pd.read_csv(FOODB_DIR / 'Content.csv', low_memory=False)
    content = content[content['source_type'] == 'Nutrient']
    content = content[['source_id', 'food_id', 'standard_content', 'orig_unit', 'preparation_type']]
    
    content['prep_priority'] = content['preparation_type'].apply(
        lambda x: 0 if x == 'raw' else (1 if pd.isna(x) else 2)
    )
    content = content.sort_values('prep_priority').drop_duplicates(
        subset=['food_id', 'source_id'], keep='first'
    )
    
    content = content.merge(nutrients, on='source_id', how='left')
    content['column_name'] = content.apply(
        lambda row: f"{row['nutrient_name']} ({row['orig_unit'].upper().replace('/', '_')})" 
        if pd.notna(row['orig_unit']) else row['nutrient_name'], 
        axis=1
    )
    
    print("  Pivoting to wide format...")
    foodb_pivot = content.pivot_table(
        index='food_id', columns='column_name', values='standard_content', aggfunc='mean'
    ).reset_index()
    
    foodb_df = foods.merge(foodb_pivot, on='food_id', how='inner')
    
    # FooDB BUG FIX: FooDB's raw data labels Energy as 'kcal' but values are actually kJ.
    # Verified against 10 known foods: FooDB values / 4.184 = correct kcal (e.g. Banana: 371/4.184 = 88.7 kcal).
    # Fix the column name so downstream standardization handles it correctly.
    if 'Energy (KCAL_100G)' in foodb_df.columns:
        foodb_df = foodb_df.rename(columns={'Energy (KCAL_100G)': 'Energy (KJ_100G)'})
    
    foodb_df['data_source'] = 'FooDB'
    foodb_df['food_id'] = foodb_df['food_id'].astype(str)
    
    return foodb_df


def load_cofid():
    """Load UK CoFID and reshape into standard format."""
    print("\nLoading UK CoFID...")
    
    cofid_column_map = {
        'Protein': 'Protein', 'Fat': 'Fat', 'Carbohydrate': 'Carbohydrate',
        'kcal': 'kcal', 'Na': 'Na', 'Ca': 'Ca', 'Fe': 'Fe', 'Vitamin C': 'Vit C'
    }
    
    sheets_to_load = [
        ('1.3 Proximates', 2), ('1.4 Inorganics', 2), ('1.5 Vitamins', 2),
        ('1.6 Vitamin Fractions', 2), ('1.8 (SFA per 100gFood)', 2),
        ('1.10 (MUFA per 100gFood)', 2), ('1.12 (PUFA per 100gFood)', 2),
        ('1.13 Phytosterols', 2), ('1.14 Organic Acids', 2),
    ]
    
    dfs = []
    for sheet_name, header_row in sheets_to_load:
        try:
            df = pd.read_excel(COFID_FILE, sheet_name=sheet_name, header=header_row)
            if 'Unnamed: 0' in df.columns: df = df.rename(columns={'Unnamed: 0': 'food_code'})
            if 'Unnamed: 1' in df.columns: df = df.rename(columns={'Unnamed: 1': 'description'})
            dfs.append((sheet_name, df))
        except Exception as e:
            print(f"  Warning: Could not load {sheet_name}: {e}")
            
    cofid_df = dfs[0][1][['food_code', 'description']].copy()
    
    for sheet_name, df in dfs:
        meta_cols = ['food_code', 'description', 'Unnamed: 2', 'Unnamed: 3', 'Unnamed: 4', 'Unnamed: 5', 'Unnamed: 6']
        nutrient_cols = [c for c in df.columns if c not in meta_cols and not c.startswith('Unnamed')]
        if nutrient_cols:
            df_subset = df[['food_code', 'description'] + nutrient_cols].copy()
            cofid_df = cofid_df.merge(df_subset, on=['food_code', 'description'], how='outer')
            
    cofid_df = cofid_df.drop_duplicates(subset=['food_code', 'description'])
    cofid_df['data_source'] = 'UK_CoFID'
    cofid_df['food_id'] = 'cofid_' + cofid_df['food_code'].astype(str)
    
    # CoFID from Excel requires parsing
    cofid_df = clean_nutrient_columns(cofid_df)
    return cofid_df


if __name__ == '__main__':
    print("=" * 60)
    print("STAGE: BRONZE — Raw source extraction")
    print("=" * 60)
    
    DATA_BRONZE.mkdir(parents=True, exist_ok=True)
    
    foodb_df = load_foodb()
    bronze_foodb_path = DATA_BRONZE / 'foodb.csv'
    foodb_df.to_csv(bronze_foodb_path, index=False)
    print(f"  Saved bronze/foodb.csv: {len(foodb_df)} foods, {len(foodb_df.columns)} cols")
    
    cofid_df = load_cofid()
    bronze_cofid_path = DATA_BRONZE / 'cofid.csv'
    cofid_df.to_csv(bronze_cofid_path, index=False)
    print(f"  Saved bronze/cofid.csv: {len(cofid_df)} foods, {len(cofid_df.columns)} cols")
    
    # Note: USDA Bronze is expected to already be built/placed in bronze/usda.csv
