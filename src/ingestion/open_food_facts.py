"""
Open Food Facts UK Product Ingestion

Downloads and filters UK products from Open Food Facts with strict quality criteria:
- Country: United Kingdom
- Completeness >= 70%
- Required nutrients: energy, protein, carbs, fat
- At least 5 additional nutrients present

Uses the static CSV export for efficiency.
"""
import os
import gzip
import requests
import pandas as pd
from typing import List, Dict, Optional
from pathlib import Path


# Configuration
PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
DATA_RAW = PROJECT_DIR / 'data' / 'raw'
DATA_PROCESSED = PROJECT_DIR / 'data' / 'processed'

# Open Food Facts static export (gzipped CSV)
OFF_CSV_URL = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"
OFF_CACHE_FILE = DATA_RAW / 'off_products.csv.gz'


# Nutrient mapping: OFF column name -> our internal name
# OFF uses nutrient_100g format
NUTRIENT_MAPPING = {
    # Core macros
    'energy-kcal_100g': 'calories',
    'proteins_100g': 'protein',
    'carbohydrates_100g': 'carbohydrate',
    'fat_100g': 'fat',
    
    # Additional macros
    'fiber_100g': 'Fiber (g)',
    'sugars_100g': 'Sugar (g)',
    'saturated-fat_100g': 'Saturated Fat (g)',
    'monounsaturated-fat_100g': 'Monounsaturated Fat (g)',
    'polyunsaturated-fat_100g': 'Polyunsaturated Fat (g)',
    'cholesterol_100g': 'Cholesterol (mg)',
    
    # Sodium (OFF is in g, we want mg)
    'sodium_100g': 'Sodium (mg)',  # Will multiply by 1000
    'salt_100g': 'Salt (g)',
    
    # Vitamins
    'vitamin-a_100g': 'Vitamin A (mcg RAE)',
    'vitamin-c_100g': 'Vitamin C (mg)',
    'vitamin-d_100g': 'Vitamin D (mcg)',
    'vitamin-e_100g': 'Vitamin E (mg)',
    'vitamin-k_100g': 'Vitamin K (mcg)',
    'vitamin-b1_100g': 'Thiamin (B1) (mg)',
    'vitamin-b2_100g': 'Riboflavin (B2) (mg)',
    'vitamin-pp_100g': 'Niacin (B3) (mg NE)',  # PP = niacin
    'vitamin-b6_100g': 'Vitamin B6 (mg)',
    'vitamin-b9_100g': 'Folate (mcg DFE)',
    'vitamin-b12_100g': 'Vitamin B12 (mcg)',
    
    # Minerals
    'calcium_100g': 'Calcium (mg)',
    'iron_100g': 'Iron (mg)',
    'magnesium_100g': 'Magnesium (mg)',
    'phosphorus_100g': 'Phosphorus (mg)',
    'potassium_100g': 'Potassium (mg)',
    'zinc_100g': 'Zinc (mg)',
    'copper_100g': 'Copper (mg)',
    'manganese_100g': 'Manganese (mg)',
    'selenium_100g': 'Selenium (mcg)',
    'iodine_100g': 'Iodine (mcg)',
}

# Required core nutrients (must be present)
REQUIRED_NUTRIENTS = ['energy-kcal_100g', 'proteins_100g', 'carbohydrates_100g', 'fat_100g']

# Minimum additional nutrients for quality
MIN_ADDITIONAL_NUTRIENTS = 3


class OpenFoodFactsClient:
    """Download and filter UK products from Open Food Facts."""
    
    def __init__(self, cache_file: Path = OFF_CACHE_FILE):
        self.cache_file = cache_file
        
    def download_csv(self, force_download: bool = False) -> Path:
        """
        Download the OFF CSV export if not cached.
        
        Note: This file is ~2GB compressed, ~7GB uncompressed.
        We'll stream and filter to avoid memory issues.
        """
        if self.cache_file.exists() and not force_download:
            print(f"Using cached file: {self.cache_file}")
            return self.cache_file
        
        print(f"Downloading Open Food Facts export from {OFF_CSV_URL}")
        print("This may take a while (~2GB)...")
        
        # Ensure directory exists
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Stream download
        response = requests.get(OFF_CSV_URL, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(self.cache_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    pct = (downloaded / total_size) * 100
                    print(f"\rDownloading: {pct:.1f}%", end='', flush=True)
        
        print(f"\nSaved to {self.cache_file}")
        return self.cache_file
    
    def load_uk_products(self, max_products: int = None) -> pd.DataFrame:
        """
        Load UK products from the CSV export.
        
        Filters to UK only during loading to reduce memory usage.
        """
        print("Loading UK products from Open Food Facts...")
        
        # Columns we need
        columns_to_load = [
            'code', 'product_name', 'brands', 'categories_en', 
            'countries_en', 'completeness',
        ] + list(NUTRIENT_MAPPING.keys())
        
        # Read in chunks, filter to UK
        chunks = []
        chunk_size = 50000
        rows_loaded = 0
        
        with gzip.open(self.cache_file, 'rt', encoding='utf-8', errors='replace') as f:
            reader = pd.read_csv(f, sep='\t', chunksize=chunk_size, usecols=lambda x: x in columns_to_load,
                                low_memory=False, on_bad_lines='skip')
            
            for chunk in reader:
                # Filter to UK products
                if 'countries_en' in chunk.columns:
                    uk_mask = chunk['countries_en'].str.contains('United Kingdom', case=False, na=False)
                    uk_chunk = chunk[uk_mask]
                else:
                    uk_chunk = chunk
                
                if len(uk_chunk) > 0:
                    chunks.append(uk_chunk)
                    rows_loaded += len(uk_chunk)
                    print(f"\rLoaded {rows_loaded} UK products...", end='', flush=True)
                
                if max_products and rows_loaded >= max_products:
                    break
        
        print(f"\nTotal UK products found: {rows_loaded}")
        
        if chunks:
            df = pd.concat(chunks, ignore_index=True)
            return df
        else:
            return pd.DataFrame()
    
    def filter_by_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply strict quality filters.
        
        Criteria:
        1. Completeness >= 0.7 (70%)
        2. All required nutrients present
        3. At least MIN_ADDITIONAL_NUTRIENTS extra nutrients
        """
        initial_count = len(df)
        print(f"Applying quality filters to {initial_count} products...")
        
        # 1. Completeness filter
        if 'completeness' in df.columns:
            df['completeness'] = pd.to_numeric(df['completeness'], errors='coerce')
            df = df[df['completeness'] > 0.5]
            print(f"  After completeness filter: {len(df)}")
        
        # 2. Required nutrients filter
        for nutrient in REQUIRED_NUTRIENTS:
            if nutrient in df.columns:
                df[nutrient] = pd.to_numeric(df[nutrient], errors='coerce')
                df = df[df[nutrient].notna() & (df[nutrient] >= 0)]
        print(f"  After required nutrients filter: {len(df)}")
        
        # 3. Additional nutrients count filter
        additional_cols = [col for col in NUTRIENT_MAPPING.keys() 
                         if col not in REQUIRED_NUTRIENTS and col in df.columns]
        
        if additional_cols:
            additional_count = df[additional_cols].notna().sum(axis=1)
            df = df[additional_count >= MIN_ADDITIONAL_NUTRIENTS]
        print(f"  After additional nutrients filter: {len(df)}")
        
        # 4. Must have a product name
        if 'product_name' in df.columns:
            df = df[df['product_name'].notna() & (df['product_name'].str.len() > 0)]
        print(f"  After name filter: {len(df)}")
        
        print(f"Quality filtering: {initial_count} -> {len(df)} products")
        return df
    
    def convert_to_standard_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert OFF format to our standard food database format.
        """
        result = pd.DataFrame()
        
        # Food identification
        result['food_id'] = 'off_' + df['code'].astype(str)
        result['description'] = df['product_name'].fillna('') + ' - ' + df['brands'].fillna('')
        result['description'] = result['description'].str.strip(' -')
        result['data_source'] = 'OpenFoodFacts'
        
        # Categories
        if 'categories_en' in df.columns:
            result['food_group'] = df['categories_en'].str.split(',').str[0]
        else:
            result['food_group'] = 'Unknown'
        
        # Map nutrients
        for off_col, our_col in NUTRIENT_MAPPING.items():
            if off_col in df.columns:
                result[our_col] = pd.to_numeric(df[off_col], errors='coerce')
                
                # Special handling: sodium g -> mg
                if off_col == 'sodium_100g':
                    result[our_col] = result[our_col] * 1000
        
        return result
    
    def save_to_parquet(self, df: pd.DataFrame, output_path: Path) -> None:
        """Save processed foods to parquet file."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        print(f"Saved {len(df)} products to {output_path}")
    
    def run_full_pipeline(self, output_path: Path = None, max_products: int = None) -> pd.DataFrame:
        """
        Run the full ingestion pipeline:
        1. Download CSV (if needed)
        2. Load UK products
        3. Filter by quality
        4. Convert to standard format
        5. Save to parquet
        """
        if output_path is None:
            output_path = DATA_PROCESSED / 'OFF_UK_products.parquet'
        
        # Step 1: Download
        self.download_csv()
        
        # Step 2: Load UK products
        df = self.load_uk_products(max_products=max_products)
        if len(df) == 0:
            print("No UK products found!")
            return pd.DataFrame()
        
        # Step 3: Quality filter
        df = self.filter_by_quality(df)
        if len(df) == 0:
            print("No products passed quality filters!")
            return pd.DataFrame()
        
        # Step 4: Convert format
        df = self.convert_to_standard_format(df)
        
        # Step 5: Save
        self.save_to_parquet(df, output_path)
        
        return df


def main():
    """Run the Open Food Facts ingestion pipeline."""
    print("=" * 60)
    print("Open Food Facts UK Product Ingestion")
    print("=" * 60)
    
    client = OpenFoodFactsClient()
    df = client.run_full_pipeline()
    
    if len(df) > 0:
        print("\n" + "=" * 60)
        print("Summary")
        print("=" * 60)
        print(f"Total products: {len(df)}")
        print(f"Columns: {len(df.columns)}")
        
        # Sample products
        print("\nSample products:")
        for _, row in df.head(10).iterrows():
            print(f"  - {row['description'][:60]}")
        
        # Nutrient coverage
        print("\nNutrient coverage:")
        nutrient_cols = [col for col in df.columns if col not in 
                        ['food_id', 'description', 'data_source', 'food_group']]
        for col in nutrient_cols[:10]:
            coverage = (df[col].notna().sum() / len(df)) * 100
            print(f"  {col}: {coverage:.1f}%")


if __name__ == '__main__':
    main()
