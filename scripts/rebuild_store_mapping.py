"""
Rebuild Store Products Mapping using improved fuzzy matching.

This script:
1. Loads canonical foods from FooDB/CoFID
2. Loads OFF UK products 
3. Uses new fuzzy_match_canonical() with manual mappings
4. Saves to store_products_mapping.parquet
"""
import sys
import os
sys.path.append(os.getcwd())

import pandas as pd
from src.calculator.db_interface import FoodDatabase
from src.ingestion.product_matcher import ProductMatcher


def main():
    print("=== Rebuilding Store Products Mapping ===\n")
    
    # 1. Load canonical foods
    print("1. Loading canonical foods...")
    db = FoodDatabase()
    db.load_from_parquet("data/processed/real_food_nutrition.parquet")
    canonical_foods = db.get_all_foods()
    print(f"   Loaded {len(canonical_foods)} canonical foods\n")
    
    # 2. Load OFF products (if exists)
    off_path = "data/processed/off_uk_products.parquet"
    if not os.path.exists(off_path):
        print(f"   ERROR: {off_path} not found!")
        print("   Run: py -m src.ingestion.open_food_facts to download first")
        return
    
    print("2. Loading OFF UK products...")
    off_df = pd.read_parquet(off_path)
    print(f"   Loaded {len(off_df)} OFF products\n")
    
    # 3. Run improved matching
    print("3. Running fuzzy matching...")
    matcher = ProductMatcher()
    matches_df = matcher.fuzzy_match_canonical(
        off_df, 
        canonical_foods,
        fuzzy_threshold=70.0
    )
    
    # 4. Show statistics
    print(f"\n4. Match Statistics:")
    print(f"   Total matches: {len(matches_df)}")
    if 'match_type' in matches_df.columns:
        type_counts = matches_df['match_type'].value_counts()
        for match_type, count in type_counts.items():
            print(f"   - {match_type}: {count}")
    
    # 5. Sample high-quality matches
    print("\n5. Sample matches (score >= 90):")
    high_quality = matches_df[matches_df['match_score'] >= 90].head(10)
    for _, row in high_quality.iterrows():
        print(f"   {row['off_name'][:40]:<40} -> {row['canonical_name'][:40]}")
    
    # 6. Save
    output_path = "data/processed/store_products_mapping.parquet"
    matches_df.to_parquet(output_path, index=False)
    print(f"\n6. Saved {len(matches_df)} matches to {output_path}")
    
    # 7. Coverage analysis
    print("\n7. Coverage Analysis:")
    unique_canonical = matches_df['canonical_id'].nunique()
    coverage = unique_canonical / len(canonical_foods) * 100
    print(f"   Unique canonical foods with matches: {unique_canonical}")
    print(f"   Coverage: {coverage:.1f}%")


if __name__ == "__main__":
    main()
