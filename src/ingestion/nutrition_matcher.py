"""
Nutrition-based Product Matcher

Matches OFF products to canonical foods using DUAL-GATE approach:
1. Name similarity (fuzzy match ≥70%) - ensures same type of food
2. Nutritional validation (≤10% deviation) - ensures not a different variant

This prevents:
- "Peanut butter" matching "Butter" (name fails)
- "Tomato" matching "Tomato sauce" (nutrition fails)
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
from rapidfuzz import fuzz
from pathlib import Path


class NutritionMatcher:
    """
    Matches OFF products to canonical foods using name + nutrition validation.
    """
    
    # Nutrients to compare (per 100g)
    MATCH_NUTRIENTS = ['calories', 'protein', 'fat', 'carbohydrate']
    
    # OFF column name mapping
    OFF_NUTRIENT_COLS = {
        'calories': 'energy-kcal_100g',
        'protein': 'proteins_100g',
        'fat': 'fat_100g',
        'carbohydrate': 'carbohydrates_100g'
    }
    
    def __init__(
        self,
        name_threshold: float = 70.0,
        nutrition_tolerance: float = 0.10
    ):
        """
        Args:
            name_threshold: Minimum fuzzy match score (0-100)
            nutrition_tolerance: Max allowed deviation per nutrient (0.10 = 10%)
        """
        self.name_threshold = name_threshold
        self.nutrition_tolerance = nutrition_tolerance
    
    def match_products(
        self,
        off_df: pd.DataFrame,
        canonical_foods: List,
        verbose: bool = True
    ) -> pd.DataFrame:
        """
        Match OFF products to canonical foods using dual-gate approach.
        
        Args:
            off_df: DataFrame with OFF products (must have nutrition columns)
            canonical_foods: List of FoodItem objects from canonical database
            
        Returns:
            DataFrame with validated matches
        """
        # Build canonical lookup with nutrition
        canonical_data = []
        for food in canonical_foods:
            canonical_data.append({
                'id': food.id,
                'name': food.name,
                'name_lower': food.name.lower(),
                'calories': food.calories,
                'protein': food.nutrients.get('protein', 0),
                'fat': food.nutrients.get('fat', 0),
                'carbohydrate': food.nutrients.get('carbohydrate', 0)
            })
        
        canonical_df = pd.DataFrame(canonical_data)
        
        # Filter OFF to products with complete nutrition
        off_df = self._filter_complete_nutrition(off_df)
        if verbose:
            print(f"OFF products with complete nutrition: {len(off_df)}")
        
        matches = []
        stats = {'name_pass': 0, 'nutrition_pass': 0, 'both_pass': 0}
        
        for idx, off_row in off_df.iterrows():
            off_name = str(off_row.get('product_name', '')).lower()
            if not off_name or off_name == 'nan':
                continue
            
            # Get OFF nutrition
            off_nutrition = {
                'calories': off_row.get(self.OFF_NUTRIENT_COLS['calories'], 0),
                'protein': off_row.get(self.OFF_NUTRIENT_COLS['protein'], 0),
                'fat': off_row.get(self.OFF_NUTRIENT_COLS['fat'], 0),
                'carbohydrate': off_row.get(self.OFF_NUTRIENT_COLS['carbohydrate'], 0)
            }
            
            # Find best match
            best_match = self._find_best_match(
                off_name, off_nutrition, canonical_df, stats
            )
            
            if best_match:
                matches.append({
                    'off_id': off_row.get('code', idx),
                    'off_name': off_row.get('product_name', ''),
                    'off_brand': off_row.get('brands', ''),
                    'canonical_id': best_match['canonical_id'],
                    'canonical_name': best_match['canonical_name'],
                    'name_score': best_match['name_score'],
                    'nutrition_deviation': best_match['nutrition_deviation'],
                    'off_calories': off_nutrition['calories'],
                    'canonical_calories': best_match['canonical_calories']
                })
        
        if verbose:
            print(f"\nMatching Statistics:")
            print(f"  Name matches (≥{self.name_threshold}%): {stats['name_pass']}")
            print(f"  Nutrition validated (≤{self.nutrition_tolerance*100}%): {stats['both_pass']}")
            print(f"  Final matches: {len(matches)}")
        
        return pd.DataFrame(matches)
    
    def _filter_complete_nutrition(self, df: pd.DataFrame) -> pd.DataFrame:
        """Keep only rows with all required nutrition columns."""
        required = list(self.OFF_NUTRIENT_COLS.values())
        return df.dropna(subset=required)
    
    def _find_best_match(
        self,
        off_name: str,
        off_nutrition: Dict[str, float],
        canonical_df: pd.DataFrame,
        stats: Dict[str, int]
    ) -> Optional[Dict]:
        """
        Find best canonical match using dual-gate approach.
        
        Returns match dict if both gates pass, None otherwise.
        """
        best_match = None
        best_combined_score = 0
        
        for _, can_row in canonical_df.iterrows():
            # Gate 1: Name similarity
            name_score = fuzz.token_set_ratio(off_name, can_row['name_lower'])
            
            if name_score < self.name_threshold:
                continue
            
            stats['name_pass'] = stats.get('name_pass', 0) + 1
            
            # Gate 2: Nutritional similarity
            can_nutrition = {
                'calories': can_row['calories'],
                'protein': can_row['protein'],
                'fat': can_row['fat'],
                'carbohydrate': can_row['carbohydrate']
            }
            
            deviation = self._calculate_nutrition_deviation(off_nutrition, can_nutrition)
            
            if deviation <= self.nutrition_tolerance:
                stats['both_pass'] = stats.get('both_pass', 0) + 1
                
                # Combined score: prioritize name match, penalize nutrition deviation
                combined_score = name_score * (1 - deviation)
                
                if combined_score > best_combined_score:
                    best_combined_score = combined_score
                    best_match = {
                        'canonical_id': can_row['id'],
                        'canonical_name': can_row['name'],
                        'name_score': name_score,
                        'nutrition_deviation': deviation,
                        'canonical_calories': can_row['calories']
                    }
        
        return best_match
    
    def _calculate_nutrition_deviation(
        self,
        off_nutrition: Dict[str, float],
        can_nutrition: Dict[str, float]
    ) -> float:
        """
        Calculate max deviation across all nutrients.
        
        Returns value in range [0, 1+] where 0 = perfect match.
        """
        max_deviation = 0
        
        for nutrient in self.MATCH_NUTRIENTS:
            off_val = off_nutrition.get(nutrient, 0)
            can_val = can_nutrition.get(nutrient, 0)
            
            # Skip if canonical has 0 (can't calculate percentage)
            if can_val == 0:
                if off_val > 5:  # Allow small absolute difference
                    return float('inf')
                continue
            
            deviation = abs(off_val - can_val) / can_val
            max_deviation = max(max_deviation, deviation)
        
        return max_deviation


def main():
    """Test the nutrition matcher."""
    from src.calculator.db_interface import FoodDatabase
    
    print("=== Nutrition-Based Product Matching ===\n")
    
    # Load canonical foods
    print("1. Loading canonical foods...")
    db = FoodDatabase()
    db.load_from_parquet("data/processed/real_food_nutrition.parquet")
    canonical_foods = db.get_all_foods()
    print(f"   Loaded {len(canonical_foods)} canonical foods\n")
    
    # Load OFF with nutrition
    print("2. Loading OFF products with nutrition...")
    try:
        off_df = pd.read_csv(
            'data/raw/off_products.csv.gz',
            sep='\t',
            usecols=[
                'code', 'product_name', 'brands', 'countries_tags',
                'energy-kcal_100g', 'proteins_100g', 'fat_100g', 'carbohydrates_100g'
            ],
            on_bad_lines='skip',
            nrows=100000  # Limit for testing
        )
        # Filter to UK
        off_df = off_df[off_df['countries_tags'].str.contains('united-kingdom', na=False, case=False)]
        print(f"   Loaded {len(off_df)} UK products\n")
    except Exception as e:
        print(f"   Error: {e}")
        return
    
    # Run matching
    print("3. Running dual-gate matching...")
    matcher = NutritionMatcher(name_threshold=70.0, nutrition_tolerance=0.10)
    matches_df = matcher.match_products(off_df, canonical_foods)
    
    # Show sample matches
    print("\n4. Sample matches:")
    for _, row in matches_df.head(10).iterrows():
        print(f"   {row['off_name'][:35]:<35} -> {row['canonical_name'][:35]} "
              f"(name:{row['name_score']:.0f}%, nutr:{row['nutrition_deviation']*100:.1f}%)")
    
    # Save
    output_path = "data/processed/store_products_mapping.parquet"
    matches_df.to_parquet(output_path, index=False)
    print(f"\n5. Saved {len(matches_df)} matches to {output_path}")


if __name__ == "__main__":
    main()
