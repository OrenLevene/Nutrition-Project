"""
Script to visualize the food probability distributions used in the Genetic Algorithm.
Shows how foods are stratified into nutrient buckets and their relative scores.
"""
import sys
import os
# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.calculator.db_interface import FoodDatabase
from src.optimizer.genetic_optimizer import GeneticMealOptimizer
from src.utils.food_filter import TOGGLEABLE_EXCLUSIONS

def print_table(data, headers):
    # Simple table printer
    col_widths = [max(len(str(x)) for x in col) for col in zip(*([headers] + data))]
    col_widths = [max(w, len(h)) for w, h in zip(col_widths, headers)]
    
    header_row = " | ".join(f"{h:<{w}}" for h, w in zip(headers, col_widths))
    print("-" * len(header_row))
    print(header_row)
    print("-" * len(header_row))
    
    for row in data:
        print(" | ".join(f"{str(cell):<{w}}" for cell, w in zip(row, col_widths)))
    print()

def main():
    print("Loading database...")
    db = FoodDatabase()
    parquet_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data/processed/real_food_nutrition.parquet'))
    if os.path.exists(parquet_path):
        db.load_from_parquet(parquet_path)
    else:
        print("Real database not found, using mock.")
        db.load_mock_data()

    all_foods = db.get_all_foods()
    print(f"Total foods loaded: {len(all_foods)}")

    # Initialize optimizer (this handles exclusions)
    print("Initializing optimizer and calculating scores...")
    optimizer = GeneticMealOptimizer(
        foods=all_foods,
        population_size=10, 
        generations=1
    )
    
    # Set dummy nutrient targets (normally set in optimize())
    # This is required for _calculate_global_rarity
    optimizer.nutrient_targets = {
        'Protein': 150,
        'Vitamin D (D2 + D3)': 10,
        'Iron, Fe': 15,
        'Calcium, Ca': 1000,
        # Add a few others to ensure keys exist for rarity calculation
    }
    # Actually, calculate_global_rarity uses self.foods to find what nutrients exist
    # AND it iterates over self.nutrient_targets IF IT EXISTS, otherwise it uses all nutrients
    # Let's look at the code for _calculate_global_rarity... 
    # It iterates self.nutrient_targets.items(). So we must provide the nutrients we care about.
    # To see global rarity for ALL nutrients, we should populate this with all nutrients found in DB.
    
    all_nutrients = set()
    for f in all_foods:
        all_nutrients.update(f.nutrients.keys())
        
    optimizer.nutrient_targets = {n: {'min': 1} for n in all_nutrients}

    # Manually trigger the scoring logic normally done in _init_population -> _stratified_nutrient_seed
    # We need to access the internal methods to show the data
    
    # 1. Calculate Global Rarity
    print("\n1. Calculating Nutrient Rarity...")
    optimizer._calculate_global_rarity()
    
    rarity_data = []
    # FIX: Attribute is global_rarity, not nutrient_rarity
    for nutrient, rarity in sorted(optimizer.global_rarity.items(), key=lambda x: x[1], reverse=True):
        if rarity > 0.5: # Show mostly the rare ones
            rarity_data.append([nutrient, f"{rarity:.2f}"])
            
    print_table(rarity_data, ['Nutrient', 'Rarity Score'])

    # 2. Assign to Buckets
    print("\n2. Assigning Foods to Nutrient Buckets...")
    optimizer._assign_to_nutrient_buckets()
    
    bucket_counts = []
    for nutrient, foods in sorted(optimizer.nutrient_buckets.items(), key=lambda x: len(x[1]), reverse=True):
        bucket_counts.append([nutrient, len(foods)])
        
    print_table(bucket_counts, ['Bucket (Primary Nutrient)', 'Count'])
    
    # 3. Show Top Foods per Bucket
    print("\n3. Top Foods by Probability in Key Buckets:")
    
    # Focus on a few interesting buckets
    target_buckets = [b for b, _ in rarity_data[:5]] # Top 5 rarest nutrients
    
    for bucket in target_buckets:
        if bucket not in optimizer.nutrient_buckets:
            continue
            
        foods_in_bucket = optimizer.nutrient_buckets[bucket]
        if not foods_in_bucket:
            continue
            
        print(f"\nBucket: {bucket} (Rarity: {optimizer.global_rarity.get(bucket, 0):.2f})")
        print(f"Total foods: {len(foods_in_bucket)}")
        
        # Get the scores (stored in food_secondary_scores dict)
        # Note: _assign_to_nutrient_buckets stores FoodItem objects in the list
        
        # Sort by secondary score descending
        sorted_foods = sorted(foods_in_bucket, key=lambda f: optimizer.food_secondary_scores.get(f.id, 0), reverse=True)
        
        if len(foods_in_bucket) > 5:
            # Show softmax distribution for this bucket
            import math
            print(f"\nSoftmax Sampling Probabilities (Temperature=0.3):")
            
            # extract scores
            scores = [optimizer.food_secondary_scores.get(f.id, 0) for f in sorted_foods]
            
            # approximate softmax math from optimizer
            temperature = 0.3
            max_score = max(scores)
            exp_scores = [math.exp((s - max_score) / temperature) for s in scores]
            sum_exp = sum(exp_scores)
            probs = [e / sum_exp for e in exp_scores]
            
            # Show curve data
            curve_data = []
            cumulative = 0
            for i in range(min(15, len(sorted_foods))):
                food = sorted_foods[i]
                prob = probs[i] * 100
                cumulative += prob
                
                # Debug filter for suspicious items
                debug_info = ""
                if "egg" in food.name.lower() and "dried" in food.name.lower():
                    from src.utils.food_filter import is_excluded_food
                    is_excluded = is_excluded_food(food)
                    debug_info = f"[Excluded? {is_excluded}]"
                    
                curve_data.append([
                    i+1, 
                    food.name[:60] + debug_info, # Show longer name
                    f"{scores[i]:.1f}", 
                    f"{prob:.1f}%", 
                    f"{cumulative:.1f}%"
                ])
            
            print_table(curve_data, ['Rank', 'Food', 'Score', 'Prob', 'Cumul%'])
            
        else:
            top_data = []
            for i, food_item in enumerate(sorted_foods[:10]):
                score = optimizer.food_secondary_scores.get(food_item.id, 0)
                # Show name and the nutrient value
                nut_val = food_item.nutrients.get(bucket, 0)
                top_data.append([i+1, food_item.name[:60], f"{score:.1f}", f"{nut_val:.1f}"])
                
            print_table(top_data, ['Rank', 'Food', 'Secondary Score', 'Nutrient Val'])

if __name__ == "__main__":
    main()
