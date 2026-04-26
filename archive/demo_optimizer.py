import os
import sys

# Add the project root to the python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.calculator.db_interface import FoodDatabase
from src.optimizer.genetic_optimizer import GeneticMealOptimizer, UserProfile

def run_demo():
    print("Loading Gold Reference Data...")
    db = FoodDatabase()
    
    # Path to the new reference unified gold CSV
    csv_path = os.path.join(
        "nutrition_data_pipeline", "data", "reference", "gold", "reference_unified_gold.csv"
    )
    
    if not os.path.exists(csv_path):
        print(f"Error: Could not find {csv_path}")
        return
        
    db.load_from_csv(csv_path)
    foods = db.get_all_foods()
    
    print(f"Loaded {len(foods)} foods.")
    if len(foods) == 0:
        print("No foods loaded. Exiting.")
        return

    # Define a basic user profile (maintenance, 2000 calories)
    print("\nInitializing User Profile (2000 kcal, balanced macros)...")
    profile = UserProfile(
        weight_kg=75.0,
        height_cm=180.0,
        age=30,
        gender="male",
        activity_level="moderate",
        goal="maintenance",
        # Custom constraints can be added here if needed
    )
    
    # Initialize Optimizer
    print("Initializing Genetic Optimizer...")
    optimizer = GeneticMealOptimizer(
        foods=foods,
        user_profile=profile,
        population_size=100,  # Smaller size for quicker demo
        generations=50        # Fewer generations for quicker demo
    )
    
    print("\nRunning Optimization (this may take a moment)...")
    best_solution = optimizer.optimize()
    
    print("\n" + "="*50)
    print("OPTIMIZATION COMPLETE")
    print("="*50)
    
    if best_solution:
        print(f"Final Fitness Score: {best_solution.fitness_score:.4f}")
        print(f"Constraint Violations: {best_solution.constraint_violations}")
        
        print("\nShopping List (Archetypes):")
        print("-" * 30)
        
        total_calories = 0
        total_protein = 0
        
        # Sort by weight (descending)
        items = sorted(best_solution.items, key=lambda x: x.weight_grams, reverse=True)
        
        for item in items:
            weight = item.weight_grams
            cals = item.food.calories * (weight / 100)
            protein = item.food.nutrients.get("protein", 0) * (weight / 100)
            
            total_calories += cals
            total_protein += protein
            
            print(f"- {item.food.name}: {weight:.0f}g ({cals:.0f} kcal, {protein:.1f}g protein)")
            
        print("-" * 30)
        print(f"Total Calories: {total_calories:.0f} / {profile.tdee:.0f}")
        print(f"Total Protein: {total_protein:.1f}g")
        
    else:
        print("Optimization failed to find a solution.")

if __name__ == "__main__":
    run_demo()
