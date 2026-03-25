import unittest
from src.optimizer.optimizer import NutritionOptimizer
from src.calculator.db_interface import FoodItem

class TestOptimizerRanges(unittest.TestCase):
    def setUp(self):
        # Create a small mock database
        self.foods = [
            FoodItem(id="1", name="C", calories=10, nutrients={"Vitamin C": 100}), # High C
            FoodItem(id="2", name="Fill", calories=100, nutrients={"Vitamin C": 0}), # Filler
        ]
        self.optimizer = NutritionOptimizer(self.foods)

    def test_dict_constraints(self):
        # Target: 200 calories, Vit C 50-150
        # "C" provides 10 cal, 100 Vit C. 100g = 1 unit
        # "Fill" provides 100 cal.
        # Solution should pick some amount of C
        
        constraints = {
            "Vitamin C": {"min": 50, "max": 150}
        }
        
        result = self.optimizer.optimize_diet(target_calories=200, nutrient_constraints=constraints)
        
        self.assertEqual(result['status'], 'Optimal')
        totals = result['totals']['nutrients']
        self.assertGreaterEqual(totals['Vitamin C'], 50)
        self.assertLessEqual(totals['Vitamin C'], 150)
        
    def test_tuple_constraints_compatibility(self):
        # Ensure old tuple format still works
        constraints = {
            "Vitamin C": (50, 150)
        }
        result = self.optimizer.optimize_diet(target_calories=200, nutrient_constraints=constraints)
        self.assertEqual(result['status'], 'Optimal')

    def test_auto_constraints(self):
        # Test that user profile triggers auto constraints
        # Profile: Male, 30, 80kg. Protein min should be 0.8 * 80 = 64g
        # Mock DB items need 'protein'
        self.foods.append(FoodItem(id="3", name="Chicken", calories=150, nutrients={"protein": 30}))
        
        optimizer = NutritionOptimizer(self.foods)
        profile = {"gender": "male", "age": 30, "weight": 80}
        
        # We expect a constraint on protein >= 64g
        result = optimizer.optimize_diet(target_calories=500, user_profile=profile)
        
        # Check if protein was constrained. If it wasn't, it might just pick 0 protein.
        # But if constrained, it must pick sufficient Chicken.
        # 64g protein ~ 2.13 servings of Chicken (320 cals).
        # Target 500 cals. Feasible.
        
        self.assertEqual(result['status'], 'Optimal')
        self.assertGreaterEqual(result['totals']['nutrients'].get('protein', 0), 64)
        
if __name__ == '__main__':
    unittest.main()
