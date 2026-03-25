
import unittest
from src.utils.nutrient_limits import NutrientLimitResolver

class TestNutrientLimits(unittest.TestCase):
    def setUp(self):
        self.resolver = NutrientLimitResolver()

    def test_protein_calculation_sedentary(self):
        # Male, 30, 80kg, 2500 cal
        limits = self.resolver.get_limits("male", 30, 80, 2500, activity_level="sedentary")
        # Min RDA: 0.8 * 80 = 64g
        self.assertGreaterEqual(limits["protein"][0], 64)
    
    def test_protein_calculation_muscle_build(self):
         # Male, 30, 80kg, 2500 cal, Goal: Build
        limits = self.resolver.get_limits("male", 30, 80, 2500, goal="muscle_gain")
        # Optimal Build: 2.2 * 80 = 176g
        # Code might use 2.2 as min if set as optimal? 
        # Logic: if goal=build, min -> optimal_build (2.2)
        self.assertAlmostEqual(limits["protein"][0], 2.2 * 80, delta=1.0)

    def test_fat_calculation(self):
        # 2000 cals. 20-35%. 9 cal/g.
        # Min: 20% of 2000 = 400 cal / 9 = 44.44g
        # Max: 35% of 2000 = 700 cal / 9 = 77.77g
        limits = self.resolver.get_limits("female", 25, 60, 2000)
        self.assertAlmostEqual(limits["fat"][0], 44.44, delta=0.1)
        self.assertAlmostEqual(limits["fat"][1], 77.78, delta=0.1)

    def test_vitamins_general(self):
        limits = self.resolver.get_limits("male", 30, 80, 2500)
        # Vit C: 90 - 2000
        self.assertEqual(limits["Vitamin C (mg)"], (90, 2000))
        
        # Iron Male: 8 - 45
        self.assertEqual(limits["Iron (mg)"], (8, 45))

    def test_vitamins_gender_diff(self):
        limits_m = self.resolver.get_limits("male", 30, 70, 2000)
        limits_f = self.resolver.get_limits("female", 30, 70, 2000)
        
        # Iron: M=8, F=18
        self.assertEqual(limits_m["Iron (mg)"][0], 8)
        self.assertEqual(limits_f["Iron (mg)"][0], 18)

    def test_calories(self):
        # Male, 30, 80kg, 180cm, Sedentary (1.2)
        # BMR = (10*80) + (6.25*180) - (5*30) + 5
        # BMR = 800 + 1125 - 150 + 5 = 1780
        # TDEE = 1780 * 1.2 = 2136
        cals = self.resolver.calculate_calories("male", 30, 80, 180, "sedentary")
        self.assertAlmostEqual(cals, 2136, delta=5)

if __name__ == '__main__':
    unittest.main()
