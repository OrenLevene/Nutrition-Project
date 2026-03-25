"""
Genetic Algorithm Meal Optimizer

Uses evolutionary optimization to find N-day meal plans that:
- Meet nutrient targets (N-day total)
- Balance macros daily (±10%)
- Limit ingredient trio repetitions

More flexible than MILP for complex variety constraints.
"""
import random
import math
from typing import List, Dict, Tuple, Any, Optional
from dataclasses import dataclass
from itertools import combinations
from src.calculator.db_interface import FoodItem
from src.utils.food_filter import filter_excluded_foods, RDA_VALUES


@dataclass
class MealDay:
    """Represents foods for a single day."""
    foods: List[Tuple[str, float]]  # [(food_id, grams), ...]


@dataclass
class MealPlan:
    """A full N-day meal plan (chromosome)."""
    days: List[MealDay]
    fitness: float = 0.0


class GeneticMealOptimizer:
    """
    Genetic algorithm optimizer for N-day meal planning.
    
    Uses evolution to find meal plans that satisfy nutrient targets
    while maintaining variety and daily balance.
    """
    
    # Nutrient tier classification for daily tolerance
    # Based on scientific research (see docs/nutrient_tiers.md)
    NUTRIENT_TIERS = {
        # Tier 1: Strict daily (±10% tolerance) - require consistent daily intake
        'tier1': {
            'tolerance': 0.10,
            'steepness': 5.0,
            'daily_weight': 5.0,
            'weekly_weight': 10.0,
            'nutrients': ['calories', 'protein', 'carbohydrate', 'fat', 'Fiber (g)']
        },
        # Tier 2: Moderate daily (±25% tolerance) - daily preferred but some flexibility
        'tier2': {
            'tolerance': 0.25,
            'steepness': 4.0,
            'daily_weight': 2.0,
            'weekly_weight': 5.0,
            'nutrients': [
                'Vitamin C (mg)', 'Thiamin (B1) (mg)', 'Riboflavin (B2) (mg)',
                'Niacin (B3) (mg NE)', 'Pantothenic Acid (mg)', 'Vitamin B6 (mg)',
                'Folate (mcg DFE)', 'Sodium (mg)', 'Potassium (mg)', 'Choline (mg)'
            ]
        },
        # Tier 3: Weekly-OK (±50% daily, strict weekly) - body stores these nutrients
        'tier3': {
            'tolerance': 0.50,
            'steepness': 3.0,
            'daily_weight': 0.5,
            'weekly_weight': 5.0,
            'nutrients': [
                'Iron (mg)', 'Zinc (mg)', 'Vitamin D (mcg)', 'Vitamin A (mcg RAE)',
                'Vitamin E (mg)', 'Vitamin K (mcg)', 'Omega-3 (ALA) (g)',
                'Omega-6 (Linoleic) (g)', 'Selenium (mcg)', 'Iodine (mcg)',
                'Vitamin B12 (mcg)', 'Calcium (mg)', 'Magnesium (mg)',
                'Phosphorus (mg)', 'Copper (mg)', 'Manganese (mg)',
                'Molybdenum (mcg)', 'Biotin (mcg)'
            ]
        }
    }
    
    def __init__(
        self,
        foods: List[FoodItem],
        population_size: int = 100,
        generations: int = 200,
        mutation_rate: float = 0.15,
        crossover_rate: float = 0.7,
        elite_count: int = 5
    ):
        # Filter out industrial/impractical foods
        filtered_foods = filter_excluded_foods(foods)
        
        # Deduplicate by name (keeping the first occurrence)
        self.foods = []
        seen_names = set()
        duplicates_count = 0
        
        for f in filtered_foods:
            if f.name not in seen_names:
                self.foods.append(f)
                seen_names.add(f.name)
            else:
                duplicates_count += 1
                
        if duplicates_count > 0:
            print(f"Removed {duplicates_count} duplicate food entries")

        self.food_dict = {f.id: f for f in self.foods}
        self.food_ids = [f.id for f in self.foods]
        
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.elite_count = elite_count
        
        print(f"GA Optimizer initialized with {len(self.foods)} foods")
    
    def optimize(
        self,
        n_days: int,
        target_calories: float,
        nutrient_targets: Dict[str, Dict[str, float]],
        daily_tolerance: float = 0.10,
        foods_per_day: int = 5,
        max_grams_per_food: float = 300.0
    ) -> Dict[str, Any]:
        """
        Run genetic algorithm optimization for N-day meal plan.
        
        Args:
            n_days: Number of days (1-30)
            target_calories: Daily calorie target
            nutrient_targets: Dict of nutrient -> {min, max} per day
            daily_tolerance: Allowed deviation from daily targets (default 10%)
            foods_per_day: Number of foods per day (default 5)
            max_grams_per_food: Maximum grams for any single food (default 300)
        
        Returns:
            Dict with meal plan and analysis
        """
        if not self.foods:
            return {"status": "Error", "message": "No foods available after filtering"}
        
        # Store targets for fitness calculation
        self.n_days = n_days
        self.target_calories = target_calories
        self.nutrient_targets = nutrient_targets
        self.daily_tolerance = daily_tolerance
        self.foods_per_day = foods_per_day
        self.max_grams = max_grams_per_food
        
        # Calculate max trio repetitions: round(N/3)
        self.max_trio_reps = max(1, round(n_days / 3))
        
        # Initialize population
        population = self._init_population()
        
        # Evolution loop
        best_ever = None
        for gen in range(self.generations):
            # Evaluate fitness
            for individual in population:
                individual.fitness = self._calculate_fitness(individual)
            
            # Sort by fitness (higher is better)
            population.sort(key=lambda x: x.fitness, reverse=True)
            
            # Track best
            if best_ever is None or population[0].fitness > best_ever.fitness:
                best_ever = self._clone_plan(population[0])
            
            # Progress logging
            if gen % 50 == 0 or gen == self.generations - 1:
                print(f"Gen {gen}: Best fitness = {population[0].fitness:.2f}")
            
            # Early termination if perfect score
            if population[0].fitness >= 95:
                print(f"Early termination at generation {gen} - excellent solution found")
                break
            
            # Create next generation
            new_population = []
            
            # Elitism: keep best individuals
            for i in range(self.elite_count):
                new_population.append(self._clone_plan(population[i]))
            
            # Fill rest with crossover and mutation
            while len(new_population) < self.population_size:
                # Tournament selection
                parent1 = self._tournament_select(population)
                parent2 = self._tournament_select(population)
                
                # Crossover
                if random.random() < self.crossover_rate:
                    child = self._crossover(parent1, parent2)
                else:
                    child = self._clone_plan(parent1)
                
                # Mutation
                if random.random() < self.mutation_rate:
                    self._mutate(child)
                
                new_population.append(child)
            
            population = new_population
        
        # Build result from best plan
        return self._build_result(best_ever)
    
    def _init_population(self) -> List[MealPlan]:
        """
        Create initial population with STRATIFIED BUCKET SAMPLING.
        
        Uses global nutrient rarity to:
        1. Assign foods to primary nutrient buckets
        2. Rank foods within buckets by secondary nutrient value
        3. Sample using softmax for probabilistic selection
        
        This ensures coverage of rare nutrients while maximizing secondary value.
        """
        # Calculate global rarity and set up buckets
        self._calculate_global_rarity()
        self._assign_to_nutrient_buckets()
        
        # Pre-identify seasoning foods for seeding
        self.seasoning_foods = [f for f in self.foods if self._is_seasoning(f.name)]
        print(f"Found {len(self.seasoning_foods)} seasoning foods for seeding")
        
        population = []
        for i in range(self.population_size):
            days = []
            for d in range(self.n_days):
                day_foods = []
                
                # FORCE 2 seasonings into each day (small amounts)
                if self.seasoning_foods and len(self.seasoning_foods) >= 2:
                    seasonings = random.sample(self.seasoning_foods, min(2, len(self.seasoning_foods)))
                    for food in seasonings:
                        if self._is_dried_spice(food.name):
                            grams = random.uniform(2, 8)
                        else:
                            grams = random.uniform(10, 30)
                        day_foods.append((food.id, grams))
                
                # Fill remaining slots with stratified bucket sampling
                remaining_slots = self.foods_per_day - len(day_foods)
                if remaining_slots > 0:
                    selected = self._stratified_nutrient_seed(remaining_slots)
                    for food in selected:
                        grams = random.uniform(50, self.max_grams)
                        day_foods.append((food.id, grams))
                
                days.append(MealDay(foods=day_foods))
            population.append(MealPlan(days=days))
        
        return population
    
    def _calculate_global_rarity(self):
        """
        Calculate how 'rare' each nutrient is across the entire food database.
        
        Rarity = 1 / (average % of RDA provided per 100 calories across all foods)
        
        High rarity = few foods provide meaningful amounts (e.g., Vitamin E, Omega-3)
        Low rarity = many foods provide meaningful amounts (e.g., Protein, Vitamin C)
        """
        self.global_rarity = {}
        
        for nutrient, targets in self.nutrient_targets.items():
            rda_min = targets.get('min', 0)
            if rda_min <= 0:
                self.global_rarity[nutrient] = 1.0
                continue
            
            # Calculate average RDA contribution per 100 calories across ALL foods
            contributions = []
            for food in self.foods:
                if food.calories > 10:  # Ignore near-zero calorie foods
                    nut_value = food.nutrients.get(nutrient, 0)
                    # Contribution = % of RDA per 100 kcal
                    contribution = (nut_value / rda_min) / (food.calories / 100)
                    contributions.append(contribution)
            
            if contributions:
                avg_contribution = sum(contributions) / len(contributions)
                # Rarity = 1 / average (higher rarity for scarce nutrients)
                # Use log to dampen extreme values
                raw_rarity = 1 / max(avg_contribution, 0.001)
                self.global_rarity[nutrient] = math.log(1 + raw_rarity)
            else:
                self.global_rarity[nutrient] = 10.0  # Very rare if no data
        
        # Report rarity values
        sorted_rarity = sorted(self.global_rarity.items(), key=lambda x: x[1], reverse=True)
        print(f"Nutrient rarity (top 5 hardest to find):")
        for nutrient, rarity in sorted_rarity[:5]:
            print(f"  {nutrient}: rarity={rarity:.2f}")
    
    def _assign_to_nutrient_buckets(self):
        """
        Assign each food to its PRIMARY nutrient bucket.
        
        A food's primary bucket is the nutrient where it provides the highest
        rarity-weighted RDA contribution (i.e., where it's most valuable).
        """
        self.nutrient_buckets = {n: [] for n in self.nutrient_targets.keys()}
        self.food_secondary_scores = {}  # Cache secondary scores for each food
        
        for food in self.foods:
            if food.calories < 10:
                continue
            
            # Find which nutrient this food is best at (rarity-weighted)
            best_nutrient = None
            best_value = 0
            
            for nutrient, targets in self.nutrient_targets.items():
                rda_min = targets.get('min', 0)
                if rda_min <= 0:
                    continue
                
                nut_value = food.nutrients.get(nutrient, 0)
                rda_pct = nut_value / rda_min
                rarity = self.global_rarity.get(nutrient, 1.0)
                weighted_value = rda_pct * rarity
                
                if weighted_value > best_value:
                    best_value = weighted_value
                    best_nutrient = nutrient
            
            if best_nutrient:
                self.nutrient_buckets[best_nutrient].append(food)
            
            # Calculate secondary score (sum of rarity-weighted contributions EXCLUDING primary)
            secondary_score = 0
            for nutrient, targets in self.nutrient_targets.items():
                if nutrient == best_nutrient:
                    continue
                rda_min = targets.get('min', 0)
                if rda_min > 0:
                    nut_value = food.nutrients.get(nutrient, 0)
                    rda_pct = nut_value / rda_min
                    rarity = self.global_rarity.get(nutrient, 1.0)
                    secondary_score += rda_pct * rarity
            
            self.food_secondary_scores[food.id] = secondary_score
        
        # Report bucket sizes
        print(f"Nutrient bucket sizes:")
        for nutrient, foods in sorted(self.nutrient_buckets.items(), key=lambda x: len(x[1]), reverse=True)[:5]:
            print(f"  {nutrient}: {len(foods)} foods")
    
    def _softmax_sample(self, foods: List, scores: List[float], temperature: float = 1.5) -> 'FoodItem':
        """
        Sample a food using softmax probabilities.
        
        Args:
            foods: List of FoodItem objects
            scores: Corresponding scores for each food
            temperature: Higher = more uniform, Lower = more greedy
        
        Returns:
            Selected FoodItem
        """
        if not foods or not scores:
            return random.choice(self.foods) if self.foods else None
        
        # Normalize scores to prevent overflow
        max_score = max(scores) if scores else 0
        if max_score == 0:
            max_score = 1
        
        # Apply softmax with temperature
        exp_scores = []
        for s in scores:
            normalized = (s - max_score) / temperature
            exp_scores.append(math.exp(normalized))
        
        total = sum(exp_scores)
        if total == 0:
            return random.choice(foods)
        
        probs = [e / total for e in exp_scores]
        
        # Sample
        return random.choices(foods, weights=probs, k=1)[0]
    
    def _stratified_nutrient_seed(self, n_foods: int) -> List['FoodItem']:
        """
        Sample foods using stratified bucket sampling with secondary ranking.
        
        Strategy:
        1. Iterate through nutrients in order of rarity (hardest first)
        2. For each nutrient bucket, sample one food using softmax on secondary scores
        3. Stop when we have n_foods
        
        This ensures coverage of rare nutrients while maximizing secondary value.
        """
        selected = []
        used_buckets = set()
        
        # Sort nutrients by rarity (highest first = hardest to find)
        sorted_nutrients = sorted(
            self.global_rarity.keys(),
            key=lambda n: self.global_rarity[n],
            reverse=True
        )
        
        # First pass: sample from each bucket in rarity order
        for nutrient in sorted_nutrients:
            if len(selected) >= n_foods:
                break
            
            bucket = self.nutrient_buckets.get(nutrient, [])
            if not bucket:
                continue
            
            # Filter out already selected foods
            available = [f for f in bucket if f.id not in {s.id for s in selected}]
            if not available:
                continue
            
            # Get secondary scores for available foods
            scores = [self.food_secondary_scores.get(f.id, 0.01) for f in available]
            
            # Sample using softmax
            chosen = self._softmax_sample(available, scores, temperature=1.5)
            if chosen:
                selected.append(chosen)
                used_buckets.add(nutrient)
        
        # Second pass: fill remaining slots from any bucket
        if len(selected) < n_foods:
            remaining = n_foods - len(selected)
            # Collect all foods not yet selected
            available = [f for f in self.foods if f.id not in {s.id for s in selected} and f.calories >= 10]
            if available:
                scores = [self.food_secondary_scores.get(f.id, 0.01) for f in available]
                for _ in range(remaining):
                    if not available:
                        break
                    chosen = self._softmax_sample(available, scores, temperature=2.0)
                    if chosen:
                        selected.append(chosen)
                        # Remove from available
                        idx = available.index(chosen)
                        available.pop(idx)
                        scores.pop(idx)
        
        return selected
    
    def _calculate_food_weights(self):
        """
        LEGACY: Calculate micronutrient density score for each food.
        Kept for backwards compatibility but _calculate_global_rarity is preferred.
        """
        self.food_weights = []
        
        macros = {'protein', 'fat', 'carbohydrate', 'calories'}
        
        for food in self.foods:
            if food.calories < 10:
                self.food_weights.append(0.01)
                continue
            
            score = 0.0
            for nutrient, targets in self.nutrient_targets.items():
                if nutrient.lower() in macros:
                    continue
                rda_min = targets.get('min', 0)
                if rda_min > 0:
                    value = food.nutrients.get(nutrient, 0)
                    contribution = (value / rda_min) / (food.calories / 100)
                    score += contribution
            
            if self._is_seasoning(food.name):
                score = max(score, 0.5)
                score *= 5.0
            
            self.food_weights.append(max(0.01, score))
        
        total = sum(self.food_weights)
        if total > 0:
            self.food_weights = [w / total for w in self.food_weights]
    
    def _weighted_food_sample(self, n: int) -> List[str]:
        """LEGACY: Sample n foods using probability weights (without replacement)."""
        if not hasattr(self, 'food_weights') or not self.food_weights:
            self._calculate_food_weights()
        
        indices = list(range(len(self.foods)))
        weights = list(self.food_weights)
        selected = []
        
        for _ in range(min(n, len(indices))):
            total = sum(weights)
            if total == 0:
                break
            probs = [w / total for w in weights]
            chosen_idx = random.choices(range(len(indices)), weights=probs, k=1)[0]
            selected.append(self.foods[indices[chosen_idx]].id)
            indices.pop(chosen_idx)
            weights.pop(chosen_idx)
        
        return selected
    
    def _calculate_fitness(self, plan: MealPlan) -> float:
        """
        Calculate fitness score for a meal plan.
        
        Components:
        - Nutrient compliance (0-20 points)
        - Daily macro balance (0-25 points) - STRICT enforcement
        - Daily micronutrient balance (0-10 points) - LENIENT, spread vitamins
        - Variety/trio penalty (0-10 points)
        - Food diversity bonus (0-5 points)
        - Red meat limit penalty (0-10 points) - WCRF cancer prevention
        - Spice/herb limit penalty (0-5 points) - sensible quantities
        - Minimum seasonings bonus (0-15 points) - make food taste good!
        """
        score = 0.0
        
        # Calculate N-day totals
        totals = self._calculate_totals(plan)
        
        # 1. Nutrient compliance (20 points max)
        nutrient_score = self._score_nutrients(totals)
        score += nutrient_score * 20
        
        # 2. Daily macro balance (25 points max) - STRICT
        balance_score = self._score_daily_balance(plan)
        score += balance_score * 25
        
        # 3. Daily micronutrient balance (10 points max) - LENIENT
        micro_balance_score = self._score_daily_micronutrients(plan)
        score += micro_balance_score * 10
        
        # 4. Variety - trio repetition penalty (10 points max)
        variety_score = self._score_variety(plan)
        score += variety_score * 10
        
        # 5. Food diversity bonus (5 points max)
        diversity = len(self._get_unique_foods(plan)) / (self.foods_per_day * 2)
        score += min(1.0, diversity) * 5
        
        # 6. Red meat limit (10 points max) - WCRF: max 500g/week
        meat_score = self._score_red_meat_limit(plan)
        score += meat_score * 10
        
        # 7. Spice/herb limits (5 points max) - sensible quantities
        spice_score = self._score_spice_limits(plan)
        score += spice_score * 5
        
        # 8. Minimum seasonings per day (15 points max) - tasty food!
        seasoning_score = self._score_min_seasonings(plan)
        score += seasoning_score * 15
        
        return score
    
    # Red meat patterns for identification
    RED_MEAT_PATTERNS = [
        'beef', 'steak', 'ground beef', 'veal', 'lamb', 'mutton',
        'pork', 'ham', 'bacon', 'sausage', 'hot dog', 'frankfurter',
        'salami', 'pepperoni', 'chorizo', 'prosciutto', 'pancetta',
        'goat', 'bison', 'venison', 'game'
    ]
    
    PROCESSED_MEAT_PATTERNS = [
        'bacon', 'sausage', 'hot dog', 'frankfurter', 'salami',
        'pepperoni', 'chorizo', 'prosciutto', 'pancetta', 'ham',
        'deli', 'luncheon', 'bologna', 'pastrami', 'corned beef'
    ]
    
    # Weekly limits (grams) based on WCRF recommendations
    RED_MEAT_WEEKLY_LIMIT = 500  # 500g/week max
    PROCESSED_MEAT_WEEKLY_LIMIT = 70  # Minimize processed meat
    
    def _is_red_meat(self, food_name: str) -> bool:
        """Check if food is red meat."""
        name_lower = food_name.lower()
        return any(pattern in name_lower for pattern in self.RED_MEAT_PATTERNS)
    
    def _is_processed_meat(self, food_name: str) -> bool:
        """Check if food is processed meat."""
        name_lower = food_name.lower()
        return any(pattern in name_lower for pattern in self.PROCESSED_MEAT_PATTERNS)
    
    def _score_red_meat_limit(self, plan: MealPlan) -> float:
        """
        Score based on red meat consumption limits.
        WCRF recommends max 500g/week red meat, minimal processed meat.
        """
        total_red_meat = 0
        total_processed_meat = 0
        
        for day in plan.days:
            for food_id, grams in day.foods:
                if food_id in self.food_dict:
                    food_name = self.food_dict[food_id].name
                    if self._is_processed_meat(food_name):
                        total_processed_meat += grams
                    elif self._is_red_meat(food_name):
                        total_red_meat += grams
        
        # Score: full points if within limits, decreasing penalty for excess
        score = 1.0
        
        # Red meat penalty (exceeding 500g/week)
        if total_red_meat > self.RED_MEAT_WEEKLY_LIMIT:
            excess_ratio = (total_red_meat - self.RED_MEAT_WEEKLY_LIMIT) / self.RED_MEAT_WEEKLY_LIMIT
            score -= min(0.5, excess_ratio * 0.5)  # Max 50% penalty for red meat
        
        # Processed meat penalty (exceeding 70g/week - stricter)
        if total_processed_meat > self.PROCESSED_MEAT_WEEKLY_LIMIT:
            excess_ratio = (total_processed_meat - self.PROCESSED_MEAT_WEEKLY_LIMIT) / self.PROCESSED_MEAT_WEEKLY_LIMIT
            score -= min(0.5, excess_ratio * 0.5)  # Max 50% penalty for processed
        
        return max(0, score)
    
    # Spice/herb/sauce patterns for identification
    # NOTE: Patterns must be specific to avoid matching non-spice foods
    DRIED_SPICE_PATTERNS = [
        'spices,',           # USDA format: "Spices, paprika"
        'paprika', 'turmeric', 'cumin', 'coriander', 'cinnamon',
        'nutmeg', 'cloves', 'cardamom', 'pepper, black', 'pepper, white',
        'chili powder', 'cayenne', 'curry powder', 'garam masala',
        'oregano, dried', 'thyme, dried', 'basil, dried', 'rosemary, dried',
        'garlic powder', 'onion powder', 'mustard seed', 'fennel seed',
        'caraway', 'allspice', 'bay leaf', 'sage, ground'
    ]
    
    FRESH_HERB_PATTERNS = [
        'parsley, fresh', 'cilantro', 'basil, fresh', 'mint, fresh',
        'thyme, fresh', 'rosemary, fresh', 'dill, fresh', 'chives',
        'tarragon', 'sage, fresh', 'oregano, fresh'
    ]
    
    SAUCE_PATTERNS = [
        'soy sauce', 'teriyaki', 'worcestershire', 'hot sauce',
        'vinegar', 'ketchup', 'mayonnaise', 'salsa',
        'pesto', 'hummus', 'tahini', 'harissa', 'sriracha',
        'fish sauce', 'oyster sauce', 'hoisin', 'bbq sauce',
        'tomato sauce', 'pasta sauce', 'marinara',
        'dressing,', 'ranch', 'caesar'
    ]
    
    # Weekly limits for spices/herbs (grams)
    DRIED_SPICE_WEEKLY_LIMIT = 30   # ~4g/day is plenty of dried spices
    FRESH_HERB_WEEKLY_LIMIT = 100   # ~15g/day of fresh herbs
    
    # Minimum seasonings per day (makes food taste good!)
    MIN_SEASONINGS_PER_DAY = 3
    
    def _is_dried_spice(self, food_name: str) -> bool:
        """Check if food is a dried spice."""
        name_lower = food_name.lower()
        return any(pattern in name_lower for pattern in self.DRIED_SPICE_PATTERNS)
    
    def _is_fresh_herb(self, food_name: str) -> bool:
        """Check if food is a fresh herb."""
        name_lower = food_name.lower()
        return any(pattern in name_lower for pattern in self.FRESH_HERB_PATTERNS)
    
    def _is_sauce(self, food_name: str) -> bool:
        """Check if food is a sauce/condiment."""
        name_lower = food_name.lower()
        return any(pattern in name_lower for pattern in self.SAUCE_PATTERNS)
    
    def _is_seasoning(self, food_name: str) -> bool:
        """Check if food is any type of seasoning (spice, herb, or sauce)."""
        return (self._is_dried_spice(food_name) or 
                self._is_fresh_herb(food_name) or 
                self._is_sauce(food_name))
    
    def _score_spice_limits(self, plan: MealPlan) -> float:
        """
        Score based on sensible spice/herb quantities.
        Penalize unrealistic amounts (e.g., 100g of dried thyme).
        """
        total_dried_spice = 0
        total_fresh_herb = 0
        
        for day in plan.days:
            for food_id, grams in day.foods:
                if food_id in self.food_dict:
                    food_name = self.food_dict[food_id].name
                    if self._is_dried_spice(food_name):
                        total_dried_spice += grams
                    elif self._is_fresh_herb(food_name):
                        total_fresh_herb += grams
        
        score = 1.0
        
        # Dried spice penalty (exceeding 30g/week)
        if total_dried_spice > self.DRIED_SPICE_WEEKLY_LIMIT:
            excess_ratio = (total_dried_spice - self.DRIED_SPICE_WEEKLY_LIMIT) / self.DRIED_SPICE_WEEKLY_LIMIT
            score -= min(0.5, excess_ratio * 0.3)
        
        # Fresh herb penalty (exceeding 100g/week)
        if total_fresh_herb > self.FRESH_HERB_WEEKLY_LIMIT:
            excess_ratio = (total_fresh_herb - self.FRESH_HERB_WEEKLY_LIMIT) / self.FRESH_HERB_WEEKLY_LIMIT
            score -= min(0.5, excess_ratio * 0.3)
        
        return max(0, score)
    
    def _score_min_seasonings(self, plan: MealPlan) -> float:
        """
        Score based on having minimum seasonings per day.
        Encourages at least 3 seasonings (spices, herbs, sauces) per day
        to make food taste good!
        """
        days_meeting_minimum = 0
        
        for day in plan.days:
            seasonings_today = 0
            for food_id, grams in day.foods:
                if food_id in self.food_dict:
                    food_name = self.food_dict[food_id].name
                    if self._is_seasoning(food_name):
                        seasonings_today += 1
            
            if seasonings_today >= self.MIN_SEASONINGS_PER_DAY:
                days_meeting_minimum += 1
        
        # Score based on proportion of days meeting minimum
        return days_meeting_minimum / len(plan.days) if plan.days else 0
    
    def _calculate_totals(self, plan: MealPlan) -> Dict[str, float]:
        """Calculate total nutrients across all days."""
        totals = {"calories": 0.0}
        for nutrient in self.nutrient_targets:
            totals[nutrient] = 0.0
        
        for day in plan.days:
            for food_id, grams in day.foods:
                if food_id not in self.food_dict:
                    continue
                food = self.food_dict[food_id]
                multiplier = grams / 100.0
                
                totals["calories"] += food.calories * multiplier
                for nutrient in self.nutrient_targets:
                    val = food.nutrients.get(nutrient, 0)
                    totals[nutrient] += val * multiplier
        
        return totals
    
    # ALL nutrients are now priority - strict enforcement across the board
    PRIORITY_NUTRIENTS = {
        'protein', 'fat', 'carbohydrate',
        'Vitamin A (mcg RAE)', 'Vitamin C (mg)', 'Vitamin E (mg)', 'Vitamin K (mcg)',
        'Thiamin (B1) (mg)', 'Riboflavin (B2) (mg)', 'Niacin (B3) (mg NE)',
        'Vitamin B6 (mg)', 'Folate (mcg DFE)', 'Vitamin B12 (mcg)',
        'Calcium (mg)', 'Iron (mg)', 'Magnesium (mg)', 'Phosphorus (mg)',
        'Potassium (mg)', 'Zinc (mg)', 'Selenium (mcg)'
    }
    
    def _score_nutrients(self, totals: Dict[str, float]) -> float:
        """
        Score how well totals meet N-day targets.
        STRICT: Requires 95%+ of all minimums, penalizes exceeding max.
        """
        scores = []
        
        # Calorie score
        target_cals = self.target_calories * self.n_days
        actual_cals = totals["calories"]
        if target_cals > 0:
            cal_ratio = actual_cals / target_cals
            cal_score = max(0, 1 - abs(1 - cal_ratio) * 2)
            scores.append(cal_score)
        
        # Nutrient scores - STRICT for all
        for nutrient, targets in self.nutrient_targets.items():
            target_min = (targets.get('min') or 0) * self.n_days
            target_max = targets.get('max')
            if target_max:
                target_max *= self.n_days
            
            actual = totals.get(nutrient, 0)
            
            if target_min > 0:
                ratio = actual / target_min
                
                # Check if exceeding max (HEAVY penalty)
                if target_max and actual > target_max:
                    over_ratio = actual / target_max
                    # Very steep penalty: score drops rapidly when over max
                    score = max(0, 1 - (over_ratio - 1) * 10)
                # Meeting minimum (95%+ required for full score)
                elif ratio >= 1.0:
                    score = 1.0
                elif ratio >= 0.95:
                    score = 0.95  # Almost there
                elif ratio >= 0.90:
                    score = 0.85  # Getting close
                elif ratio >= 0.80:
                    score = 0.70  # Needs work
                else:
                    score = ratio * 0.6  # Heavy penalty
                
                # Priority nutrients get double weight
                if nutrient in self.PRIORITY_NUTRIENTS:
                    scores.append(score)
                    scores.append(score)
                else:
                    scores.append(score)
        
        return sum(scores) / len(scores) if scores else 0
    
    def _score_daily_balance(self, plan: MealPlan) -> float:
        """
        Score how evenly macros are distributed across days.
        STRICT: Checks calories + protein + fat + carbs per day.
        """
        if self.n_days <= 1:
            return 1.0
        
        day_scores = []
        
        for day in plan.days:
            # Calculate day nutrients
            day_cals = 0
            day_protein = 0
            day_fat = 0
            day_carbs = 0
            
            for food_id, grams in day.foods:
                if food_id in self.food_dict:
                    food = self.food_dict[food_id]
                    mult = grams / 100
                    day_cals += food.calories * mult
                    day_protein += food.nutrients.get('protein', 0) * mult
                    day_fat += food.nutrients.get('fat', 0) * mult
                    day_carbs += food.nutrients.get('carbohydrate', 0) * mult
            
            # Score each macro for this day
            macro_scores = []
            
            # Calories
            if self.target_calories > 0:
                cal_dev = abs(day_cals - self.target_calories) / self.target_calories
                macro_scores.append(self._deviation_to_score(cal_dev, 'calories'))
            
            # Protein, Fat, Carbs (if targets specified)
            if 'protein' in self.nutrient_targets:
                target = self.nutrient_targets['protein'].get('min', 0)
                if target > 0:
                    dev = abs(day_protein - target) / target
                    macro_scores.append(self._deviation_to_score(dev, 'protein'))
            
            if 'fat' in self.nutrient_targets:
                target = self.nutrient_targets['fat'].get('min', 0)
                if target > 0:
                    dev = abs(day_fat - target) / target
                    macro_scores.append(self._deviation_to_score(dev, 'fat'))
            
            if 'carbohydrate' in self.nutrient_targets:
                target = self.nutrient_targets['carbohydrate'].get('min', 0)
                if target > 0:
                    dev = abs(day_carbs - target) / target
                    macro_scores.append(self._deviation_to_score(dev, 'carbohydrate'))
            
            # Average macro score for this day
            if macro_scores:
                day_scores.append(sum(macro_scores) / len(macro_scores))
        
        return sum(day_scores) / len(day_scores) if day_scores else 0
    
    def _deviation_to_score(self, deviation: float, nutrient: str = None) -> float:
        """
        Convert deviation % to score using tier-aware exponential penalty.
        
        The penalty is forgiving near optimal and becomes steep near threshold.
        Formula: penalty = (exp(k * normalized_dev) - 1) / (exp(k) - 1)
        where k = steepness and normalized_dev = deviation / tolerance
        
        Args:
            deviation: Fractional deviation from target (0.1 = 10%)
            nutrient: Nutrient name for tier lookup (None = use tier1 defaults)
        """
        tier_config = self._get_nutrient_tier(nutrient)
        tolerance = tier_config['tolerance']
        steepness = tier_config['steepness']
        
        if deviation <= 0:
            return 1.0
        
        # Normalize deviation relative to tolerance (0 = optimal, 1 = at threshold)
        normalized = deviation / tolerance
        
        # Exponential penalty: gentle near 0, steep near 1, very steep beyond
        if normalized <= 0:
            return 1.0
        
        # Calculate penalty (0 at optimal, 1 at threshold, >1 beyond)
        penalty = (math.exp(steepness * normalized) - 1) / (math.exp(steepness) - 1)
        
        # Convert penalty to score:
        # - At 0% deviation: score = 1.0
        # - At threshold (penalty=1): score = 0.1 (harsh but not zero)
        # - Beyond threshold: continues declining towards 0
        score = max(0.01, 1.0 - penalty * 0.9)
        return score
    
    def _get_nutrient_tier(self, nutrient: str) -> dict:
        """Get tier configuration for a nutrient."""
        if nutrient is None:
            return self.NUTRIENT_TIERS['tier1']  # Default to strict
        
        for tier_name, tier_config in self.NUTRIENT_TIERS.items():
            if nutrient in tier_config['nutrients']:
                return tier_config
        
        # Default to tier2 for unknown nutrients
        return self.NUTRIENT_TIERS['tier2']
    
    def _score_variety(self, plan: MealPlan) -> float:
        """Score variety - penalize excessive trio repetitions."""
        trio_counts = {}
        
        for day in plan.days:
            food_ids = [f[0] for f in day.foods]
            # Generate all 3-food combinations
            for trio in combinations(sorted(food_ids), 3):
                trio_key = tuple(trio)
                trio_counts[trio_key] = trio_counts.get(trio_key, 0) + 1
        
        # Count violations
        violations = 0
        for count in trio_counts.values():
            if count > self.max_trio_reps:
                violations += (count - self.max_trio_reps)
        
        # Score: 1.0 if no violations, decreasing with violations
        if violations == 0:
            return 1.0
        else:
            return max(0, 1 - violations * 0.1)
    
    def _score_daily_micronutrients(self, plan: MealPlan) -> float:
        """
        Score how evenly MICRONUTRIENTS are distributed across days.
        LENIENT: Uses ±30% tolerance to encourage spreading vitamin-rich foods.
        Only scores key vitamins/minerals (not macros - those are in _score_daily_balance).
        """
        if self.n_days <= 1:
            return 1.0
        
        # Key micronutrients to balance across days
        MICRO_KEYS = [
            'Vitamin A (mcg RAE)', 'Vitamin C (mg)', 'Vitamin E (mg)',
            'Folate (mcg DFE)', 'Vitamin B12 (mcg)', 'Iron (mg)',
            'Calcium (mg)', 'Potassium (mg)', 'Magnesium (mg)', 'Zinc (mg)'
        ]
        
        # Calculate daily target (average per day)
        daily_micro_targets = {}
        for nutrient in MICRO_KEYS:
            if nutrient in self.nutrient_targets:
                min_val = self.nutrient_targets[nutrient].get('min', 0)
                if min_val > 0:
                    daily_micro_targets[nutrient] = min_val
        
        if not daily_micro_targets:
            return 1.0
        
        day_scores = []
        
        for day in plan.days:
            # Calculate day's micronutrients
            day_micros = {n: 0 for n in daily_micro_targets}
            
            for food_id, grams in day.foods:
                if food_id in self.food_dict:
                    food = self.food_dict[food_id]
                    mult = grams / 100
                    for nutrient in daily_micro_targets:
                        day_micros[nutrient] += food.nutrients.get(nutrient, 0) * mult
            
            # Score each micronutrient for this day (lenient ±30%)
            micro_scores = []
            for nutrient, target in daily_micro_targets.items():
                if target > 0:
                    actual = day_micros[nutrient]
                    dev = abs(actual - target) / target
                    # Lenient scoring: full score within 30%, gradual penalty beyond
                    if dev <= 0.30:
                        micro_scores.append(1.0)
                    elif dev <= 0.50:
                        micro_scores.append(0.8)
                    elif dev <= 0.70:
                        micro_scores.append(0.5)
                    else:
                        micro_scores.append(0.2)
            
            if micro_scores:
                day_scores.append(sum(micro_scores) / len(micro_scores))
        
        return sum(day_scores) / len(day_scores) if day_scores else 1.0
    
    def _get_unique_foods(self, plan: MealPlan) -> set:
        """Get set of unique food IDs in plan."""
        unique = set()
        for day in plan.days:
            for food_id, _ in day.foods:
                unique.add(food_id)
        return unique
    
    def _tournament_select(self, population: List[MealPlan], k: int = 3) -> MealPlan:
        """Select individual via tournament selection."""
        contestants = random.sample(population, min(k, len(population)))
        return max(contestants, key=lambda x: x.fitness)
    
    def _crossover(self, parent1: MealPlan, parent2: MealPlan) -> MealPlan:
        """Create child by combining days from two parents."""
        child_days = []
        for i in range(self.n_days):
            # 50% chance to take from each parent
            if random.random() < 0.5:
                child_days.append(MealDay(foods=list(parent1.days[i].foods)))
            else:
                child_days.append(MealDay(foods=list(parent2.days[i].foods)))
        return MealPlan(days=child_days)
    
    def _mutate(self, plan: MealPlan):
        """Apply random mutations to plan."""
        mutation_type = random.choice(['swap_food', 'change_quantity', 'swap_days'])
        
        if mutation_type == 'swap_food':
            # Replace one food with another random food
            day_idx = random.randint(0, len(plan.days) - 1)
            if plan.days[day_idx].foods:
                food_idx = random.randint(0, len(plan.days[day_idx].foods) - 1)
                new_food_id = random.choice(self.food_ids)
                old_grams = plan.days[day_idx].foods[food_idx][1]
                plan.days[day_idx].foods[food_idx] = (new_food_id, old_grams)
        
        elif mutation_type == 'change_quantity':
            # Adjust quantity of one food
            day_idx = random.randint(0, len(plan.days) - 1)
            if plan.days[day_idx].foods:
                food_idx = random.randint(0, len(plan.days[day_idx].foods) - 1)
                food_id = plan.days[day_idx].foods[food_idx][0]
                new_grams = random.uniform(30, self.max_grams)
                plan.days[day_idx].foods[food_idx] = (food_id, new_grams)
        
        elif mutation_type == 'swap_days' and len(plan.days) > 1:
            # Swap two days
            i, j = random.sample(range(len(plan.days)), 2)
            plan.days[i], plan.days[j] = plan.days[j], plan.days[i]
    
    def _clone_plan(self, plan: MealPlan) -> MealPlan:
        """Create deep copy of a meal plan."""
        new_days = []
        for day in plan.days:
            new_days.append(MealDay(foods=list(day.foods)))
        return MealPlan(days=new_days, fitness=plan.fitness)
    
    def _build_result(self, plan: MealPlan) -> Dict[str, Any]:
        """Convert best plan to result format."""
        totals = self._calculate_totals(plan)
        
        # Calculate meat consumption
        total_red_meat = 0
        total_processed_meat = 0
        for day in plan.days:
            for food_id, grams in day.foods:
                if food_id in self.food_dict:
                    food_name = self.food_dict[food_id].name
                    if self._is_processed_meat(food_name):
                        total_processed_meat += grams
                    elif self._is_red_meat(food_name):
                        total_red_meat += grams
        
        # Calculate spice/herb consumption
        total_dried_spice = 0
        total_fresh_herb = 0
        for day in plan.days:
            for food_id, grams in day.foods:
                if food_id in self.food_dict:
                    food_name = self.food_dict[food_id].name
                    if self._is_dried_spice(food_name):
                        total_dried_spice += grams
                    elif self._is_fresh_herb(food_name):
                        total_fresh_herb += grams
        
        # Print stats
        print(f"\n=== WEEKLY CONSUMPTION STATS ===")
        print(f"Red meat: {total_red_meat:.0f}g / {self.RED_MEAT_WEEKLY_LIMIT}g " +
              ("[OK]" if total_red_meat <= self.RED_MEAT_WEEKLY_LIMIT else "[!] EXCEEDED"))
        print(f"Processed meat: {total_processed_meat:.0f}g / {self.PROCESSED_MEAT_WEEKLY_LIMIT}g " +
              ("[OK]" if total_processed_meat <= self.PROCESSED_MEAT_WEEKLY_LIMIT else "[!] EXCEEDED"))
        print(f"Dried spices: {total_dried_spice:.0f}g / {self.DRIED_SPICE_WEEKLY_LIMIT}g " +
              ("[OK]" if total_dried_spice <= self.DRIED_SPICE_WEEKLY_LIMIT else "[!] EXCEEDED"))
        print(f"Fresh herbs: {total_fresh_herb:.0f}g / {self.FRESH_HERB_WEEKLY_LIMIT}g " +
              ("[OK]" if total_fresh_herb <= self.FRESH_HERB_WEEKLY_LIMIT else "[!] EXCEEDED"))
        
        # Build daily breakdown
        daily_plans = []
        for i, day in enumerate(plan.days):
            day_info = {
                "day": i + 1,
                "foods": []
            }
            day_cals = 0
            for food_id, grams in day.foods:
                if food_id in self.food_dict:
                    food = self.food_dict[food_id]
                    day_info["foods"].append({
                        "name": food.name,
                        "grams": round(grams, 1),
                        "calories": round(food.calories * grams / 100, 1)
                    })
                    day_cals += food.calories * grams / 100
            day_info["total_calories"] = round(day_cals, 1)
            daily_plans.append(day_info)
        
        # Aggregate for shopping list
        shopping_list = {}
        for day in plan.days:
            for food_id, grams in day.foods:
                if food_id in self.food_dict:
                    name = self.food_dict[food_id].name
                    shopping_list[name] = shopping_list.get(name, 0) + grams
        
        # Round shopping amounts
        shopping_list = {k: round(v, 1) for k, v in shopping_list.items()}
        
        return {
            "status": "Optimal" if plan.fitness >= 80 else "Feasible",
            "fitness_score": round(plan.fitness, 2),
            "n_days": self.n_days,
            "max_trio_repetitions": self.max_trio_reps,
            "unique_foods": len(self._get_unique_foods(plan)),
            "daily_plans": daily_plans,
            "shopping_list": shopping_list,
            "totals": {
                "period": totals,
                "daily_average": {k: round(v / self.n_days, 2) for k, v in totals.items()}
            },
            "meat_stats": {
                "red_meat_g": round(total_red_meat, 1),
                "red_meat_limit_g": self.RED_MEAT_WEEKLY_LIMIT,
                "red_meat_ok": total_red_meat <= self.RED_MEAT_WEEKLY_LIMIT,
                "processed_meat_g": round(total_processed_meat, 1),
                "processed_meat_limit_g": self.PROCESSED_MEAT_WEEKLY_LIMIT,
                "processed_meat_ok": total_processed_meat <= self.PROCESSED_MEAT_WEEKLY_LIMIT
            }
        }
