import pulp
from typing import List, Dict, Tuple, Union, Optional
from src.calculator.db_interface import FoodItem

class NutritionOptimizer:
    def __init__(self, foods: List[FoodItem]):
        self.foods = foods

    def optimize_diet(
        self, 
        target_calories: float, 
        nutrient_constraints: Dict[str, Union[Tuple[float, float], Dict[str, float]]] = None,
        user_profile: Dict[str, Union[str, int, float]] = None,
        days: int = 7,
        calorie_tolerance: float = 0.05,
        nutrient_tolerance: float = 0.20,
        max_food_weight: float = 1000.0,
        time_limit: int = 60
    ):
        """
        Optimizes weekly diet with hard constraints and hybrid discrete/continuous portions.
        
        Args:
            target_calories: Target DAILY calorie intake
            nutrient_constraints: Dict of nutrient -> (min, max) or {'min': x, 'max': y} (DAILY)
            user_profile: {age, gender, weight, activity_level, goal} for auto-constraints
            days: Number of days to optimize for (default 7 for weekly)
            calorie_tolerance: Fraction tolerance for calorie target (default 5%)
            nutrient_tolerance: Fraction tolerance for nutrient constraints (default 20%)
            max_food_weight: Maximum grams of any single food per week (default 1000g)
            time_limit: Solver time limit in seconds
        
        Returns:
            Dict with:
                - selected_foods: Weekly quantities in grams
                - totals: {"weekly": {...}, "daily_average": {...}}
                - nutrient_analysis: Comparison to daily targets
        """
        # Resolve dynamic constraints if profile provided
        if user_profile:
            from src.utils.nutrient_limits import NutrientLimitResolver
            resolver = NutrientLimitResolver()
            
            if target_calories is None or target_calories <= 0:
                height = user_profile.get("height", 170)
                target_calories = resolver.calculate_calories(
                    gender=user_profile.get("gender", "male"),
                    age=user_profile.get("age", 30),
                    weight_kg=user_profile.get("weight", 70),
                    height_cm=height,
                    activity_level=user_profile.get("activity_level", "sedentary"),
                    goal=user_profile.get("goal", "general")
                )
            
            auto_constraints = resolver.get_limits(
                gender=user_profile.get("gender", "male"),
                age=user_profile.get("age", 30),
                weight_kg=user_profile.get("weight", 70),
                target_calories=target_calories,
                activity_level=user_profile.get("activity_level", "sedentary"),
                goal=user_profile.get("goal", "general")
            )
            final_constraints = auto_constraints.copy()
            if nutrient_constraints:
                final_constraints.update(nutrient_constraints)
            nutrient_constraints = final_constraints
        
        if nutrient_constraints is None:
            nutrient_constraints = {}
        
        # Scale daily targets to weekly for optimization
        weekly_calories = target_calories * days
        weekly_constraints = {}
        for nutrient, constraint in nutrient_constraints.items():
            min_val, max_val = None, None
            if isinstance(constraint, dict):
                min_val = constraint.get('min')
                max_val = constraint.get('max')
            elif isinstance(constraint, (tuple, list)):
                min_val = constraint[0] if len(constraint) > 0 else None
                max_val = constraint[1] if len(constraint) > 1 else None
            elif isinstance(constraint, (int, float)):
                min_val = constraint
            
            weekly_constraints[nutrient] = {
                'min': min_val * days if min_val is not None else None,
                'max': max_val * days if max_val is not None else None
            }
        
        # Get portion resolver for hybrid mode
        from src.utils.portion_sizes import get_resolver
        portion_resolver = get_resolver()
        
        # Create the LP problem
        prob = pulp.LpProblem("Weekly_Diet_Optimization", pulp.LpMinimize)
        
        # Decision Variables
        food_vars = {}      # Stores (variable, pack_size, mode) for each food
        use_vars = {}       # Binary: is food used?
        
        for food in self.foods:
            y = pulp.LpVariable(f"use_{food.id}", cat='Binary')
            use_vars[food.id] = y
            
            # Get portion info for this food
            portion_info = portion_resolver.get_portion_info(food.name)
            pack_size = portion_info["pack_size"]
            is_discrete = portion_info["is_perishable"]  # True = discrete (integer packs)
            
            if is_discrete:
                # ========================================
                # DISCRETE MODE: Integer number of packs
                # For perishables and canned goods
                # ========================================
                max_packs = max(1, int(max_food_weight / pack_size))
                n = pulp.LpVariable(f"packs_{food.id}", lowBound=0, upBound=max_packs, cat='Integer')
                food_vars[food.id] = (n, pack_size, "discrete")
                
                # Big-M constraint: n <= max_packs * y
                prob += n <= max_packs * y, f"BigM_{food.id}"
            else:
                # ========================================
                # CONTINUOUS MODE: Grams (can weigh from bag)
                # For non-perishables like rice, pasta, nuts
                # ========================================
                x = pulp.LpVariable(f"amount_{food.id}", lowBound=0, upBound=max_food_weight, cat='Continuous')
                food_vars[food.id] = (x, 1.0, "continuous")
                
                # Big-M constraint: x <= max_weight * y
                prob += x <= max_food_weight * y, f"BigM_{food.id}"

        # Helper function to get the amount expression for a food (in grams)
        def get_amount_expr(food_id):
            var, pack_size, mode = food_vars[food_id]
            if mode == "discrete":
                return var * pack_size  # packs × grams_per_pack
            else:
                return var  # direct grams

        # Objective: Minimize number of different foods
        objective = pulp.lpSum([use_vars[food.id] for food in self.foods])
        prob.setObjective(objective)
        
        # =========================================
        # CONSTRAINTS (ALL HARD)
        # =========================================
        
        # 1. Calories (Hard Constraint with ±5% tolerance) - WEEKLY
        min_cals = weekly_calories * (1 - calorie_tolerance)
        max_cals = weekly_calories * (1 + calorie_tolerance)
        
        total_calories = pulp.lpSum([
            get_amount_expr(food.id) * (food.calories / 100.0) 
            for food in self.foods
        ])
        prob += total_calories >= min_cals, "Min_Weekly_Calories"
        prob += total_calories <= max_cals, "Max_Weekly_Calories"
        
        # 2. Nutrients (Hard Constraints with ±20% tolerance) - WEEKLY
        for nutrient, weekly_constraint in weekly_constraints.items():
            min_val = weekly_constraint.get('min')
            max_val = weekly_constraint.get('max')
            
            total_nutrient = pulp.lpSum([
                get_amount_expr(food.id) * (food.nutrients.get(nutrient, 0) / 100.0) 
                for food in self.foods
            ])
            
            # Apply tolerance to create a band around the target
            if min_val is not None:
                adjusted_min = min_val * (1 - nutrient_tolerance)  # Allow 20% below
                prob += total_nutrient >= adjusted_min, f"Min_{nutrient}"
                
            if max_val is not None:
                adjusted_max = max_val * (1 + nutrient_tolerance)  # Allow 20% above
                prob += total_nutrient <= adjusted_max, f"Max_{nutrient}"

        # Solve with Time Limit
        print(f"Solving MILP (HYBRID mode) with {len(self.foods)} foods for {days} days. Limit: {time_limit}s")
        solver = pulp.PULP_CBC_CMD(msg=1, timeLimit=time_limit)
        prob.solve(solver)
        
        status = pulp.LpStatus[prob.status]
        print(f"Solver Status: {status}")
        
        # Build result
        result = {
            "status": status,
            "days": days,
            "selected_foods": {},         # Weekly quantities in grams
            "selected_portions": {},      # Pack info for discrete foods
            "totals": {
                "weekly": {"calories": 0, "nutrients": {}},
                "daily_average": {"calories": 0, "nutrients": {}}
            },
            "mode": "hybrid"
        }
        
        # Only read values if solver found a solution (Optimal or time-limited feasible)
        # Don't read on "Infeasible" - the values are garbage
        if status in ["Optimal", "Not Solved"]:
            weekly_cals = 0
            weekly_nutrients = {n: 0 for n in weekly_constraints.keys()}
            
            # Check if we have any values
            has_values = False
            for food in self.foods:
                var, _, _ = food_vars[food.id]
                val = var.varValue
                if val is not None and val > 0:
                    has_values = True
                    break
            
            if has_values:
                for food in self.foods:
                    var, pack_size, mode = food_vars[food.id]
                    
                    if mode == "discrete":
                        packs = var.varValue
                        if packs and packs > 0.001:
                            packs = int(round(packs))
                            amount = packs * pack_size
                            result["selected_foods"][food.name] = amount
                            result["selected_portions"][food.name] = {
                                "packs": packs,
                                "pack_size_g": pack_size,
                                "total_g": amount,
                                "mode": "discrete"
                            }
                            
                            weekly_cals += amount * (food.calories / 100.0)
                            for n in weekly_constraints.keys():
                                n_val = food.nutrients.get(n, 0)
                                weekly_nutrients[n] += amount * (n_val / 100.0)
                    else:
                        amount = var.varValue
                        if amount and amount > 0.001:
                            result["selected_foods"][food.name] = round(amount, 1)
                            result["selected_portions"][food.name] = {
                                "grams": round(amount, 1),
                                "mode": "continuous"
                            }
                            
                            weekly_cals += amount * (food.calories / 100.0)
                            for n in weekly_constraints.keys():
                                n_val = food.nutrients.get(n, 0)
                                weekly_nutrients[n] += amount * (n_val / 100.0)
                
                # Store weekly totals
                result["totals"]["weekly"]["calories"] = round(weekly_cals, 1)
                result["totals"]["weekly"]["nutrients"] = {k: round(v, 2) for k, v in weekly_nutrients.items()}
                
                # Calculate daily averages
                result["totals"]["daily_average"]["calories"] = round(weekly_cals / days, 1)
                result["totals"]["daily_average"]["nutrients"] = {
                    k: round(v / days, 2) for k, v in weekly_nutrients.items()
                }
                
                # Store original daily constraints for comparison
                result["daily_constraints"] = nutrient_constraints
                result["daily_calorie_target"] = target_calories
                
                if status != "Optimal":
                    result["status"] = "Feasible (Time Limit)" if status == "Not Solved" else status
            else:
                result["status"] = "No Solution Found"
        
        # If infeasible, diagnose WHY
        if status == "Infeasible":
            result["infeasibility_analysis"] = self._analyze_infeasibility(
                weekly_calories, weekly_constraints, nutrient_tolerance
            )

        return result
    
    def _analyze_infeasibility(
        self, 
        weekly_calories: float, 
        weekly_constraints: dict,
        nutrient_tolerance: float
    ) -> dict:
        """
        Analyze why the optimization might be infeasible.
        For each nutrient, calculate how many calories are needed to hit the target.
        """
        analysis = {
            "calorie_budget": weekly_calories,
            "nutrient_requirements": [],
            "total_min_calories_needed": 0,
            "most_problematic": []
        }
        
        for nutrient, constraint in weekly_constraints.items():
            min_val = constraint.get('min')
            if min_val is None or min_val <= 0:
                continue
            
            # Adjusted minimum with tolerance
            target = min_val * (1 - nutrient_tolerance)
            
            # Find the best source for this nutrient (per calorie) in our food pool
            best_density = 0
            best_food = None
            
            for food in self.foods:
                if food.calories > 0:
                    nutrient_value = food.nutrients.get(nutrient, 0)
                    density = nutrient_value / food.calories  # per kcal
                    if density > best_density:
                        best_density = density
                        best_food = food.name
            
            # Calculate calories needed to hit target from best source alone
            if best_density > 0:
                kcal_needed = target / best_density
                pct_of_budget = (kcal_needed / weekly_calories) * 100
            else:
                kcal_needed = float('inf')
                pct_of_budget = float('inf')
            
            analysis["nutrient_requirements"].append({
                "nutrient": nutrient,
                "weekly_target": round(target, 1),
                "best_source": best_food[:40] if best_food else "None",
                "kcal_needed": round(kcal_needed, 0) if kcal_needed != float('inf') else "∞",
                "pct_of_budget": round(pct_of_budget, 1) if pct_of_budget != float('inf') else "∞"
            })
            
            if pct_of_budget != float('inf'):
                analysis["total_min_calories_needed"] += kcal_needed
        
        # Sort by % of budget (descending) to find most problematic
        sorted_reqs = sorted(
            [r for r in analysis["nutrient_requirements"] if r["pct_of_budget"] != "∞"],
            key=lambda x: x["pct_of_budget"],
            reverse=True
        )
        analysis["most_problematic"] = sorted_reqs[:5]
        
        # Calculate if total requirements exceed budget
        analysis["total_pct"] = round(
            (analysis["total_min_calories_needed"] / weekly_calories) * 100, 1
        )
        analysis["explanation"] = (
            f"To meet ALL nutrient minimums would require at least "
            f"{analysis['total_pct']}% of your calorie budget. "
            f"The most demanding nutrients are: "
            + ", ".join([f"{r['nutrient']} ({r['pct_of_budget']}%)" for r in analysis["most_problematic"][:3]])
        )
        
        return analysis

