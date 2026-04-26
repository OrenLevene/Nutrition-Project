# Formal Specification: N-Day Dietary Optimization Problem

## 1. Problem Overview
The objective is to generate an N-day meal plan (typically N=7) from a database of thousands of foods. The output must consist of both an aggregated grocery list and a daily breakdown of foods and quantities. 

The generated plan must mathematically guarantee complete nutritional adequacy (meeting minimums for 27+ micronutrients) without violating safety upper bounds (e.g., max saturated fat, max red meat), while simultaneously ensuring the plan is highly varied, palatable, and balanced from day to day.

---

## 2. Decision Variables
For each food `i` in the `Foods` database and each day `j` from 1 to N:
* `x(i,j)`: The quantity (in grams) of food `i` consumed on day `j`.
* `y(i,j)`: A binary variable indicating whether food `i` is consumed on day `j` (1 if yes, 0 if no).

---

## 3. Hard Constraints (Strict Mathematical Bounds)
A solution is considered **infeasible** if any of the following constraints are violated:

### A. Nutritional Adequacy (N-Day Aggregates)
* **Minimums:** The sum of each nutrient over N days must meet or exceed the N-day minimum target (RDA * N). This applies to ~27 essential vitamins and minerals.
* **Maximums:** The sum of specific nutrients (Fat, Saturated Fat, Sugar, Sodium) over N days must be strictly less than or equal to their N-day maximum limits.
* **Calories:** Total calories over N days must exactly equal (or fall within a strict +/- 1% bound of) Target Calories * N.

### B. Health & Realism Limits
* **Red Meat Limit:** Total red meat consumption over N days must be <= 500g.
* **Processed Meat Limit:** Total processed meat consumption over N days must be <= 70g.
* **Culinary Limits:** Quantities of specific categories (e.g., dried spices, fresh herbs, liquids) must not exceed realistic human consumption thresholds (e.g., dried spices <= 30g over N days).
* **Portion Limits:** For any food `i`, `x(i,j)` must not exceed a realistic daily maximum (e.g., 300g).
* **Non-Negativity:** `x(i,j)` must be >= 0.

---

## 4. Soft Constraints (Optimization Objectives / Penalties)
While satisfying the hard constraints, the algorithm should optimize the following criteria (either as a weighted multi-objective function or hierarchical constraints):

### A. Daily Consistency (Scheduling)
* **Macro Balance:** Total calories, protein, carbs, and fat consumed on day `j` should ideally equal the exact daily target, with an allowed deviation of +/- 10%.
* **Micro Balance:** Vitamins and minerals should ideally be spread across the N days rather than consumed all at once, with an allowed deviation of +/- 30% per day.

### B. Variety & Diversity
* **Unique Foods:** Maximize the total number of unique foods used across the N days.
* **Trio Repetition:** Minimize the repetition of the exact same 3-food combinations (trios). A specific trio should not appear more than `Round(N/3)` times.

### C. Palatability (Culinary Cohesion)
* **Minimum Seasonings:** Each day `j` must ideally contain a minimum of 3 "seasoning" items (defined as spices, herbs, or sauces) to ensure the food is flavorful.

---

## 5. Current Challenges & Complexity
1. **The "Multivitamin Pill" Problem:** The algorithm easily finds single, highly-dense foods (e.g., liver, yeast extract, massive piles of dried thyme) to instantly satisfy the micronutrient constraints, leading to highly unnatural diets. We currently use regex filters and grouping to prevent this, but it requires careful tuning.
2. **Conflicting Constraints:** Finding a plan that hits 27 minimums without exceeding the fat/calorie maximum is mathematically dense. 
3. **Continuous vs Discrete:** While ingredient quantities (`x(i,j)`) are continuous, constraints involving variety, trios, and minimum daily seasonings require boolean logic (`y(i,j)`), turning this into a complex Mixed-Integer Problem (MIP) that scales poorly over a massive database.
