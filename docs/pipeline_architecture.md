# NutriShop Pipeline Architecture

## Overview

This document describes the complete methodology from raw food data sources through to optimized, purchasable shopping lists. Each stage includes the scientific rationale, code locations, and data flow.

---

## High-Level Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                 1. DATA SOURCES                                          │
├───────────────────┬───────────────────┬───────────────────┬─────────────────────────────┤
│  FooDB (6.7K)     │  CoFID (3K)       │  USDA (9K)        │  Open Food Facts (42K+ UK)  │
│  [Primary Acids/  │  [UK Foods]       │  [Gap-fill for    │  [Store Products with       │
│   Micronutrients] │                   │   Vit D,E,Omega]  │   Brands & Prices]          │
└────────┬──────────┴─────────┬─────────┴─────────┬─────────┴──────────────┬──────────────┘
         │                    │                   │                        │
         ▼                    ▼                   ▼                        ▼
┌────────────────────────────────────────────────────┐   ┌────────────────────────────────┐
│         2. INGESTION & MERGING                      │   │   2b. OFF UK PROCESSING         │
│         scripts/build_merged_nutrition_db.py        │   │   - Ingest UK products          │
│         → MERGED_FOODB_COFID_DB.parquet             │   │   - Quality filtering           │
│         → PRIMARY_NUTRITION_DB.parquet              │   │   - Deduplication               │
└────────────────────────┬───────────────────────────┘   │   - Brand grouping              │
                         │                               │   → OFF_UK_QUALITY.parquet       │
                         ▼                               └──────────────┬─────────────────┘
┌────────────────────────────────────────────────────┐                  │
│         3. FOOD FILTERING                           │                  │
│         src/utils/food_filter.py                    │                  │
│         - Remove industrial ingredients             │                  │
│         - Remove exotic meats, baby food            │                  │
│         - Remove restaurant/fast food               │                  │
│         → ~5,500 filtered canonical foods           │                  │
└────────────────────────┬───────────────────────────┘                  │
                         ▼                                              │
┌────────────────────────────────────────────────────┐                  │
│         4. NUTRIENT ANALYSIS                        │                  │
│         - Calculate global nutrient rarity          │                  │
│         - Assign foods to primary nutrient buckets  │                  │
│         - Calculate secondary nutrient scores       │                  │
└────────────────────────┬───────────────────────────┘                  │
                         ▼                                              │
┌────────────────────────────────────────────────────┐                  │
│         5. GENETIC ALGORITHM OPTIMIZATION           │                  │
│         src/optimizer/genetic_optimizer.py          │                  │
│         - Stratified bucket sampling initialization │                  │
│         - Tiered nutrient penalties (±10%/25%/50%)  │                  │
│         - Tournament selection + crossover          │                  │
│         - Daily balance + variety scoring           │                  │
│         → Optimized N-day meal plan                 │                  │
└────────────────────────┬───────────────────────────┘                  │
                         ▼                                              │
┌────────────────────────────────────────────────────────────────────────┘
│         6. STORE MATCHING & OUTPUT                  │
│         src/ingestion/product_matcher.py            │
│         - Fuzzy match canonical → OFF products      │
│         - Extract sizes, deduplicate by brand       │
│         - Link to supermarket availability          │
│         → JSON shopping list with store_options[]   │
└────────────────────────────────────────────────────┘
```

---

## Detailed Methodology

### Step 1: Data Sources

| Source | Foods | Purpose | Primary Nutrients | File |
|--------|-------|---------|-------------------|------|
| **FooDB** | 6,700+ | Research-grade amino acids, fatty acids, comprehensive micronutrients | All B vitamins, minerals, amino acids | `scripts/build_merged_nutrition_db.py` |
| **CoFID** | 3,000+ | UK-specific foods with local naming conventions | General nutrition | `scripts/build_merged_nutrition_db.py` |
| **USDA** | 9,000+ | Gap-filling for nutrients poorly covered in FooDB | Vitamin D, E, Omega-3/6 | `src/ingestion/ingest_usda.py` |
| **Open Food Facts** | 42,000+ UK | Real store products with brands, barcodes, prices | Macros + key micros | `src/ingestion/open_food_facts.py` |

**Why these sources?**
- **FooDB** provides the most complete micronutrient profiles for optimization
- **CoFID** ensures UK product names are recognized (e.g., "courgette" not "zucchini")
- **USDA** fills specific gaps where FooDB data is sparse
- **OFF** bridges the gap between canonical foods and purchasable products

---

### Step 2: Ingestion & Merging

#### 2a. Canonical Nutrition Database

**File:** `scripts/build_merged_nutrition_db.py`

**Process:**
1. Load FooDB foods and nutrients, pivot to food-per-row format
2. Load CoFID from Excel, handle multiple food group sheets
3. Standardize column names to unified format (e.g., `Vitamin C (mg)`)
4. Match CoFID foods to FooDB by fuzzy name similarity
5. Merge nutrients: prefer FooDB values, CoFID as fallback
6. Consolidate duplicate columns (e.g., `fiber` vs `Fiber (g)`)

**Outputs:**
- `MERGED_FOODB_COFID_DB.parquet` - Raw merged data
- `PRIMARY_NUTRITION_DB.parquet` - Final canonical nutrition database

#### 2b. Open Food Facts UK Processing

**Pipeline Steps:**

| Step | Script | Input → Output | Purpose |
|------|--------|----------------|---------|
| 1. Download & Filter | `src/ingestion/open_food_facts.py` | OFF CSV → `off_uk_products_full.parquet` | Extract UK products with completeness ≥50% |
| 2. Quality Filter | `scripts/filter_off_uk_quality.py` | Full → `OFF_UK_QUALITY.parquet` | Require all 4 core macros (kcal, protein, carbs, fat) |
| 3. Deduplicate | `scripts/deduplicate_off.py` | Quality → `OFF_UK_DEDUPED.parquet` | Remove exact duplicates, normalize brand names |
| 4. Group | `scripts/group_off_products.py` | Deduped → `OFF_UK_GROUPED.parquet` | Group by normalized name + nutrition fingerprint |

**OFF Quality Criteria:**
```python
REQUIRED_MACROS = ['energy-kcal_100g', 'proteins_100g', 'carbohydrates_100g', 'fat_100g']
# Completeness > 0.5 (50%)
# All 4 macros present and >= 0
# Country contains 'united-kingdom'
```

**Brand Normalization:**
```python
# Consolidates UK supermarket variants:
# "By Sainsbury's", "Sainsburys" → "Sainsbury's"
# "M&S", "Marks And Spencer" → "Marks & Spencer"
# "Tesco Finest", "Tesco Everyday Value" → "Tesco"
```

---

### Step 3: Food Filtering

**File:** `src/utils/food_filter.py`

**Philosophy:** Focus on whole, minimally-processed foods that users can actually purchase and prepare at home.

#### Exclusion Categories

| Category | Examples | Rationale |
|----------|----------|-----------|
| **Industrial ingredients** | Dried egg, protein isolate, seed gums, pectin | Not consumer-purchasable |
| **Fortified drinks** | Vitamin water, meal shakes, supplement | Artificial nutrient sources |
| **Exotic/game meats** | Bison, elk, deer, rabbit, bear, ostrich | Not in regular UK supermarkets |
| **Organ meats** | Liver, kidney, brain, heart, tongue | Low consumer acceptance |
| **Baby food** | Gerber, infant formula | Specialized products |
| **Restaurant/prepared foods** | Fast food, frozen dinners, meal kits | Not reproducible at home |
| **Yeast extract** | Marmite | Extremely concentrated B vitamins, not a staple |

**Pattern Matching:**
```python
EXCLUDE_PATTERNS = [
    'industrial', 'infant formula', 'medical food',
    'protein isolate', 'game meat', 'bison', 'venison',
    'liver', 'kidney', 'brain', 'baby food',
    'fast food', 'hospital', 'frozen dinner', ...
]
```

**Result:** ~5,500 filtered foods available for optimization

---

### Step 4: Nutrient Analysis & Bucketing

**File:** `src/optimizer/genetic_optimizer.py`

#### 4a. Global Nutrient Rarity

**Formula:**
```python
# For each nutrient, calculate how "hard" it is to get from food
rarity[nutrient] = 1 / mean(rda_pct_per_100kcal across all foods)
```

**Result:** Rare nutrients (Biotin, Omega-3, Vitamin E, Molybdenum) get higher rarity scores than common ones (Protein, Vitamin C).

#### 4b. Primary Bucket Assignment

Each food is assigned to ONE primary nutrient bucket:
```python
bucket[food] = argmax(rda_pct[nutrient] × rarity[nutrient])
```

**Example:** Eggs have both Protein (common) and Biotin (rare). Rarity-weighting assigns eggs to the **Biotin bucket** because that's where they provide the most unique value.

#### 4c. Secondary Score

For each food, calculate value from all OTHER nutrients:
```python
secondary_score = Σ (rda_pct[n] × rarity[n])  for n ≠ primary_bucket
```

**Purpose:** Prefer "multi-tasking" foods over single-nutrient specialists.

---

### Step 5: Genetic Algorithm Optimization

**File:** `src/optimizer/genetic_optimizer.py`

#### 5a. Population Initialization (Stratified Bucket Sampling)

**Strategy:**
1. Sort nutrients by rarity (hardest first)
2. For each nutrient bucket, sample one food using softmax on secondary scores
3. This ensures rare nutrients are covered from the initial generation

```python
# Softmax sampling with temperature for exploration
probabilities = softmax(secondary_scores / temperature)
selected_food = random.choices(bucket_foods, weights=probabilities)[0]
```

#### 5b. Fitness Function (100 points max)

| Component | Points | Description |
|-----------|--------|-------------|
| **Nutrient compliance** | 20 | Weekly targets met (95%+ of RDA minimum, below UL) |
| **Daily macro balance** | 25 | Calories, protein, carbs, fat within ±10% daily |
| **Daily micronutrient balance** | 10 | Key vitamins/minerals within ±25-50% daily |
| **Variety (trio penalty)** | 10 | Avoid repeating same 3-food combinations |
| **Food diversity** | 5 | Bonus for unique foods |
| **Red meat limit** | 10 | WCRF: max 500g/week red meat |
| **Spice limits** | 5 | Sensible quantities (<30g dried, <100g fresh) |
| **Minimum seasonings** | 15 | ≥3 spices/herbs/sauces per day for taste |

#### 5c. Tiered Nutrient Penalty System

**Scientific Basis:** See `docs/nutrient_tiers.md`

| Tier | Tolerance | Nutrients | Rationale |
|------|-----------|-----------|-----------|
| **Tier 1** | ±10% | Calories, Protein, Carbs, Fat, Fiber | MPS, energy balance, gut microbiome |
| **Tier 2** | ±25% | Water-soluble vitamins (C, B-complex), electrolytes | Limited storage, daily turnover |
| **Tier 3** | ±50% daily | Fat-soluble vitamins (A, D, E, K), minerals (Iron, Zinc), Omega-3 | Efficient body storage |

**Penalty Function:**
```python
def nutrient_penalty(deviation, tolerance, steepness=4.0):
    # Exponential: forgiving near optimal, steep near threshold
    normalized = deviation / tolerance
    return (exp(steepness * normalized) - 1) / (exp(steepness) - 1)
```

#### 5d. Evolution

- **Selection:** Tournament (size 3)
- **Crossover:** Day-wise swap between parents
- **Mutation:** Replace food with bucket-sampled alternative (maintains nutrient coverage)
- **Elitism:** Top 5 individuals preserved unchanged

---

### Step 6: Store Matching & Output

**Files:** `src/ingestion/product_matcher.py`, `src/utils/store_lookup.py`

#### 6a. Canonical → Store Product Matching

**Strategy:**
1. **Manual mappings first** (highest priority) - curated exceptions
2. **Category filtering** - reduce search space by ~95%
3. **Fuzzy matching** - rapidfuzz `token_set_ratio` for name similarity

#### 6b. Size Extraction & Normalization

```python
SIZE_PATTERNS = [
    r'(\d+(?:\.\d+)?)\s*kg',    # 1.5kg → 1500g
    r'(\d+(?:\.\d+)?)\s*g\b',   # 400g
    r'(\d+)\s*ml',              # 500ml ≈ 500g
    r'(\d+)\s*pint',            # 2 pints → 1136g
]
```

#### 6c. Brand & Supermarket Tracking

```python
SUPERMARKET_BRANDS = {
    'tesco', 'sainsbury', 'asda', 'morrisons',
    'waitrose', 'aldi', 'lidl', 'coop', 'marks', 'm&s'
}
# Groups track which supermarkets carry each product
```

#### 6d. Output Format

```json
{
  "days": [...],
  "shopping_list": [
    {
      "canonical_name": "Salmon, Atlantic, raw",
      "weekly_grams": 350,
      "store_options": [
        {
          "product_name": "Salmon Fillets 280g",
          "brand": "Sainsbury's",
          "size_grams": 280,
          "quantity_needed": 2,
          "supermarkets": ["Sainsbury's"]
        }
      ]
    }
  ]
}
```

---

## Data Files Reference

### Processed Nutrition Data

| File | Records | Description |
|------|---------|-------------|
| `PRIMARY_NUTRITION_DB.parquet` | ~9,000 | Merged FooDB + CoFID canonical foods |
| `MERGED_FOODB_COFID_DB.parquet` | ~9,000 | Raw merge before consolidation |
| `real_food_nutrition.parquet` | ~5,500 | After exclusion filtering |

### Open Food Facts Data

| File | Records | Description |
|------|---------|-------------|
| `off_uk_products_full.parquet` | ~45,000 | All UK products from OFF |
| `OFF_UK_QUALITY.parquet` | ~42,000 | Pass quality filters |
| `OFF_UK_DEDUPED.parquet` | ~30,000 | After deduplication |
| `OFF_UK_GROUPED.parquet` | ~25,000 | Grouped by product + nutrition |
| `store_products_mapping.parquet` | varies | Canonical → store mapping |

---

## Code-to-Step Mapping

| Step | Primary File | Key Functions |
|------|--------------|---------------|
| 1. Load data | `src/calculator/db_interface.py` | `FoodDatabase.load_from_parquet()` |
| 2a. Merge DBs | `scripts/build_merged_nutrition_db.py` | `load_foodb()`, `load_cofid()`, `merge_databases()` |
| 2b. OFF ingest | `src/ingestion/open_food_facts.py` | `OpenFoodFactsClient.run_full_pipeline()` |
| 2c. OFF quality | `scripts/filter_off_uk_quality.py` | (script) |
| 2d. OFF dedup | `scripts/deduplicate_off.py` | `normalize_brand()` |
| 2e. OFF group | `scripts/group_off_products.py` | `normalize_name()`, `create_nutrition_key()` |
| 3. Filter | `src/utils/food_filter.py` | `filter_excluded_foods()`, `is_excluded_food()` |
| 4. Score | `src/optimizer/genetic_optimizer.py` | `_calculate_global_rarity()`, `_assign_to_nutrient_buckets()` |
| 5. Optimize | `src/optimizer/genetic_optimizer.py` | `optimize()`, `_calculate_fitness()` |
| 6. Match stores | `src/ingestion/product_matcher.py` | `ProductMatcher.fuzzy_match_canonical()` |
| 7. Serve API | `src/web_app/main.py` | `optimize_diet()` endpoint |

---

## Scientific References

### Nutrient Requirements
- NIH Office of Dietary Supplements - RDA/AI values
- EFSA - European dietary reference values
- ISSN Position Stand on Protein (2017)

### Optimization Rationale
- WCRF - Red meat limits (500g/week)
- AHA - Omega-3 guidelines (2-3x/week fish)
- Stanford Medicine - Fiber and gut microbiome

### Tiered Tolerance Basis
- See `docs/nutrient_tiers.md` for full citations
