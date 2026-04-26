# NutriShop Data Pipeline — Complete Reference

> **Last updated:** 2026-04-04
> **Status:** Bronze ✅ | Silver ✅ | Gold ✅ | Platinum 🚧 (not started)

---

## Architecture Overview

The pipeline is split into **two parallel streams** that converge at the **Platinum** layer:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         RAW DATA SOURCES                                    │
├───────────────────────────────────┬─────────────────────────────────────────┤
│  REFERENCE STREAM                 │  PRODUCT (OFF) STREAM                   │
│  (What IS a food)                 │  (What can you BUY)                     │
│  FooDB, CoFID, USDA               │  Open Food Facts (~3.6M products)       │
└───────────┬───────────────────────┴────────────────────┬────────────────────┘
            │                                            │
            ▼                                            ▼
┌───────────────────────────┐              ┌───────────────────────────┐
│  BRONZE (Extract)          │              │  BRONZE (Extract)          │
│  Raw parse → typed CSVs    │              │  Chunked parse → UK CSV    │
└───────────┬───────────────┘              └───────────┬───────────────┘
            ▼                                            ▼
┌───────────────────────────┐              ┌───────────────────────────┐
│  SILVER (Clean)            │              │  SILVER (Clean)            │
│  Standardize, classify,   │              │  Scale units, deduplicate, │
│  validate physics          │              │  merge brand variants      │
└───────────┬───────────────┘              └───────────┬───────────────┘
            ▼                                            ▼
┌───────────────────────────┐              ┌───────────────────────────┐
│  GOLD (Merge)              │              │  GOLD (Group) [LEGACY]     │
│  Fuzzy dedup across        │              │  Quality filter, dedup,    │
│  3 sources → archetypes    │              │  semantic grouping         │
└───────────┬───────────────┘              └───────────┬───────────────┘
            │                                            │
            └──────────────┬─────────────────────────────┘
                           ▼
              ┌──────────────────────────┐
              │  PLATINUM (Merge + Enrich) │
              │  Match OFF → Reference     │
              │  Taxonomy grouping          │
              │  🚧 NOT YET BUILT          │
              └──────────────────────────┘
```

---

## Layer Definitions

| Layer | Purpose | Guarantees |
|-------|---------|------------|
| **Bronze** | Raw extraction with minimal transforms | Typed CSVs, core macros present, nothing dropped silently |
| **Silver** | Standardise to uniform schema | 53-nutrient whitelist, human-readable names, physics-validated, semantic descriptors |
| **Gold** | Deduplicate and merge within each stream | One archetype per food concept, cross-source nutrient averages, merge audit log |
| **Platinum** | Merge the two streams; enrich OFF with reference micronutrients | Every purchasable product linked to its canonical reference food with full nutrient profile |

---

## REFERENCE STREAM

### Bronze — Raw Source Extraction

**Goal:** Parse each raw academic database into a flat, typed CSV with one food per row.

#### Scripts

| Script | Source | Input | Output |
|--------|--------|-------|--------|
| [`download_raw.py`](file:///c:/Users/Oren%20Arie%20Levene/Nutrition%20Project/nutrition_data_pipeline/pipeline/reference/bronze/download_raw.py) | — | Internet | `data/reference/raw/foodb/`, `data/reference/raw/cofid/` |
| [`build_bronze.py`](file:///c:/Users/Oren%20Arie%20Levene/Nutrition%20Project/nutrition_data_pipeline/pipeline/reference/bronze/build_bronze.py) | FooDB, CoFID | Raw files | `data/reference/bronze/foodb.csv`, `cofid.csv` |

#### FooDB Bronze

1. Load `Food.csv` — drops internal metadata (picture, timestamps, IDs).
2. Load `Content.csv` — filters to `source_type == 'Nutrient'`.
3. **Preparation priority:** Prefer `raw` entries, then `NaN`, then `cooked`. Deduplicates `(food_id, source_id)` keeping highest-priority prep.
4. Pivot to wide format: one food per row, one nutrient per column.
5. Merge with food metadata → `bronze/foodb.csv`.

#### CoFID Bronze

1. Load 9 Excel sheets from the McCance & Widdowson 2021 dataset (Proximates, Inorganics, Vitamins, Vitamin Fractions, SFA, MUFA, PUFA, Phytosterols, Organic Acids).
2. Merge all sheets on `(food_code, description)`.
3. Run `clean_nutrient_columns()` to coerce all nutrients to numeric.
4. Output → `bronze/cofid.csv`.

#### USDA Bronze

> [!NOTE]
> USDA is **not auto-downloaded** by `download_raw.py`. The USDA bronze CSV (`bronze/usda.csv`) is expected to already exist, presumably built via the USDA FoodData Central API in a previous workflow. The exact script for this is **not in the current `bronze/` folder**.

**Data on disk:** `data/reference/bronze/` contains `foodb.csv` (707KB, ~900 foods), `cofid.csv` (1.3MB, ~3K foods), `usda.csv` (4.9MB, ~9K foods).

---

### Silver — Standardise, Classify, Validate

**Goal:** Every silver CSV has the **exact same schema** (53 approved nutrients + metadata), human-readable column names, physics-validated, and a semantic descriptor assigned.

#### Script

| Script | Input | Output |
|--------|-------|--------|
| [`build_silver.py`](file:///c:/Users/Oren%20Arie%20Levene/Nutrition%20Project/nutrition_data_pipeline/pipeline/reference/silver/build_silver.py) | `bronze/*.csv` | `silver/*.csv` + `flagged/*_flagged.csv` |

#### Per-Source Cleaners

| Cleaner | Taxonomy Strategy |
|---------|-------------------|
| [`clean_foodb.py`](file:///c:/Users/Oren%20Arie%20Levene/Nutrition%20Project/nutrition_data_pipeline/pipeline/reference/silver/clean_foodb.py) | Maps `food_group` → `semantic_descriptor` via a static dict (e.g. `'Vegetables' → 'plant \| variable \| variable'`) |
| [`clean_cofid.py`](file:///c:/Users/Oren%20Arie%20Levene/Nutrition%20Project/nutrition_data_pipeline/pipeline/reference/silver/clean_cofid.py) | Keyword-matches `description` → `semantic_descriptor` (searches for words like "beef", "spinach", "beer") |
| [`clean_usda.py`](file:///c:/Users/Oren%20Arie%20Levene/Nutrition%20Project/nutrition_data_pipeline/pipeline/reference/silver/clean_usda.py) | Maps `food_category` → `semantic_descriptor` via a static dict |

#### Shared Processing (via [`reference_cleaning.py`](file:///c:/Users/Oren%20Arie%20Levene/Nutrition%20Project/nutrition_data_pipeline/pipeline/utils/reference_cleaning.py))

Each cleaner calls these functions in order:

1. **`standardize_column_names()`** — The central engine:
   - **Priority resolution:** For nutrients with multiple conflicting columns (Folate, Fiber, Vitamin A, Niacin, Carbs, Fat, Protein, Calories), uses a strict priority list to pick the best source.
   - **Name mapping:** ~200+ raw column names → human-readable format (e.g. `'Calcium, Ca (MG)'` → `'Calcium (mg)'`).
   - **FooDB unit conversion:** `MG_100G` columns → divide macros by 1000 to get grams.
   - **Impossible value capping:** Macros > 100g → NaN; Calories > 900 → NaN.
   - **Omega imputation:** Sums ALA + EPA + DHA → Omega-3 if the total is missing.
   - **Whitelist filter:** Keeps only the 53 approved nutrients + reference metadata. All "noise" columns (individual amino acids, obscure lipid fractions, etc.) are discarded.

2. **`enforce_uniform_schema()`** — Creates any missing whitelisted columns, padded with NaN.

3. **`clean_nutrient_columns()`** — Coerces all non-metadata to numeric, rounds to 3 significant figures.

4. **`sort_columns()`** — Metadata first, then nutrients alphabetically.

#### Physics Validation (`validate_physics()`)

Two checks; failures are **flagged but not discarded**:

| Check | Threshold | Action if failed |
|-------|-----------|------------------|
| **Macro sum** | `Protein + Fat + Carbs + Fiber > 105g` per 100g | Flag row |
| **Calorie cross-check** | Atwater estimate (`P×4 + C×4 + F×9 + Alc×7`) deviates > 25% from stated kcal | Flag row |

Flagged rows saved to `data/reference/flagged/{source}_flagged.csv` for manual review.

**Data on disk:** `data/reference/silver/` contains `foodb.csv` (273KB), `cofid.csv` (724KB), `usda.csv` (2.2MB).

---

### Gold — Cross-Source Merge & Deduplication

**Goal:** Merge all three silver reference databases into a single deduplicated "archetype" database with statistical ranges.

#### Scripts

| Script | Strategy | Output |
|--------|----------|--------|
| [`build_gold.py`](file:///c:/Users/Oren%20Arie%20Levene/Nutrition%20Project/nutrition_data_pipeline/pipeline/reference/gold/build_gold.py) | Simple name-based dedup (FooDB+CoFID merge, then add USDA) | `gold_ref_food_db.csv` (2.2MB) |
| [`build_unified_reference.py`](file:///c:/Users/Oren%20Arie%20Levene/Nutrition%20Project/nutrition_data_pipeline/pipeline/reference/gold/build_unified_reference.py) | **Preferred:** Fuzzy merge with `rapidfuzz` + nutrient compatibility | `reference_unified_gold.csv` (4.2MB) + `merge_log.csv` |

#### `build_unified_reference.py` — The Primary Gold Builder

**Two-pass merge strategy:**

**Pass 1: Primary Sources (USDA + CoFID)**
- Fuzzy threshold: `token_sort_ratio >= 60`
- Requires **5 shared non-null nutrients** to merge
- Sorted by nutrient completeness so data-rich rows seed archetypes first
- For each candidate match: checks all shared nutrients within **10% tolerance**

**Pass 2: FooDB Enrichment**
- Fuzzy threshold: `token_sort_ratio >= 80` (stricter name match)
- Requires **0 shared nutrients** (FooDB may only add new columns)
- `discard_unmatched=True` — FooDB foods not matching a USDA/CoFID archetype are dropped (avoids inflating the DB with research-only entries)

**Archetype output columns:**
- `ref_name` — shortest name variant (used as canonical)
- `name_variations` — all names found across sources
- `source` / `source_id` / `source_count` — provenance tracking
- For each nutrient: `Avg`, `Min`, `Max` (ranges when merged from 2+ sources)

**Merge audit log:** `data/reference/gold/merge_log.csv` — every merge recorded with fuzzy score, source, and archetype name.

#### `match_micronutrients.py` — OFF↔Reference Matching (Legacy)

> [!WARNING]
> This script is in the `gold/` folder but is actually a **platinum-stage** concern. It uses sentence-transformers to match OFF products to reference foods and inherit micronutrients. It reads from `.parquet` files that **no longer exist** (`SILVER_OFF_semantic.parquet`, `GOLD_REF_food_db.parquet`). This script is **broken** and will need rewriting for the platinum stage.

---

## PRODUCT (OFF) STREAM

### Bronze — Raw Extraction

**Goal:** Extract the ~50 columns we need from the 9M-row global OFF CSV dump, keeping only products with all 4 core macros.

#### Script

| Script | Input | Output |
|--------|-------|--------|
| [`build_bronze.py`](file:///c:/Users/Oren%20Arie%20Levene/Nutrition%20Project/nutrition_data_pipeline/pipeline/off/bronze/build_bronze.py) | `data/off/raw/off_products.csv.gz` (1.2GB) | `data/off/bronze/off.csv` (1.4GB) |

**Process:**
1. Reads compressed global OFF dump in 250K-row chunks
2. Selects ~50 specific columns (metadata + all available nutrients)
3. **Hard filter:** Drops any row missing `energy-kcal_100g`, `proteins_100g`, `carbohydrates_100g`, or `fat_100g`
4. Writes valid rows to CSV with zero type inference (all `dtype=str` for speed)

**Data on disk:** `data/off/bronze/off.csv` — 1.4GB

---

### Silver — Standardise & Deduplicate

The OFF silver layer has **two approaches** that were built in sequence. The current preferred approach is the **unified** scripts.

#### Approach A: Sequential Silver (3-step) — `pipeline/off/silver/`

| Step | Script | Strategy | Approximate Output |
|------|--------|----------|--------------------|
| Silver 1 | [`build_silver_1.py`](file:///c:/Users/Oren%20Arie%20Levene/Nutrition%20Project/nutrition_data_pipeline/pipeline/off/silver/build_silver_1.py) | Scale g→mg/ug, rename to 53-item schema, groupby `(brand, name)` keeping first | `off_silver_1.csv` (1.3GB) |
| Silver 2 | [`build_silver_2.py`](file:///c:/Users/Oren%20Arie%20Levene/Nutrition%20Project/nutrition_data_pipeline/pipeline/off/silver/build_silver_2.py) | Groupby **all numeric nutrients + category** (exact match → collapse) | `off_silver_2.csv` (1.2GB) |
| Silver 3 | [`build_silver_3.py`](file:///c:/Users/Oren%20Arie%20Levene/Nutrition%20Project/nutrition_data_pipeline/pipeline/off/silver/build_silver_3.py) | Normalize name (strip digits/units), round nutrients to 1DP, groupby `(norm_name, category, all_1DP_nutrients)` | `off_silver_3.csv` (1.2GB) |

#### Approach B: Unified Silver — `pipeline/off/unified/`

| Script | Scope | Strategy | Output |
|--------|-------|----------|--------|
| [`build_uk_silver.py`](file:///c:/Users/Oren%20Arie%20Levene/Nutrition%20Project/nutrition_data_pipeline/pipeline/off/unified/build_uk_silver.py) | UK-only | Filters `countries_en contains "United Kingdom"`, then 10%-tolerance smart merge within each `(norm_name, category)` group | `off_uk_silver.csv` (38MB) |
| [`build_unified_silver.py`](file:///c:/Users/Oren%20Arie%20Levene/Nutrition%20Project/nutrition_data_pipeline/pipeline/off/unified/build_unified_silver.py) | Global | Same 10%-tolerance smart merge, all countries | `off_unified_silver.csv` (761MB) |

**Unified Silver Smart Merge algorithm:**
1. Group products by `(normalized_name, main_category)`.
2. Within each group, seed the first row as an archetype.
3. For each subsequent row, check **all shared non-null nutrients** against every archetype using a **10% relative tolerance**.
4. If compatible → merge (backfill NaNs from new row into archetype).
5. If not compatible → create a new archetype (e.g. "Yogurt 0% Fat" vs "Yogurt Full Fat").
6. Output includes per-nutrient **Avg, Min, Max** statistics across merged members.

**Data on disk:** `data/product/silver/` contains both approaches' output files.

---

### Gold — Quality Filtering & Semantic Grouping (LEGACY)

> [!IMPORTANT]
> The scripts remaining in `pipeline/off/gold/` and `pipeline/reference/gold/` are **legacy reference material** for the upcoming Platinum stage. They currently point to old `.parquet` files and use outdated column names. They are **not functional** in the current pipeline.

| Script | Purpose | Status |
|--------|---------|--------|
| [`group_off_semantic.py`](file:///c:/Users/Oren%20Arie%20Levene/Nutrition%20Project/nutrition_data_pipeline/pipeline/off/gold/group_off_semantic.py) | Sentence-transformer clustering within nutrition buckets | ⚠️ **Legacy Reference Only** |
| [`match_micronutrients.py`](file:///c:/Users/Oren%20Arie%20Levene/Nutrition%20Project/nutrition_data_pipeline/pipeline/reference/gold/match_micronutrients.py) | Initial logic for matching OFF products to Reference foods | ⚠️ **Legacy Reference Only** |

---

### Platinum — Merge Streams (🚧 NOT YET BUILT)

**Goal:** Match each OFF product archetype to its closest reference food, then inherit the reference food's full micronutrient profile.

The legacy `match_micronutrients.py` in `reference/gold/` provides a starting design:
- Encode names with `sentence-transformers` (all-MiniLM-L6-v2)
- Top-5 cosine similarity candidates per OFF product
- Composite scoring: semantic similarity + macro agreement + raw/cooked preference bonus
- Confidence tiers: HIGH (sem≥0.85, dev≤0.30), MEDIUM, LOW
- HIGH+MEDIUM matches inherit all 28 reference micronutrient columns

---

## Shared Utilities

### [`reference_cleaning.py`](file:///c:/Users/Oren%20Arie%20Levene/Nutrition%20Project/nutrition_data_pipeline/pipeline/utils/reference_cleaning.py) (523 lines)

The single most important shared module. Used by both streams.

| Function | Purpose |
|----------|---------|
| `standardize_column_names()` | Priority resolution + name mapping + unit conversion + whitelist filtering |
| `enforce_uniform_schema()` | Pad missing columns with NaN to guarantee 53-item schema |
| `clean_nutrient_columns()` | Coerce to numeric, round to 3 significant figures |
| `sort_columns()` | Metadata first, nutrients alphabetically |
| `validate_physics()` | Macro-sum upper-bound check + Atwater calorie cross-check |
| `parse_weight_to_grams()` | Parse strings like "500g", "1.5kg", "12 x 330ml" → numeric grams |

### The 53-Item Nutrient Whitelist

```
Calories (kcal), Energy (kJ)
Protein (g), Carbohydrate (g), Fat (g), Fiber (g), Sugars (g)
Water (g), Ash (g), Alcohol (g)
Starch (g), Glucose (g), Fructose (g), Sucrose (g), Lactose (g), Maltose (g)
Saturated Fat (g), Monounsaturated Fat (g), Polyunsaturated Fat (g)
Trans Fat (g), Omega-3 (g), Omega-6 (g), EPA (g), DHA (g), ALA (g)
Cholesterol (mg)
Vitamin A (ug), Vitamin B6 (mg), Vitamin B12 (ug), Vitamin C (mg)
Vitamin D (ug), Vitamin E (mg), Vitamin K (ug)
Thiamin (mg), Riboflavin (mg), Niacin (mg), Folate (ug)
Pantothenic Acid (mg), Biotin (ug), Choline (mg)
Calcium (mg), Iron (mg), Magnesium (mg), Phosphorus (mg)
Potassium (mg), Sodium (mg), Zinc (mg), Copper (mg)
Manganese (mg), Selenium (ug), Iodine (ug), Chloride (mg)
Caffeine (mg)
```

### Semantic Descriptor Format

Every food gets a 3-part classification: `origin | state | process`

| Component | Values |
|-----------|--------|
| **Origin** | `plant`, `animal`, `mixed`, `unknown` |
| **State** | `raw`, `dried`, `liquid`, `processed`, `variable`, `unknown` |
| **Process** | `whole`, `ground`, `milled`, `baked`, `brewed`, `cooked`, `extracted`, `manufactured`, `cured`, `alcoholic`, `variable`, `unknown` |

**Purpose:** Acts as a blocking key for the Gold/Platinum fuzzy merge to prevent cross-category false matches (e.g. "Apple" ≠ "Apple Smoked Bacon").

---

## Data File Inventory

### Reference Stream

| Tier | File | Size | Description |
|------|------|------|-------------|
| Raw | `data/reference/raw/foodb/` | ~200MB | FooDB 2020 CSV bundle |
| Raw | `data/reference/raw/cofid/` | ~4MB | McCance & Widdowson 2021 Excel |
| Bronze | `bronze/foodb.csv` | 707KB | ~900 FooDB foods |
| Bronze | `bronze/cofid.csv` | 1.3MB | ~3,000 CoFID foods |
| Bronze | `bronze/usda.csv` | 4.9MB | ~9,000 USDA foods |
| Silver | `silver/foodb.csv` | 273KB | Standardised FooDB |
| Silver | `silver/cofid.csv` | 724KB | Standardised CoFID |
| Silver | `silver/usda.csv` | 2.2MB | Standardised USDA |
| Flagged | `flagged/*_flagged.csv` | varies | Physics-failed rows |
| Gold | `gold/gold_ref_food_db.csv` | 2.2MB | Simple merge output |
| Gold | `gold/reference_unified_gold.csv` | 4.2MB | **Preferred** — fuzzy-merged archetypes |
| Gold | `gold/merge_log.csv` | 28KB | Merge audit trail |

### Product (OFF) Stream

| Tier | File | Size | Description |
|------|------|------|-------------|
| Raw | `data/off/raw/off_products.csv.gz` | 1.2GB | Full global OFF dump |
| Bronze | `data/off/bronze/off.csv` | 1.4GB | Global, macro-complete products |
| Silver | `product/silver/off_silver_1.csv` | 1.3GB | Step 1 (schema + brand dedup) |
| Silver | `product/silver/off_silver_2.csv` | 1.2GB | Step 2 (nutrition-exact dedup) |
| Silver | `product/silver/off_silver_3.csv` | 1.2GB | Step 3 (1DP + norm name dedup) |
| Silver | `product/silver/off_uk_silver.csv` | 38MB | **Preferred** — UK-only, 10% smart merge |
| Silver | `product/silver/off_unified_silver.csv` | 761MB | Global, 10% smart merge |

---

## Gaps, Issues & Recommendations

### 🔴 Critical Issues

#### 1. USDA Bronze has no build script
There is no `download_usda.py` or `build_bronze_usda.py`. The `usda.csv` file exists but its provenance is undocumented. If the raw data is lost, there's no way to rebuild it.

**Recommendation:** Create `pipeline/reference/bronze/build_bronze_usda.py` that fetches from the USDA FoodData Central API or parses the bulk download CSVs.

#### 2. OFF Gold layer is entirely broken
All 4 scripts in `pipeline/off/gold/` reference `.parquet` files from the old monolithic pipeline (`data/processed/*.parquet`). These files were deleted during the GitHub cleanup. The scripts cannot run.

**Recommendation:** Either delete these legacy scripts (they're superseded by the unified silver approach) or rewrite them to read from the current CSV outputs. Given the unified silver scripts already do more sophisticated deduplication, these can likely be **deleted**.

#### 3. `match_micronutrients.py` is misplaced and broken
This platinum-stage script sits in `reference/gold/` and reads `.parquet` files that no longer exist. It also uses a different column naming convention than the current pipeline.

**Recommendation:** Move to `pipeline/platinum/` when building that stage. Rewrite to consume the current CSVs and use the standardised column names.

### 🟡 Design Concerns

#### 4. Two competing OFF silver approaches
There are two complete implementations of OFF silver:
- **Sequential 3-step** (`pipeline/off/silver/build_silver_1,2,3.py`) — produces 3 separate files, each ~1.2GB
- **Unified** (`pipeline/off/unified/build_uk_silver.py`, `build_unified_silver.py`) — smarter 10% tolerance merge

The sequential approach takes up ~3.6GB of disk for intermediate files. The unified approach is more principled and produces better output.

**Recommendation:** Formally deprecate the sequential 3-step approach. Keep only the unified scripts. Delete `off_silver_1.csv`, `off_silver_2.csv`, `off_silver_3.csv` to recover 3.6GB disk space.

#### 5. CoFID Bronze reads from wrong path
`build_bronze.py` looks for `DATA_RAW / 'UK_CoFID_2021.xlsx'`, but `download_raw.py` saves to `DATA_RAW / 'cofid' / 'McCance_and_Widdowsons_...xlsx'`. These paths don't match — someone must have manually renamed/moved the file for it to work.

**Recommendation:** Align the paths. Either have `download_raw.py` save to the path `build_bronze.py` expects, or update `build_bronze.py` to use the path `download_raw.py` creates.

#### 6. OFF Silver unit scaling may double-convert
In `build_silver_1.py`, Vitamin E is scaled from grams → milligrams (`×1000`), but it's also listed in the `SCALE_TO_UG` array elsewhere (which would scale `×1000000`). The code explicitly handles Vitamin E separately, but this is fragile. 

**Recommendation:** Add a unit test or assertion that checks the output range of Vitamin E values in silver (typically 0.1–30 mg per 100g).

#### 7. `build_gold.py` vs `build_unified_reference.py` — which is canon?
Two gold reference builders exist. `build_gold.py` does simple concat + name dedup. `build_unified_reference.py` does proper fuzzy merge with nutrient validation. Both produce output files.

**Recommendation:** Mark `build_unified_reference.py` as the canonical gold builder. Delete `build_gold.py` or rename it to `build_gold_simple.py` with a deprecation note.

#### 8. Semantic descriptor not used downstream
The semantic descriptors (`plant | raw | whole` etc.) are carefully assigned in Silver but **never consumed** by the Gold merge step. `build_unified_reference.py` doesn't use them as blocking keys.

**Recommendation:** Use `semantic_descriptor` as a blocking/filtering key during fuzzy matching in Gold. This would prevent cross-category false matches and dramatically speed up the O(n²) comparison.

### 🟢 Minor Issues

#### 9. No master runner script
There's no single `run_pipeline.py` that executes all stages in order. Each script must be run manually.

**Recommendation:** Create `pipeline/run_all.py` that chains: reference bronze → silver → gold, then OFF bronze → silver, with clear stage logging.

#### 10. OFF data directory is confusingly named
OFF silver outputs go to `data/product/silver/` but OFF bronze lives in `data/off/bronze/`. The inconsistency makes it hard to follow.

**Recommendation:** Standardise: either everything goes under `data/off/` (both bronze and silver), or rename `data/product/` to something more explicit.

#### 11. FooDB Bronze drops to ~900 foods
FooDB has 6,700+ food entries, but the bronze extraction produces only ~900 rows. This is because the inner merge (`foods.merge(foodb_pivot, on='food_id', how='inner')`) drops any food that has zero nutrient content entries. This is probably correct but worth validating.

---

## Pre-Platinum Checklist

Before building the Platinum layer (merge OFF ↔ Reference + taxonomy grouping), address these:

- [ ] **Fix USDA Bronze provenance** — write or document its build script
- [x] **Delete legacy OFF Gold scripts** — (Done: deleted 3/4 redundant scripts)
- [ ] **Rewrite legacy reference scripts** — (Update `group_off_semantic.py` and `match_micronutrients` to work with current CSVs)
- [ ] **Choose ONE canonical gold reference builder** — remove the duplicate
- [ ] **Choose ONE canonical OFF silver approach** — delete the other's outputs
- [ ] **Fix CoFID Bronze path mismatch**
- [ ] **Wire semantic descriptors into Gold merge** as blocking keys
- [ ] **Validate Vitamin E unit scaling** in OFF silver
- [ ] **(Optional) Create master runner script**

---

## Directory Structure Reference

```
nutrition_data_pipeline/
├── data/
│   ├── off/
│   │   ├── raw/              # off_products.csv.gz (1.2GB)
│   │   ├── bronze/           # off.csv (1.4GB)
│   │   ├── gold/             # (empty — legacy scripts broken)
│   │   ├── platinum/         # (empty — not built yet)
│   │   └── silver/           # (empty — outputs go to product/silver/)
│   ├── product/
│   │   └── silver/           # off_silver_1,2,3.csv + uk + unified
│   └── reference/
│       ├── raw/              # foodb/, cofid/
│       ├── bronze/           # foodb.csv, cofid.csv, usda.csv
│       ├── silver/           # foodb.csv, cofid.csv, usda.csv (standardised)
│       ├── flagged/          # Physics-failed rows
│       ├── gold/             # Reference unified gold + merge log
│       └── platinum/         # (empty — not built yet)
├── pipeline/
│   ├── off/
│   │   ├── bronze/           # build_bronze.py
│   │   ├── silver/           # build_silver_1,2,3.py (legacy approach)
│   │   ├── gold/             # group_off_semantic.py (⚠️ LEGACY REFERENCE)
│   │   ├── platinum/         # (empty)
│   │   └── unified/          # build_uk_silver.py, build_unified_silver.py
│   ├── reference/
│   │   ├── bronze/           # download_raw.py, build_bronze.py
│   │   ├── silver/           # build_silver.py, clean_foodb/cofid/usda.py
│   │   ├── gold/             # build_gold.py, build_unified_reference.py, match_micronutrients.py (⚠️ LEGACY REFERENCE)
│   │   └── platinum/         # (empty)
│   └── utils/
│       └── reference_cleaning.py  # Central standardisation engine
└── scripts/                  # Ad-hoc analysis scripts (see scripts/README.md)
```
