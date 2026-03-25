# Scripts

Utility scripts for data processing, analysis, and testing.

## Pipeline Scripts

| Script | Purpose |
|--------|---------|
| `build_merged_nutrition_db.py` | Build merged nutrition DB from FooDB + UK CoFID |
| `generate_portions.py` | Generate portion size data for foods |
| `group_off_products.py` | Group similar OFF products by nutrition |
| `deduplicate_off.py` | Deduplicate Open Food Facts products |
| `rebuild_store_mapping.py` | Rebuild store product → canonical food mapping |

## Data Extraction

| Script | Purpose |
|--------|---------|
| `extract_off_uk.py` | Extract UK products from Open Food Facts |
| `filter_off_uk_quality.py` | Filter OFF UK products by quality criteria |

## Quick Checks

| Script | Purpose |
|--------|---------|
| `check_grouping.py` | Verify grouping results |
| `check_off_data.py` | Check OFF UK data status |
| `analyze_grouping_issues.py` | Find potential grouping problems |
| `analyze_off_duplicates.py` | Analyze duplicate products |

## Demos & Testing

| Script | Purpose |
|--------|---------|
| `demo.py` | Interactive demo of the optimizer |
| `test_api_quick.py` | Quick API endpoint test (requires running server) |

## Utilities

| Script | Purpose |
|--------|---------|
| `md_to_pdf.py` | Convert markdown to PDF |
| `show_food_distributions.py` | Visualize food nutrient distributions |
| `extract_words.py` | Word frequency analysis for product names |

---

## analysis/

Detailed analysis scripts for data exploration:

| Script | Purpose |
|--------|---------|
| `analyze_db.py` | Database statistics and coverage |
| `analyze_cheat_foods.py` | Find "cheat" high-calorie foods |
| `analyze_food_types.py` | Categorize foods by type |
| `check_api_data.py` | Verify API data consistency |
| `check_data_types.py` | Check column data types |
| `compare_parquets.py` | Compare two parquet files |
| `export_food_list.py` | Export food list to CSV |
| `find_fdc_ids.py` | Look up FDC IDs |
| `find_food_details.py` | Get detailed food info |
| `inspect_parquet.py` | Inspect parquet file structure |
| `list_categories.py` | List all food categories |
