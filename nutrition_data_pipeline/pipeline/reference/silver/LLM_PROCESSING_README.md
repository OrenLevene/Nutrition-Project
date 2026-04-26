# LLM Semantic Processing (Silver -> Gold Transition)

## Overview
This directory handles the transition of standardized `Silver` nutrition data into the aggregated `Gold` Reference layer. Because global raw food names are inherently messy and brand-less (e.g. "Apple, raw" vs "Pie, Apple"), we use a Large Language Model (LLM) to perform **Semantic Taxonomy Labeling**.

## The Architecture
We don't use the LLM to rewrite values or merge rows. Instead, we use the LLM as a highly-intelligent router that attaches a `food_type_label` to every single food item before it enters the final reduction pipeline.

### The Problem
Traditional fuzzy text matching will dangerously merge strings that look similar but have fundamentally different physical preparations (e.g. `Chicken, whole, raw` merging with `Chicken noodle soup`).

### The Solution: 4-Tier Zero-Shot Classification
Before `build_unified_reference.py` groups the rows, we query an LLM (such as `Gemini 1.5 Pro` or `flash`) via `google.genai` to analyze the food's name/description and sort it strictly into one of four buckets:

1.  **`single`**: The ultimate raw ingredient. Pure, foundational foods with uniform nutrition across all brands (e.g., Raw Chicken, Sugar, Plain Flour, Natural Apples).
2.  **`category`**: Highly processed goods or specific manufacturer subsets. Similar function but varying nutrition (e.g., Breads, Canned Soup, Sausage, Cheese).
3.  **`composite`**: A complete recipe or dish containing multiple distinct ingredients mixed together (e.g., Spaghetti Bolognese, Mincemeat Pie).
4.  **`supplement`**: Inedibles, baby food, or highly fortified isolates (e.g., Whey Protein Isolate).

## Pipeline Execution
1.  **Script**: We generated these labels via a classification script which iterates exclusively over the unique `semantic_descriptors`. 
2.  **Storage**: The LLM decisions are cached statically in `data/reference/pipeline_cache/food_type_labels_llm.csv`.
3.  **Integration**: Rather than hitting the LLM API live during every main pipeline run, `build_gold.py` injects this statically cached dictionary into the dataframe.

## Result
When `RAPIDFUZZ` executes the final reduction, the text-matching is mathematically confined *within* these LLM boundaries. A food tagged as `composite` physically cannot merge with a food tagged as `single`, ensuring that `Apple` and `Apple Pie` never contaminate each other's macro-nutrients.
