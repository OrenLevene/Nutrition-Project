# Ideas & Deferred Work

## Fine-Tuning Training Data (Deferred)
- FoodOn training pairs are 36.6% noisy (EFSA codes, regulatory references, non-food concepts)
- Need to completely redo the training data before fine-tuning
- Options:
  1. Clean FoodOn data more aggressively (filter out all coded entries, GS1, EFSA, CCPR)
  2. Generate training pairs from our own 38k OFF products using LLM-generated facets
  3. Combine both: use clean FoodOn pairs + our own product pairs
- The organism tree in FoodOn (biological taxonomy) could supplement the food product tree for richer training data

## Nutritional Signature Facet
- Use our nutrient distinctiveness methodology as a facet dimension
- For products missing micronutrient data, use LLM to estimate typical values (e.g. "salmon is typically high in omega-3, vitamin D")
- Then compute the signature from estimated + real data

## Non-Differentiator Exclusions
- Confirmed non-differentiators for clustering: organic, decaf, free-range, fair-trade
- These are production/sourcing preferences, not food identity markers
- Store as metadata, not as clustering facets
