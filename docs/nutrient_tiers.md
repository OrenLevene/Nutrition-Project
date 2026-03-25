# Nutrient Tier Classification: Scientific Rationale

## Overview

This document defines tolerance levels for daily nutrient variation in the meal optimizer. Nutrients are classified into three tiers based on research evidence for how critical daily consistency is versus weekly sufficiency.

---

## Tier 1: Strict Daily (±10% tolerance)

These nutrients require consistent daily intake. Large daily variations cause immediate physiological or metabolic consequences.

| Nutrient | Daily Tolerance | Rationale | Source |
|----------|-----------------|-----------|--------|
| **Calories** | ±10% | Metabolic research shows ±50-100 kcal compliance is standard. Deficits >250 kcal trigger metabolic adaptation. Daily energy balance is critical for stable blood glucose and metabolic function. | [NIH] |
| **Protein** | ±10% | Muscle protein synthesis (MPS) is stimulated every 3-5h by amino acids. Daily distribution of 1.6-2.2 g/kg is optimal. Per-meal threshold: 20-40g for MPS. | [ISSN Position Stand] |
| **Carbohydrates** | ±10% | Primary fuel for brain and muscles. Day-to-day consistency prevents energy crashes and glycogen depletion. | [Examine.com] |
| **Total Fat** | ±10% | Essential for satiety, hormone production, and absorption of fat-soluble vitamins. Large daily swings affect satiety signals. | [Harvard Nutrition] |
| **Fiber** | ±15% | Gut microbiome depends on consistent fiber supply. Fiber-starved bacteria consume intestinal mucus lining. SCFAs (butyrate) produced from fiber protect gut barrier. | [Stanford Medicine, NIH] |

---

## Tier 2: Moderate Daily (±25% tolerance)

These nutrients should be consumed daily but have some body reserves or slower turnover rates allowing modest daily variation.

| Nutrient | Daily Tolerance | Rationale | Source |
|----------|-----------------|-----------|--------|
| **Vitamin C** | ±25% | Water-soluble, not stored significantly. Plasma saturates at 100-200 mg/day. However, scurvy takes weeks to develop at zero intake. Daily is ideal, but 2-3 day gaps are tolerable. | [NIH ODS] |
| **B Vitamins (B1, B2, B3, B5, B6, Folate)** | ±25% | Water-soluble, limited storage. Required daily for energy metabolism and coenzyme functions. However, tissue reserves can buffer 1-2 days of low intake. | [NIH ODS, Colostate] |
| **Sodium** | ±25% | Electrolyte balance is regulated hourly by kidneys. Large daily swings can cause fluid shifts, but healthy kidneys adapt within hours. | [EFSA] |
| **Potassium** | ±25% | Works with sodium for fluid balance. Kidney regulation allows some daily flexibility. | [EFSA] |

---

## Tier 3: Weekly-OK (±50% daily, strict weekly)

These nutrients can be consumed intermittently. The body stores them efficiently OR absorption is improved with spacing.

| Nutrient | Daily Tolerance | Weekly Tolerance | Rationale | Source |
|----------|-----------------|------------------|-----------|--------|
| **Iron** | ±50% | ±10% | Hepcidin regulation: oral iron increases hepcidin for 24-48h, blocking further absorption. Alternate-day dosing is MORE effective than daily. Weekly supplementation equals daily for improving hemoglobin. | [NIH, Haematologica, WHO] |
| **Zinc** | ±50% | ±10% | Weekly supplementation showed similar improvements in zinc status vs daily in RCTs. Body has small zinc pool in bone/muscle. | [NIH RCT] |
| **Vitamin D** | ±50% | ±10% | Fat-soluble, stored in adipose. Weekly/monthly supplementation equals daily for 25(OH)D levels. | [Frontiers in Endocrinology] |
| **Vitamin A** | ±50% | ±10% | Fat-soluble, liver stores last months. Weekly dosing safe and effective. | [NIH] |
| **Vitamin E** | ±50% | ±10% | Fat-soluble, stored in adipose tissue. Deficiency takes months to develop. | [NIH ODS] |
| **Vitamin K** | ±50% | ±10% | Fat-soluble. Dietary assessment shows 3-4 non-consecutive days capture true intake. | [NIH] |
| **Omega-3 (EPA/DHA)** | ±50% | ±10% | Fat-soluble, stored in cell membranes. Standard recommendation: fatty fish 2-3x/week, NOT daily. | [AHA Guidelines] |
| **Selenium** | ±50% | ±10% | Stored in muscles and thyroid. Daily fluctuations well-tolerated if weekly intake is adequate. | [NIH ODS] |
| **Iodine** | ±50% | ±10% | Thyroid stores significant iodine reserves. Weekly intake is the relevant measure. | [NIH ODS] |
| **B12** | ±50% | ±10% | Exception among B vitamins: stored in liver (2-5 year reserves). Weekly intake is sufficient. | [Mayo Clinic] |

---

## Penalty Function: Smooth Exponential Curve

Instead of a hard cutoff (linear penalty), we use an **exponential penalty** that is forgiving near the optimal and becomes steep as deviation approaches the threshold.

### Mathematical Formulation

```python
def nutrient_penalty(actual, target, tolerance, steepness=4.0):
    """
    Smooth exponential penalty function.
    
    - Near optimal (deviation ≈ 0): penalty ≈ 0
    - At threshold (deviation = tolerance): penalty = 1.0
    - Beyond threshold: penalty grows exponentially
    
    Args:
        actual: Actual daily intake
        target: Target daily intake (weekly/7)
        tolerance: Tier tolerance (0.10 for Tier 1, 0.25 for Tier 2, etc.)
        steepness: Controls curve shape (higher = sharper transition)
    """
    deviation = abs(actual - target) / target  # Fractional deviation
    normalized = deviation / tolerance          # 0 at optimal, 1 at threshold
    
    # Exponential: gentle near 0, steep near 1, very steep beyond
    penalty = (math.exp(steepness * normalized) - 1) / (math.exp(steepness) - 1)
    
    return penalty
```

### Visualization

```
Penalty
  │
1.0├──────────────────────────────●  ← Threshold (10% for Tier 1)
   │                           ╱
   │                         ╱
   │                       ╱
0.5├─────────────────────●
   │                   ╱
   │                ╱
   │            ╱
0.0├─●───────●───────────────────────
   └─┬───────┬───────┬───────┬───────→ Deviation %
     0%      5%     10%     15%
         (optimal)  (threshold)
```

### Tier-Specific Parameters

| Tier | Tolerance | Steepness | Penalty at 50% of Threshold | Penalty at Threshold |
|------|-----------|-----------|----------------------------|---------------------|
| **Tier 1** | 10% | 5.0 | ~0.12 | 1.0 |
| **Tier 2** | 25% | 4.0 | ~0.14 | 1.0 |
| **Tier 3** | 50% daily | 3.0 | ~0.16 | 1.0 |

### Why This Works

1. **Forgiving near optimal**: Small measurement errors or food portion variations don't heavily penalize otherwise good plans.
2. **Progressive warning zone**: As you approach the limit, the penalty increases noticeably but not catastrophically.
3. **Hard wall at threshold**: Exceeding the tolerance becomes very costly, steering the optimizer away.
4. **Differentiable**: Smooth gradients help the GA find better solutions through crossover/mutation.

---

## Tier Weight Multipliers

The smooth penalty is then multiplied by a tier-specific weight:

| Tier | Daily Weight | Weekly Weight | Effect |
|------|-------------|---------------|--------|
| **Tier 1** | 5.0× | 10.0× | Daily consistency critical |
| **Tier 2** | 2.0× | 5.0× | Daily preferred, weekly important |
| **Tier 3** | 0.5× | 5.0× | Daily flexible, weekly strict |

---

## References

1. NIH Office of Dietary Supplements - Nutrient Fact Sheets
2. ISSN Position Stand on Protein and Exercise (2017)
3. Stanford Medicine - Fiber and Gut Microbiome
4. Haematologica - Hepcidin and Iron Absorption
5. Frontiers in Endocrinology - Vitamin D Supplementation Frequency
6. American Heart Association - Omega-3 Guidelines
7. WHO - Weekly Iron Supplementation
8. EFSA - Dietary Reference Values for Sodium and Potassium
