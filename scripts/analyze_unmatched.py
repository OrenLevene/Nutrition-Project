"""Analyze unmatched OFF products to understand why they're LOW confidence."""
import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

unmatched = pd.read_parquet('data/processed/GOLD_OFF_unmatched.parquet')
print(f"Total unmatched: {len(unmatched)}")

# 1. Distribution of semantic scores
print(f"\n{'='*60}")
print("SEMANTIC SCORE DISTRIBUTION (unmatched)")
print("="*60)
for lo, hi in [(0.7, 1.0), (0.6, 0.7), (0.5, 0.6), (0.4, 0.5), (0.0, 0.4)]:
    subset = unmatched[(unmatched['semantic_score'] >= lo) & (unmatched['semantic_score'] < hi)]
    print(f"  {lo:.1f}-{hi:.1f}: {len(subset):>6} ({len(subset)/len(unmatched)*100:.1f}%)")

# 2. Near-misses: semantic score >= 0.65 but failed on macros
near_misses = unmatched[unmatched['semantic_score'] >= 0.65].copy()
near_misses = near_misses.sort_values('semantic_score', ascending=False)
print(f"\n{'='*60}")
print(f"NEAR-MISSES (semantic ≥ 0.65, failed on macros): {len(near_misses)}")
print("="*60)
print("These have a GOOD name match but macros don't agree:")
for _, row in near_misses.head(25).iterrows():
    print(f"  [{row['semantic_score']:.3f} sim, {row['macro_deviation']:.2f} dev] "
          f"{row['canonical_name'][:38]:<38} → {row['best_ref_name'][:35]}")

# 3. True mismatches: semantic < 0.50
true_miss = unmatched[unmatched['semantic_score'] < 0.50]
print(f"\n{'='*60}")
print(f"TRUE MISMATCHES (semantic < 0.50): {len(true_miss)}")
print("="*60)
print("These foods have NO good match in the reference DB:")
for _, row in true_miss.sample(min(25, len(true_miss)), random_state=42).iterrows():
    print(f"  [{row['semantic_score']:.3f}] {row['canonical_name'][:50]}")

# 4. What food types are unmatched?
# Try to extract rough categories from names
from collections import Counter
words = []
for name in unmatched['canonical_name'].str.lower():
    for w in name.split():
        if len(w) > 3:
            words.append(w)

print(f"\n{'='*60}")
print("MOST COMMON WORDS IN UNMATCHED PRODUCTS")
print("="*60)
for word, count in Counter(words).most_common(40):
    print(f"  {word:<20} {count}")
