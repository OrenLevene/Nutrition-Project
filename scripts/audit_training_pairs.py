"""Audit training pair quality."""
import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

pairs = pd.read_parquet('data/external/foodon_training_pairs.parquet')
print(f"Total pairs: {len(pairs)}")

# 1. Check for noisy labels (regulatory codes, EFSA, CCPR, etc.)
noise_patterns = ['\\(efsa', '\\(ccpr', '\\(us cfr', '\\(eurofir', 'agency food', 'codex']
noisy_a = pairs['text_a'].str.lower().str.contains('|'.join(noise_patterns), regex=True)
noisy_b = pairs['text_b'].str.lower().str.contains('|'.join(noise_patterns), regex=True)
noisy = noisy_a | noisy_b
print(f"\nNoisy pairs (regulatory codes): {noisy.sum()} ({noisy.sum()/len(pairs)*100:.1f}%)")

# 2. Check for non-food concepts
non_food = ['\\bplant\\b', '\\bmaterial\\b', 'organism', 'process', 'heating', 'cooling', 
            'boiling', 'enzyme', 'additive', 'chemical']
nf_a = pairs['text_a'].str.lower().str.contains('|'.join(non_food), regex=True)
nf_b = pairs['text_b'].str.lower().str.contains('|'.join(non_food), regex=True)
non_food_pairs = nf_a | nf_b
print(f"Non-food concept pairs: {non_food_pairs.sum()} ({non_food_pairs.sum()/len(pairs)*100:.1f}%)")

# 3. Total problematic
problematic = noisy | non_food_pairs
print(f"Total problematic: {problematic.sum()} ({problematic.sum()/len(pairs)*100:.1f}%)")

# 4. Show examples of clean vs noisy
print(f"\n{'='*60}")
print("SAMPLE NOISY PAIRS")
print("="*60)
for _, row in pairs[noisy].head(10).iterrows():
    print(f"  [{row['score']:.3f}] {row['text_a'][:50]} ↔ {row['text_b'][:50]}")

print(f"\n{'='*60}")
print("SAMPLE CLEAN PAIRS (high score)")
print("="*60)
clean_high = pairs[~problematic & (pairs['score'] > 0.8)]
for _, row in clean_high.sample(min(10, len(clean_high)), random_state=42).iterrows():
    print(f"  [{row['score']:.3f}] {row['text_a'][:50]} ↔ {row['text_b'][:50]}")

print(f"\n{'='*60}")
print("SAMPLE CLEAN PAIRS (medium score)")
print("="*60)
clean_mid = pairs[~problematic & (pairs['score'] > 0.4) & (pairs['score'] <= 0.7)]
for _, row in clean_mid.sample(min(10, len(clean_mid)), random_state=42).iterrows():
    print(f"  [{row['score']:.3f}] {row['text_a'][:50]} ↔ {row['text_b'][:50]}")

print(f"\n{'='*60}")
print("SAMPLE CLEAN PAIRS (low score)")
print("="*60)
clean_low = pairs[~problematic & (pairs['score'] < 0.2)]
for _, row in clean_low.sample(min(10, len(clean_low)), random_state=42).iterrows():
    print(f"  [{row['score']:.3f}] {row['text_a'][:50]} ↔ {row['text_b'][:50]}")

# 5. Clean dataset stats
clean = pairs[~problematic]
print(f"\n{'='*60}")
print(f"CLEAN DATASET: {len(clean)} pairs ({len(clean)/len(pairs)*100:.1f}%)")
print(f"{'='*60}")
print(f"Score distribution:")
for rel in ['sibling', 'cousin', 'distant']:
    subset = clean[clean['relationship'] == rel]
    if len(subset) > 0:
        print(f"  {rel}: n={len(subset)}, mean={subset['score'].mean():.3f}")
