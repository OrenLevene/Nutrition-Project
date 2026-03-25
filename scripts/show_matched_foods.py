"""Show all 4,044 matched foods for user review."""
import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

df = pd.read_parquet('data/processed/OFF_UK_WITH_MICRONUTRIENTS.parquet')
print(f"Total: {len(df)} foods")
print(f"Confidence: HIGH={len(df[df['match_confidence']=='HIGH'])}, MEDIUM={len(df[df['match_confidence']=='MEDIUM'])}")

print(f"\n{'='*90}")
print(f"ALL FOODS (sorted alphabetically)")
print(f"{'='*90}")
for _, row in df[['canonical_name','ref_name','match_confidence']].sort_values('canonical_name').iterrows():
    conf = 'H' if row['match_confidence'] == 'HIGH' else 'M'
    print(f"  [{conf}] {row['canonical_name'][:45]:<45} -> {str(row['ref_name'])[:42]}")
