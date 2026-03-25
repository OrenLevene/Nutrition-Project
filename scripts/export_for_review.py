"""Export matched food list to CSV for user review."""
import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

df = pd.read_parquet('data/processed/OFF_UK_WITH_MICRONUTRIENTS.parquet')
export = df[['canonical_name','ref_name','match_confidence','semantic_score',
             'macro_deviation','calories','protein','carbohydrate','fat']].sort_values('canonical_name')
export.to_csv('data/processed/MATCHED_FOODS_FOR_REVIEW.csv', index=False)
print(f"Exported {len(export)} foods to MATCHED_FOODS_FOR_REVIEW.csv")
print(f"HIGH: {(df['match_confidence']=='HIGH').sum()}")
print(f"MEDIUM: {(df['match_confidence']=='MEDIUM').sum()}")
