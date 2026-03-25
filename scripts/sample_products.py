import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

df = pd.read_parquet('data/processed/OFF_UK_SEMANTIC_GROUPS.parquet')

# Get a diverse sample of challenging products
samples = [
    # Sauces
    'ketchup', 'oyster sauce', 'hot sauce', 'worcestershire', 'pesto',
    # Ready meals
    'tikka masala', 'lasagne', 'pad thai', 'shepherd',
    # Composite
    'granola bar', 'flapjack', 'protein bar',
    # Simple
    'milk', 'salmon', 'bread', 'apple', 'rice',
    # Tricky
    'smoothie', 'hummus', 'kimchi', 'miso', 'tofu',
    # Drinks
    'lager', 'wine', 'cola', 'juice', 'water',
]

for term in samples:
    matches = df[df['canonical_name'].str.contains(term, case=False, na=False)]
    if len(matches) > 0:
        print(f"  {matches.iloc[0]['canonical_name'][:70]}")
    else:
        print(f"  [NO MATCH: {term}]")
