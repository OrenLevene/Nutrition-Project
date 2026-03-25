"""Check grouping results."""
import pandas as pd

df = pd.read_parquet('data/processed/SILVER_OFF_grouped.parquet')
print(f"Total groups: {len(df)}")
print(f"Duplicate groups (>1 member): {len(df[df['member_count']>1])}")
print(f"Products in duplicates: {df[df['member_count']>1]['member_count'].sum()}")

print("\nTop 10 duplicate groups by size:")
dupes = df[df['member_count']>1].nlargest(10,'member_count')
for _, row in dupes.iterrows():
    name = str(row['product_name'])[:40] if row['product_name'] else '(empty)'
    supers = list(row['supermarkets']) if len(row['supermarkets']) > 0 else []
    print(f"  {row['member_count']:2} products: {name} | Supermarkets: {supers}")

print("\nSupermarket coverage:")
all_supers = []
for supers in df['supermarkets']:
    if len(supers) > 0:
        all_supers.extend(supers)
from collections import Counter
for s, c in Counter(all_supers).most_common():
    print(f"  {s}: {c} groups")
