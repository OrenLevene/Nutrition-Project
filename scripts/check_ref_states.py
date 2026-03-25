"""Check raw vs cooked food states in reference DB."""
import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

ref = pd.read_parquet('data/processed/BRONZE_REF_foodb_cofid_raw.parquet')

# Check how many have raw/cooked/dried in their names
desc = ref['description'].str.lower()

raw_count = desc.str.contains('raw|uncooked|dried|dry', regex=True).sum()
cooked_count = desc.str.contains('boiled|cooked|baked|fried|roasted|grilled|steamed|stewed', regex=True).sum()
neither = len(ref) - raw_count - cooked_count

print(f"Reference DB states:")
print(f"  Raw/dried: {raw_count} ({raw_count/len(ref)*100:.1f}%)")
print(f"  Cooked: {cooked_count} ({cooked_count/len(ref)*100:.1f}%)")
print(f"  Neither: {neither} ({neither/len(ref)*100:.1f}%)")

# Show pasta/rice/grain examples
for term in ['pasta', 'rice', 'bean', 'lentil', 'chicken', 'salmon', 'bread', 'cheese']:
    matches = ref[desc.str.contains(term)]
    if len(matches) > 0:
        print(f"\n--- {term} ({len(matches)} entries) ---")
        for _, row in matches.head(8).iterrows():
            cals = row.get('Energy (KCAL)', '?')
            prot = row.get('Protein (G)', '?')
            carb = row.get('Carbohydrate, by difference (G)', '?')
            fat = row.get('Total lipid (fat) (G)', '?')
            print(f"  {row['description'][:55]:<55} cal={cals:<6} P={prot:<5} C={carb:<5} F={fat}")
