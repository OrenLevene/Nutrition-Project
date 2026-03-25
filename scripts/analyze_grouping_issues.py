"""
Analyze grouped OFF products for potential issues.
Check if products that were grouped together actually belong together.
"""
import pandas as pd

products = pd.read_parquet('data/processed/SILVER_OFF_quality.parquet')
groups = pd.read_parquet('data/processed/SILVER_OFF_grouped.parquet')
mapping = pd.read_parquet('data/processed/SILVER_OFF_group_map.parquet')

# Get only duplicate groups (>1 member)
dupes = groups[groups['member_count'] > 1].copy() 👀
print(f"Analyzing {len(dupes)} duplicate groups ({dupes['member_count'].sum()} products)\n")

# Analyze each duplicate group 👀
issues = []

for _, group in dupes.iterrows(): 👀
    group_id = group['group_id']
    member_ids = group['member_food_ids']
    
    # Get full product details for this group
    group_products = products[products['food_id'].isin(member_ids)] 👀
    
    # Check for issues
    
    # Issue 1: Very different product names in same group
    names = group_products['description'].tolist() 👀
    if len(set(names)) > 1:
        # Different names - check if they're really the same product
        # Get unique name prefixes (before brand)
        name_parts = [n.split(' - ')[0].lower().strip() for n in names]
        unique_parts = set(name_parts)
        
        if len(unique_parts) > 1:
            # Different product names with same nutrition - might be an issue
            issues.append({
                'group_id': group_id,
                'issue': 'Different product names',
                'member_count': len(names),
                'names': names[:5],  # First 5
                'nutrition': f"{group['calories']:.0f}cal, {group['protein']:.1f}p, {group['carbohydrate']:.1f}c, {group['fat']:.1f}f"
            })

print("="*80)
print("POTENTIAL ISSUES: Different product names grouped together")
print("="*80)

# Show first 20 issues
for i, issue in enumerate(issues[:20]):
    print(f"\nGroup {issue['group_id']}: {issue['member_count']} products | {issue['nutrition']}")
    for name in issue['names']:
        print(f"  - {name[:70]}")

print(f"\n\nTotal groups with different names: {len(issues)}")
print(f"This is {len(issues)/len(dupes)*100:.1f}% of duplicate groups")

# Also check for groups where products have very different brands
print("\n" + "="*80)
print("SAMPLE OF MULTI-BRAND GROUPS (expected - same product, different stores)")
print("="*80)

multi_brand = dupes[dupes['brands'].apply(len) > 3].head(10)
for _, g in multi_brand.iterrows():
    print(f"\n{g['product_name'][:50]}")
    print(f"  Brands: {g['brands'][:5]}")
    print(f"  Supermarkets: {g['supermarkets']}")
