"""Analyze the results of semantic grouping."""
import pandas as pd
import sys

# Fix Windows console encoding issues
sys.stdout.reconfigure(encoding='utf-8')

# Load data
df_original = pd.read_parquet('data/processed/SILVER_OFF_quality.parquet')
df_groups = pd.read_parquet('data/processed/SILVER_OFF_semantic.parquet')

print('=' * 60)
print('GROUPING SUMMARY')
print('=' * 60)
print(f'Original products: {len(df_original):,}')
print(f'Phase 1 groups: 39,936')
print(f'Phase 2 groups: {len(df_groups):,}')
print(f'Reduction: {39936 - len(df_groups):,} groups merged')
print(f'Products per group (avg): {df_groups["member_count"].mean():.1f}')
print(f'Largest group size: {df_groups["member_count"].max()}')

print('\n' + '=' * 60)
print('TOP 10 LARGEST GROUPS')
print('=' * 60)
for _, row in df_groups.nlargest(10, 'member_count').iterrows():
    print(f'\n{row["member_count"]:3d} products: {row["canonical_name"][:70]}')
    print(f'    {len(row["brands"])} brands: {", ".join(row["brands"][:5])}...')
    print(f'    Nutrition: {row["calories"]:.0f} kcal, P:{row["protein"]:.1f}g C:{row["carbohydrate"]:.1f}g F:{row["fat"]:.1f}g')

print('\n' + '=' * 60)
print('GROUPS WITH MULTIPLE MEMBERS (sample semantic matches)')
print('=' * 60)
multi_member = df_groups[df_groups['member_count'] > 1].sample(min(20, len(df_groups[df_groups['member_count'] > 1])))
for _, row in multi_member.iterrows():
    print(f'\n{row["member_count"]:2d} products merged into: "{row["canonical_name"]}"')
    print(f'   Brands: {", ".join(row["brands"][:3])}')
