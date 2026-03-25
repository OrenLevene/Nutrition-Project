"""Check current OFF UK data status."""
import pandas as pd

df = pd.read_parquet('data/processed/BRONZE_OFF_raw.parquet')

print(f"Total UK products: {len(df)}")
print(f"\nColumns: {list(df.columns)}")

# Check completeness
if 'completeness' in df.columns:
    print(f"\nCompleteness stats:")
    print(f"  Mean: {df['completeness'].mean():.2f}")
    print(f"  > 0.5: {len(df[df['completeness'] > 0.5])}")

# Check macros
macros = ['energy-kcal_100g', 'proteins_100g', 'carbohydrates_100g', 'fat_100g']
df_macros = df.dropna(subset=macros)
print(f"\nWith all 4 macros filled: {len(df_macros)}")

# Combined filter
if 'completeness' in df.columns:
    df_quality = df[(df['completeness'] > 0.5) & df[macros].notna().all(axis=1)]
    print(f"Completeness > 0.5 AND all macros: {len(df_quality)}")
