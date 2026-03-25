"""Extract unique words from product names for classification."""
import pandas as pd
import re
from collections import Counter

df = pd.read_parquet('data/processed/SILVER_OFF_quality.parquet')
names = df['description'].str.split(' - ').str[0]

all_words = []
for n in names:
    words = re.findall(r'[a-zA-Z]+', str(n).lower())
    all_words.extend(words)

word_counts = Counter(all_words)
print(f'Total unique words: {len(word_counts)}')

print('\nTop 300 words by frequency:')
for w, c in word_counts.most_common(300):
    print(f'{w}: {c}')
