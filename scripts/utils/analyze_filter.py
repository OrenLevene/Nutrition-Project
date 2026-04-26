import pandas as pd
import sys
from pathlib import Path

DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent
DATA_BRONZE = DATA_PIPELINE_DIR / 'data' / 'reference' / 'bronze'

EXCLUDE_PATTERNS = [
    'industrial', 'leavening agent', 'seed gum', 'gums, seed',
    'guar gum', 'infant formula', 'medical food', 'meat extender',
    'formulated bar', 'protein isolate', 'whey protein', 'soy protein',
    'casein', 'oil, industrial', 'shortening', 'margarine-like',
    'butter replacement', 'egg substitute', 'imitation', 'analogue',
    'test sample', 'experimental', 'breakfast type, low calorie', 'breakfast type, powder',
    'fruit-flavored drink, powder', 'orange-flavor drink',
    'instant breakfast', 'drink mix, powder', 'sports drink',
    'energy drink', 'meal replacement', 'nutritional supplement', 'fortified',
    'liver', 'giblets', 'gizzard', 'variety meats', 'by-products',
    'blood', 'brain', 'tongue', 'heart', 'kidney', 'tripe',
    'sweetbreads', 'intestine', 'chitlins', 'bone marrow', 'spleen', 'lungs',
    'canned, condensed', 'canned, prepared', 'canned, ready-to-serve',
    'soup, canned', 'soup, instant', 'frozen dinner', 'frozen meal',
    'frozen entree', 'tv dinner', 'microwave meal', 'ready-to-eat',
    'heat and serve', 'meal kit', 'game meat', 'squirrel', 'bison', 'elk', 'deer', 'venison',
    'rabbit', 'moose', 'bear', 'caribou', 'antelope', 'wild boar',
    'horse', 'frog legs', 'turtle', 'alligator', 'ostrich', 'emu',
    'quail', 'pheasant', 'chiton', 'alaska native', 'new zealand',
    'acerola', 'drumstick leaves', 'amaranth leaves', 'jute, potherb',
    'new zealand spinach', 'vinespinach', 'pumpkin leaves',
    'sweet potato leaves', 'cress, garden', 'dock, raw',
    'prairie turnip', 'agave, raw', 'nopales', 'mollusks, snail', 'sea cucumber',
    'baby food', 'gerber', 'stage 1', 'stage 2', 'toddler',
    'restaurant', 'fast food', 'school lunch', 'hospital',
    'pastelitos', 'apache', 'luxury loaf', 'yeast extract',
    r'dried.*egg', r'egg.*dried', r'powder', r'isolate', r'concentrate',
    r'defatted', r'low fat.*flour', r'beverage', r'drink',
    r'shake', r'supplement', r'rennet', r'vital wheat gluten',
    r'pectin', r'lecithin', r'gelatin, dry',
    r'dried whole egg', r'egg yolk, dried',
    r'flour, soy', r'oil, flaxseed, contains added',
]

def analyze_filter(source_name, csv_name, name_col='description'):
    bronze_path = DATA_BRONZE / csv_name
    if not bronze_path.exists():
        return
        
    df = pd.read_csv(bronze_path)
    names_lower = df[name_col].str.lower().fillna('')
    
    print(f"\n{'='*20} {source_name} {'='*20}")
    
    pattern_hits = {}
    total_dropped = 0
    
    for pattern in EXCLUDE_PATTERNS:
        mask = names_lower.str.contains(pattern, regex=True, na=False)
        hits = df[mask][name_col].tolist()
        if hits:
            # We only count it for the FIRST pattern it hits to avoid double counting
            df = df[~mask]
            names_lower = names_lower[~mask]
            pattern_hits[pattern] = hits
            total_dropped += len(hits)
            
    print(f"Total dropped: {total_dropped}\n")
    
    # Sort patterns by how many foods they dropped
    sorted_patterns = sorted(pattern_hits.items(), key=lambda x: len(x[1]), reverse=True)
    
    for pattern, hits in sorted_patterns:
        print(f"[{len(hits)} drops] Pattern: '{pattern}'")
        # Show up to 3 examples
        for i in range(min(3, len(hits))):
            print(f"    - {hits[i]}")

if __name__ == '__main__':
    # Analyze the big one first: USDA
    analyze_filter('USDA', 'usda.csv')
    analyze_filter('CoFID', 'cofid.csv')
