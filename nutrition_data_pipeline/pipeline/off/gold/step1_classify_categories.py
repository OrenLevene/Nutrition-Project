"""
OFF Gold Pipeline — Phase 1: Category Cleanup & Food Type Classification

Assigns every OFF UK product a food_type_label (single/category/composite/supplement)
using the OFF category taxonomy combined with keyword-based classification for
uncategorized products.

Mirrors the exact same tiering strategy used for Reference (USDA/CoFID).
"""
import pandas as pd
import numpy as np
import logging
import re
from pathlib import Path
from collections import Counter

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
SILVER_PATH = DATA_PIPELINE_DIR / 'data' / 'product' / 'silver' / 'off_uk_silver.csv'
GOLD_DIR = DATA_PIPELINE_DIR / 'data' / 'off' / 'gold'
CACHE_DIR = DATA_PIPELINE_DIR / 'data' / 'off' / 'pipeline_cache'
OUTPUT_PATH = GOLD_DIR / 'off_uk_labeled.csv'
CACHE_PATH = CACHE_DIR / 'off_category_labels.csv'

# ═══════════════════════════════════════════════════════════════════════════════
# CLASSIFICATION RULES
# ═══════════════════════════════════════════════════════════════════════════════
# These are applied to both the OFF `main_category_en` and — for uncategorized
# products — to the `norm_name` field.  The logic mirrors the LLM prompt we
# used for Reference foods:
#   single    = Pure base ingredient, uniform nutrition across brands
#   category  = Processed product with recipe variance across brands
#   composite = Multi-ingredient dish / ready meal
#   supplement = Fortified isolate, protein powder, baby food

# ── Composite indicators (multi-ingredient prepared dishes) ──────────────────
COMPOSITE_CATEGORIES = {
    'pizzas', 'pizza', 'pies', 'quiches', 'lasagnes', 'lasagna',
    'sandwiches', 'wraps', 'burritos', 'tacos',
    'meals', 'ready meals', 'microwave meals', 'frozen meals',
    'meals with chicken', 'meals with meat', 'meals with fish',
    'prepared dishes', 'prepared meals', 'one-dish meals',
    'salads', 'prepared salads', 'mixed salads',
    'soups', 'stews', 'curries', 'curry', 'chilli',
    'pasta dishes', 'noodle dishes', 'rice dishes',
    'sushi', 'dim sum', 'spring rolls',
    'pot noodles', 'instant noodles',
    'meal kits', 'cooking kits',
    'shepherds pie', 'cottage pie', 'fish pie',
    'moussaka', 'risotto', 'paella', 'biryani',
    'fajitas', 'enchiladas', 'quesadillas',
    'stuffed', 'filled', 'topped',
}

COMPOSITE_KEYWORDS = [
    'ready meal', 'microwave meal', 'frozen meal', 'meal deal',
    'meal for', 'serves 2', 'family meal', 'meal kit',
    'pie filling', 'stir fry', 'stir-fry',
    'shepherdless pie', 'shepherd pie', "shepherd's pie",
    'cottage pie', 'fish pie', 'chicken pie',
    'lasagne', 'lasagna', 'moussaka', 'risotto',
    'curry', 'biryani', 'tikka masala', 'korma', 'jalfrezi',
    'burrito', 'fajita', 'enchilada', 'taco',
    'sushi', 'dim sum', 'spring roll',
    'noodle soup',
]

# ── Supplement indicators ────────────────────────────────────────────────────
SUPPLEMENT_CATEGORIES = {
    'protein powders', 'protein bars', 'protein shakes',
    'meal replacements', 'dietary supplements', 'food supplements',
    'sports drinks', 'energy drinks', 'energy bars',
    'vitamins', 'minerals supplements',
    'weight gainers', 'pre-workout',
    'baby foods', 'baby milks', 'infant formula',
    'slimming products', 'diet shakes',
}

SUPPLEMENT_KEYWORDS = [
    'protein powder', 'protein isolate', 'whey protein',
    'protein bar', 'protein shake', 'meal replacement',
    'diet shake', 'slimming', 'weight gainer',
    'pre workout', 'pre-workout', 'bcaa', 'creatine',
    'baby food', 'infant formula', 'baby milk',
    'multivitamin', 'supplement', 'vitamin tablet',
    'collagen', 'matcha collagen',
]

# ── Single indicators (pure base ingredients) ───────────────────────────────
SINGLE_CATEGORIES = {
    # Dairy
    'milks', 'milk', 'butters', 'butter', 'creams', 'cream',
    'cheddar cheese', 'mozzarella', 'gouda', 'brie', 'camembert',
    'feta', 'stilton', 'parmesan', 'edam', 'gruyere',
    'cream cheese', 'cottage cheese',
    'eggs', 'egg',
    # Produce
    'apples', 'bananas', 'oranges', 'lemons', 'limes',
    'grapes', 'strawberries', 'blueberries', 'raspberries',
    'tomatoes', 'potatoes', 'onions', 'carrots', 'peppers',
    'mushrooms', 'spinach', 'broccoli', 'cauliflower',
    'courgettes', 'aubergines', 'avocados', 'cucumbers',
    'lettuce', 'kale', 'cabbage', 'peas', 'sweetcorn',
    'garlic', 'ginger', 'chillies',
    'fresh fruits', 'fresh vegetables', 'frozen vegetables',
    # Meat & Fish (raw)
    'chickens', 'chicken breasts', 'chicken thighs',
    'beef', 'pork', 'lamb', 'mince', 'steaks',
    'salmon', 'cod', 'tuna', 'prawns', 'haddock', 'mackerel',
    'sardines', 'anchovies', 'trout',
    'smoked salmon',
    # Staples
    'rice', 'pasta', 'noodles', 'couscous',
    'flours', 'flour', 'oats', 'oat',
    'sugars', 'sugar', 'salt', 'salts',
    'olive oils', 'sunflower oils', 'coconut oils',
    'vegetable oils', 'cooking oils', 'oils',
    # Nuts & Seeds
    'almonds', 'walnuts', 'cashews', 'peanuts', 'hazelnuts',
    'pistachios', 'pecans', 'brazil nuts', 'macadamia nuts',
    'chia seeds', 'flaxseeds', 'sunflower seeds', 'pumpkin seeds',
    'sesame seeds', 'mixed nuts', 'nuts',
    # Legumes
    'chickpeas', 'lentils', 'kidney beans', 'black beans',
    'baked beans', 'beans', 'pulses',
    # Spices & Herbs
    'spices', 'herbs', 'dried herbs',
    # Other base
    'honey', 'maple syrup', 'vinegars', 'vinegar',
    'tofu', 'tempeh',
    'coconut milk', 'almond milk', 'oat milk', 'soy milk',
    'water', 'mineral water', 'sparkling water',
    'tea', 'teas', 'coffee', 'coffees',
    'cocoa powder', 'baking powder',
    'dried fruits', 'raisins', 'dates', 'prunes',
}

SINGLE_KEYWORDS = [
    'raw chicken', 'chicken breast', 'chicken thigh',
    'raw beef', 'beef mince', 'steak',
    'raw salmon', 'salmon fillet', 'cod fillet',
    'whole milk', 'semi skimmed milk', 'skimmed milk',
    'olive oil', 'sunflower oil', 'coconut oil', 'rapeseed oil',
    'plain flour', 'self raising flour', 'wholemeal flour',
    'basmati rice', 'long grain rice', 'brown rice',
    'caster sugar', 'granulated sugar', 'brown sugar', 'icing sugar',
    'salted butter', 'unsalted butter',
    'free range eggs', 'large eggs', 'medium eggs',
    'natural yogurt', 'greek yogurt', 'plain yogurt',
    'double cream', 'single cream', 'clotted cream',
    'table salt', 'sea salt', 'rock salt',
    'balsamic vinegar', 'white wine vinegar', 'cider vinegar',
    'soy sauce', 'worcestershire sauce',
]

# ── Category indicators (processed, brand-variable) ─────────────────────────
CATEGORY_CATEGORIES = {
    'biscuits', 'cookies', 'crackers', 'crackers (appetizers)',
    'breads', 'bread', 'rolls', 'bagels', 'naan',
    'cakes', 'muffins', 'pastries', 'croissants', 'doughnuts',
    'yogurts', 'yoghurts', 'fromage frais',
    'cheeses',  # generic cheeses → category (individual types like cheddar → single)
    'cereals', 'breakfast cereals', 'mueslis', 'granola',
    'crisps', 'potato crisps', 'salted snacks', 'salted-snacks',
    'snacks', 'sweet snacks',
    'chocolates', 'chocolate candies', 'dark chocolates', 'milk chocolates',
    'candies', 'confectioneries', 'sweets',
    'ice creams', 'ice cream', 'frozen desserts',
    'sausages', 'bacon', 'ham', 'prepared meats',
    'hummus', 'dips',
    'jams', 'marmalades', 'spreads', 'nut butters', 'peanut butter',
    'ketchup', 'mayonnaise', 'mustard',
    'sauces', 'pasta sauces', 'cooking sauces',
    'juices', 'fruit juices', 'smoothies',
    'sodas', 'soft drinks', 'sweetened beverages',
    'plant-based beverages', 'plant-based foods',
    'meat analogues', 'plant-based meats',
    'canned foods', 'canned vegetables', 'canned fruits',
    'frozen foods',
    'dried pasta',
    'toppings-ingredients',
    'condiments',
    'cereals and their products',
    'beverages',
    'groceries',  # vague OFF catch-all → category as safe default
    'undefined',  # vague OFF catch-all
}

CATEGORY_KEYWORDS = [
    'biscuit', 'cookie', 'cracker', 'wafer',
    'bread', 'roll', 'bagel', 'pitta', 'tortilla',
    'cake', 'muffin', 'brownie', 'flapjack',
    'cereal', 'muesli', 'granola', 'porridge',
    'crisp', 'chip', 'pretzel', 'popcorn',
    'chocolate', 'candy', 'sweet', 'toffee', 'fudge',
    'sausage', 'frankfurter', 'hot dog',
    'ice cream', 'sorbet', 'gelato',
    'jam', 'marmalade', 'spread',
    'sauce', 'ketchup', 'mayo', 'mustard', 'dressing',
    'juice', 'squash', 'cordial', 'smoothie',
    'yogurt', 'yoghurt',
    'hummus', 'houmous',
]


def classify_category(cat_name):
    """Classify an OFF category name into single/category/composite/supplement."""
    if pd.isna(cat_name) or not str(cat_name).strip():
        return None

    cat_lower = str(cat_name).lower().strip()

    # Non-English categories (contain prefix like "fr:", "pt:")
    if ':' in cat_lower:
        return None

    # Exact match against our curated sets (highest priority)
    if cat_lower in SUPPLEMENT_CATEGORIES:
        return 'supplement'
    if cat_lower in COMPOSITE_CATEGORIES:
        return 'composite'
    if cat_lower in SINGLE_CATEGORIES:
        return 'single'
    if cat_lower in CATEGORY_CATEGORIES:
        return 'category'

    # Substring matching for partial hits
    for kw in SUPPLEMENT_KEYWORDS:
        if kw in cat_lower:
            return 'supplement'
    for kw in COMPOSITE_KEYWORDS:
        if kw in cat_lower:
            return 'composite'
    # Single keywords checked before category because single is more specific
    for kw in SINGLE_KEYWORDS:
        if kw in cat_lower:
            return 'single'
    for kw in CATEGORY_KEYWORDS:
        if kw in cat_lower:
            return 'category'

    return None


def classify_product_name(norm_name):
    """
    Fallback classifier using the product norm_name for uncategorized items.
    
    Priority order:
    1. Supplement keywords (highest priority — protein powder is always a supplement)
    2. Composite dish patterns (multi-word phrases that indicate a prepared dish)
    3. Single base-food patterns (only if NO composite indicators present)
    4. Category processed-food keywords
    5. Default → 'category' (safest assumption for unknown branded products)
    """
    if pd.isna(norm_name) or not str(norm_name).strip():
        return 'unknown'

    name_lower = str(norm_name).lower().strip()
    words = name_lower.split()

    # ── 1. Supplements (always override everything) ──
    for kw in SUPPLEMENT_KEYWORDS:
        if kw in name_lower:
            return 'supplement'

    # ── 2. Composite dish detection ──
    # Multi-word patterns that strongly indicate a prepared dish
    COMPOSITE_NAME_PATTERNS = [
        # Dishes with protein + cooking/preparation
        'sandwich', 'wrap', 'panini', 'sub roll',
        'pizza', 'calzone',
        'pie', 'pasty', 'pastie', 'quiche', 'tart',
        'lasagne', 'lasagna', 'moussaka', 'cannelloni',
        'risotto', 'paella', 'biryani', 'pilau',
        'curry', 'korma', 'tikka masala', 'jalfrezi', 'madras', 'vindaloo', 'balti', 'bhuna',
        'stir fry', 'stir-fry', 'chow mein', 'chop suey',
        'burrito', 'fajita', 'enchilada', 'taco', 'quesadilla', 'nacho',
        'sushi', 'dim sum', 'spring roll', 'samosa', 'pakora', 'bhaji',
        'casserole', 'hotpot', 'hot pot', 'stew',
        'soup', 'broth', 'chowder', 'bisque',
        'ravioli', 'tortellini', 'gnocchi',
        'salad',  # prepared salads are composite
        'meal', 'dinner', 'lunch',
        'noodle soup', 'pot noodle', 'cup noodle',
        'cheesecake', 'tiramisu', 'trifle', 'pavlova', 'gateau',
        'corndog', 'corn dog',
        # Multi-word composite indicators
        'with sauce', 'in sauce', 'in gravy',
        'with rice', 'with chips', 'with noodles', 'with pasta',
        'topped with', 'stuffed with', 'filled with',
        'served with',
    ]

    for pattern in COMPOSITE_NAME_PATTERNS:
        if pattern in name_lower:
            # But exclude things like "curry powder", "curry paste", "pie crust"
            false_composite = ['curry powder', 'curry paste', 'curry sauce',
                               'pie crust', 'pie shell', 'pastry case',
                               'soup mix', 'salad leaf', 'salad leaves',
                               'pizza base', 'pizza dough',
                               'taco shell', 'taco seasoning',
                               'sandwich bread', 'sandwich thins']
            if not any(fc in name_lower for fc in false_composite):
                return 'composite'

    # ── 3. Single base-food detection ──
    # These are very specific multi-word patterns that strongly indicate a pure ingredient.
    # BUT: if the name also contains processing/cooking indicators, it's NOT a single.
    SINGLE_DISQUALIFIERS = [
        'battered', 'breaded', 'coated', 'crumbed', 'crusted',
        'flavoured', 'flavored', 'seasoned', 'marinated', 'glazed',
        'stuffed', 'filled', 'topped', 'loaded',
        'dippers', 'nuggets', 'goujons', 'strips', 'bites', 'poppers',
        'kebab', 'skewer', 'burger', 'patty', 'pattie',
        'cake', 'pie', 'roll', 'pasty',
        'croquette', 'fritter', 'tempura',
        'kiev', 'parmigiana', 'schnitzel', 'escalope',
        'spread', 'paste', 'pate',
        'bar ', ' bar', 'balls', 'fingers',
        'mix', 'medley', 'selection',
        'and ', ' and ',  # "sweetcorn and red pepper" = composite
        'with ',  # "chicken with herbs" = composite
        'in ',    # "tuna in brine" = processed but OK... only block multi-ingredient
    ]
    has_disqualifier = any(dq in name_lower for dq in SINGLE_DISQUALIFIERS)
    SINGLE_NAME_PATTERNS = [
        # Dairy
        'whole milk', 'semi skimmed milk', 'skimmed milk', '1% milk',
        'salted butter', 'unsalted butter', 'butter block',
        'double cream', 'single cream', 'whipping cream', 'clotted cream', 'soured cream',
        'free range eggs', 'large eggs', 'medium eggs', 'barn eggs',
        'cheddar', 'mozzarella', 'parmesan', 'stilton', 'brie', 'camembert',
        'gouda', 'edam', 'gruyere', 'feta', 'halloumi', 'mascarpone', 'ricotta',
        'cream cheese', 'cottage cheese', 'red leicester', 'wensleydale',
        'natural yogurt', 'greek yogurt', 'plain yogurt',
        # Meat — raw cuts
        'chicken breast', 'chicken thigh', 'chicken drumstick', 'chicken wing',
        'whole chicken', 'chicken fillet', 'diced chicken', 'chicken legs',
        'beef mince', 'steak', 'beef joint', 'sirloin', 'ribeye', 'rump',
        'pork chop', 'pork loin', 'pork belly', 'pork joint', 'pork ribs',
        'pork mince', 'diced pork',
        'lamb chop', 'lamb leg', 'lamb mince', 'diced lamb', 'lamb shank',
        'turkey breast', 'turkey mince', 'duck breast',
        # Fish — raw/smoked
        'salmon fillet', 'cod fillet', 'haddock fillet', 'sea bass fillet',
        'tuna steak', 'mackerel fillet', 'trout fillet', 'plaice fillet',
        'smoked salmon', 'smoked haddock', 'smoked mackerel',
        'raw prawns', 'king prawns', 'tiger prawns',
        # Staples
        'plain flour', 'self raising flour', 'wholemeal flour', 'strong bread flour',
        'basmati rice', 'long grain rice', 'brown rice', 'jasmine rice', 'risotto rice',
        'caster sugar', 'granulated sugar', 'brown sugar', 'icing sugar', 'demerara sugar',
        'olive oil', 'sunflower oil', 'coconut oil', 'rapeseed oil', 'vegetable oil',
        'sea salt', 'rock salt', 'table salt',
        'balsamic vinegar', 'white wine vinegar', 'cider vinegar', 'malt vinegar',
        'soy sauce', 'worcestershire sauce',
        'dried pasta', 'spaghetti', 'penne', 'fusilli', 'tagliatelle', 'linguine',
        'egg noodles', 'rice noodles', 'udon noodles',
        'porridge oats', 'rolled oats', 'jumbo oats',
        'baking powder', 'bicarbonate of soda', 'cocoa powder',
        'coconut milk', 'almond milk', 'oat milk', 'soy milk', 'soya milk',
        # Produce
        'chopped tomatoes', 'tinned tomatoes', 'cherry tomatoes', 'plum tomatoes',
        'baked beans', 'cannellini beans', 'kidney beans', 'black beans', 'butter beans',
        'garden peas', 'frozen peas', 'mushy peas', 'marrowfat peas',
        'chickpeas', 'red lentils', 'green lentils', 'puy lentils',
        'frozen sweetcorn', 'sweetcorn',
        'pitted olives', 'green olives', 'black olives',
        # Nuts & Seeds
        'cashew nut', 'almond', 'walnut', 'peanut', 'hazelnut',
        'mixed nuts', 'cashew', 'pistachio', 'pecan', 'brazil nut',
        'chia seed', 'pumpkin seed', 'sunflower seed', 'sesame seed',
        'peanut butter',  # borderline but functionally single
        'almond butter', 'cashew butter',
        # Drinks — base
        'mineral water', 'sparkling water', 'still water',
        'green tea', 'black tea', 'herbal tea', 'rooibos',
        'ground coffee', 'instant coffee', 'coffee bean',
        # Other base
        'honey', 'maple syrup', 'golden syrup', 'treacle',
        'tofu', 'tempeh',
        'dried fruit', 'raisins', 'dates', 'prunes', 'dried apricot',
        'desiccated coconut', 'coconut cream',
    ]

    if not has_disqualifier:
        for pattern in SINGLE_NAME_PATTERNS:
            if pattern in name_lower:
                return 'single'

    # ── 4. Category processed-food keywords ──
    # Broader single-word matches that indicate a processed branded product
    CATEGORY_NAME_PATTERNS = [
        'biscuit', 'cookie', 'cracker', 'wafer', 'shortbread',
        'bread', 'bap', 'bagel', 'pitta', 'tortilla', 'naan', 'flatbread', 'crumpet',
        'cake', 'muffin', 'brownie', 'flapjack', 'scone', 'doughnut', 'donut',
        'cereal', 'muesli', 'granola',
        'crisp', 'pretzel', 'popcorn',
        'chocolate', 'candy', 'toffee', 'fudge', 'marshmallow',
        'sausage', 'frankfurter',
        'ice cream', 'sorbet', 'gelato', 'lolly', 'lollies',
        'jam', 'marmalade', 'jelly',
        'ketchup', 'mayo', 'mayonnaise', 'mustard', 'dressing', 'relish',
        'juice', 'squash', 'cordial', 'smoothie', 'milkshake',
        'yogurt', 'yoghurt',
        'hummus', 'houmous', 'tzatziki', 'guacamole', 'salsa',
        'bacon', 'ham', 'chorizo', 'salami', 'pepperoni', 'prosciutto',
        'cola', 'lemonade', 'tonic', 'ginger beer', 'soda',
        'cider', 'beer', 'lager', 'ale', 'wine', 'vodka', 'gin', 'whisky', 'rum',
        'spread', 'paste',
        'soup',  # if got past the composite check above, it's a packaged soup
    ]

    for pattern in CATEGORY_NAME_PATTERNS:
        if pattern in name_lower:
            return 'category'

    # ── 5. Default ──
    return 'category'


def run_phase1():
    logger.info("=" * 60)
    logger.info("OFF GOLD PHASE 1: Category Cleanup & Food Type Classification")
    logger.info("=" * 60)

    if not SILVER_PATH.exists():
        logger.error(f"Cannot find {SILVER_PATH}")
        return

    logger.info(f"Loading {SILVER_PATH}...")
    df = pd.read_csv(SILVER_PATH, low_memory=False)
    logger.info(f"  Loaded: {len(df):,} UK products")

    # ── Step 1: Classify by category name ──
    logger.info("\nStep 1: Classifying by OFF category name...")

    unique_cats = df['main_category_en'].dropna().unique()
    logger.info(f"  Unique categories: {len(unique_cats):,}")

    cat_labels = {}
    for cat in unique_cats:
        label = classify_category(cat)
        if label:
            cat_labels[cat] = label

    # Cache category labels
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_df = pd.DataFrame(list(cat_labels.items()), columns=['category', 'food_type_label'])
    cache_df.to_csv(CACHE_PATH, index=False)
    logger.info(f"  Cached {len(cat_labels):,} category labels to {CACHE_PATH}")

    # Apply category-based labels
    df['food_type_label'] = df['main_category_en'].map(cat_labels)

    labeled_by_cat = df['food_type_label'].notna().sum()
    logger.info(f"  Labeled by category: {labeled_by_cat:,} / {len(df):,}")

    # ── Step 2: Classify remaining by product name ──
    unlabeled_mask = df['food_type_label'].isna()
    unlabeled_count = unlabeled_mask.sum()
    logger.info(f"\nStep 2: Classifying {unlabeled_count:,} unlabeled products by name...")

    df.loc[unlabeled_mask, 'food_type_label'] = df.loc[unlabeled_mask, 'norm_name'].apply(classify_product_name)

    # ── Report ──
    logger.info("\n" + "=" * 60)
    logger.info("CLASSIFICATION RESULTS")
    logger.info("=" * 60)

    counts = df['food_type_label'].value_counts()
    for label, count in counts.items():
        pct = count / len(df) * 100
        logger.info(f"  {label:<15} {count:>7,}  ({pct:.1f}%)")

    # ── Step 3: Save ──
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    logger.info(f"\nSaved: {OUTPUT_PATH}")
    logger.info(f"Total: {len(df):,} rows")

    # ── Spot check ──
    logger.info("\n── Spot Check: Random samples per label ──")
    for label in ['single', 'category', 'composite', 'supplement']:
        subset = df[df['food_type_label'] == label]
        if len(subset) > 0:
            sample = subset.sample(min(5, len(subset)), random_state=42)
            logger.info(f"\n  [{label.upper()}] ({len(subset):,} total)")
            for _, r in sample.iterrows():
                cat = str(r['main_category_en'])[:25] if pd.notna(r['main_category_en']) else 'NO_CAT'
                logger.info(f"    {str(r['norm_name'])[:55]:<55}  cat={cat}")


if __name__ == '__main__':
    run_phase1()
