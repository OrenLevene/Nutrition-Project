"""
Product Matcher - Maps OFF products to canonical foods with deduplication.

Key features:
- Extract sizes from product names (e.g., "400g", "1kg")
- Match OFF products to canonical foods by name similarity
- Deduplicate: max 3 size variants per canonical food
- Flag suspicious entries (>3 sizes, brand mismatches)
"""
import re
import json
import pandas as pd
from typing import List, Dict, Tuple, Optional
from pathlib import Path
from difflib import SequenceMatcher
from rapidfuzz import fuzz


# Regex patterns for size extraction
# Order matters: more specific patterns first
# Use word boundary (\b) to avoid matching middle of words
SIZE_PATTERNS = [
    r'(\d+)\s*x\s*(\d+)\s*g\b',       # 4x100g (multipacks) - match first
    r'(\d+(?:\.\d+)?)\s*kg\b',         # 1kg, 1.5kg
    r'(\d+(?:\.\d+)?)\s*g\b',          # 400g, 500g (word boundary avoids "garlic")
    r'(\d+(?:\.\d+)?)\s*ml\b',         # 500ml, 1000ml
    r'(\d+(?:\.\d+)?)\s*litre',        # 1 litre, 2 litres
    r'(\d+(?:\.\d+)?)\s*l\b',          # 1l, 2l (word boundary avoids "lamb")
    r'(\d+)\s*pack\b',                 # 6 pack
    r'(\d+)\s*pint',                   # 2 pints
]

# Default pack sizes in grams by food category keyword
# These are typical UK supermarket sizes (multiple options per category)
# Note: These are PACK sizes, not serving sizes - can be split across week
DEFAULT_PACK_SIZES = {
    # Proteins
    'chicken': [300, 500, 1000],      # Small, medium, large packs
    'beef': [250, 500, 750],          # Mince packs
    'pork': [300, 500],
    'lamb': [400, 500],
    'sausage': [400, 454],            # 6-pack, 8-pack
    'bacon': [200, 300],
    'fish': [280, 400],
    'salmon': [200, 280, 400],
    'tuna': [160, 200],               # Tins
    'eggs': [360, 720],               # 6 eggs, 12 eggs
    
    # Dairy
    'milk': [568, 1136, 2272, 3408],  # 1, 2, 4, 6 pints
    'cheese': [200, 400, 550],        # Small, medium, large blocks
    'yogurt': [125, 450, 500],        # Single, multipot, large
    'butter': [250, 500],
    'cream': [150, 300, 600],
    
    # Grains
    'bread': [400, 800],              # Small, standard loaf
    'pasta': [500, 1000],
    'rice': [500, 1000, 2000],
    'cereal': [375, 500, 750],
    'oats': [500, 1000],
    'flour': [500, 1500],
    
    # Vegetables
    'potato': [1000, 2500],
    'carrot': [500, 1000],
    'onion': [500, 1000],
    'tomato': [250, 400],
    'broccoli': [200, 350],
    'spinach': [100, 200],
    
    # Fruits
    'apple': [500, 600],
    'banana': [500],
    'orange': [500, 800],
    
    # Legumes
    'beans': [400, 415],              # Standard tin sizes
    'lentils': [400, 500],
    'chickpeas': [400],
    
    # Default for unknown
    '_default': [400],
}


def get_default_pack_sizes(product_name: str, category: str = None) -> List[int]:
    """
    Get default pack sizes based on product name or category.
    Returns list of sizes in grams.
    """
    search_text = f"{product_name or ''} {category or ''}".lower()
    
    for keyword, sizes in DEFAULT_PACK_SIZES.items():
        if keyword != '_default' and keyword in search_text:
            return sizes
    
    return DEFAULT_PACK_SIZES['_default']


def get_default_pack_size(product_name: str, category: str = None) -> int:
    """
    Get the most common (first) default pack size.
    Returns size in grams.
    """
    sizes = get_default_pack_sizes(product_name, category)
    return sizes[0] if sizes else 400


def extract_size(product_name) -> Optional[str]:
    """
    Extract size from product name.
    
    Examples:
        "Heinz Baked Beans 400g" -> "400g"
        "Milk 2 Pints" -> "2 pints"
        "Eggs 6 pack" -> "6 pack"
    """
    if not product_name or pd.isna(product_name):
        return None
    
    name_lower = str(product_name).lower()
    
    # Try each pattern
    for pattern in SIZE_PATTERNS:
        match = re.search(pattern, name_lower)
        if match:
            return match.group(0)
    
    # Check for pints (common in UK)
    pint_match = re.search(r'(\d+)\s*pint', name_lower)
    if pint_match:
        return f"{pint_match.group(1)} pint"
    
    return None


def normalize_size_to_grams(size_str: str) -> Optional[float]:
    """
    Convert size string to grams for comparison.
    
    Examples:
        "400g" -> 400.0
        "1kg" -> 1000.0
        "500ml" -> 500.0 (assumes 1ml ≈ 1g for liquids)
    """
    if not size_str:
        return None
    
    size_lower = size_str.lower().strip()
    
    # Kilograms
    kg_match = re.search(r'(\d+(?:\.\d+)?)\s*kg', size_lower)
    if kg_match:
        return float(kg_match.group(1)) * 1000
    
    # Grams
    g_match = re.search(r'(\d+(?:\.\d+)?)\s*g', size_lower)
    if g_match:
        return float(g_match.group(1))
    
    # Milliliters (treat as grams for simplicity)
    ml_match = re.search(r'(\d+(?:\.\d+)?)\s*ml', size_lower)
    if ml_match:
        return float(ml_match.group(1))
    
    # Liters
    l_match = re.search(r'(\d+(?:\.\d+)?)\s*l', size_lower)
    if l_match:
        return float(l_match.group(1)) * 1000
    
    return None


def extract_brand_and_product(full_name) -> Tuple[str, str]:
    """
    Attempt to separate brand from product name.
    
    Returns: (brand, product_name)
    """
    if not full_name or pd.isna(full_name):
        return ("", "")
    
    full_name = str(full_name)
    
    # Common pattern: "Brand - Product" or "Brand Product"
    if ' - ' in full_name:
        parts = full_name.split(' - ', 1)
        return (parts[0].strip(), parts[1].strip() if len(parts) > 1 else "")
    
    # Otherwise return as-is
    return ("", full_name)


def similarity_score(s1: str, s2: str) -> float:
    """Calculate string similarity (0-1)."""
    if not s1 or not s2:
        return 0.0
    return SequenceMatcher(None, s1.lower(), s2.lower()).ratio()


class ProductMatcher:
    """
    Matches OFF products to canonical foods with deduplication.
    """
    
    def __init__(self, max_sizes_per_food: int = 3, similarity_threshold: float = 0.7):
        self.max_sizes = max_sizes_per_food
        self.similarity_threshold = similarity_threshold
        self.suspicious_products = []
    
    def process_off_products(self, off_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process OFF products: extract sizes, deduplicate, create clean mapping.
        
        Args:
            off_df: DataFrame with OFF products (must have 'product_name', 'brands' columns)
            
        Returns:
            DataFrame with deduplicated products and extracted sizes
        """
        print(f"Processing {len(off_df)} OFF products...")
        
        # Extract sizes
        off_df = off_df.copy()
        off_df['extracted_size'] = off_df['product_name'].apply(extract_size)
        off_df['size_grams'] = off_df['extracted_size'].apply(normalize_size_to_grams)
        
        # Separate brand and product
        brand_product = off_df['product_name'].apply(extract_brand_and_product)
        off_df['extracted_brand'] = brand_product.apply(lambda x: x[0])
        off_df['clean_product_name'] = brand_product.apply(lambda x: x[1])
        
        # Use provided brand if extracted is empty
        off_df['final_brand'] = off_df.apply(
            lambda row: row['extracted_brand'] if row['extracted_brand'] else 
                       (row['brands'] if pd.notna(row.get('brands')) else ''),
            axis=1
        )
        
        # Create grouping key (brand + base product, without size)
        off_df['group_key'] = off_df.apply(self._create_group_key, axis=1)
        
        print(f"  Products with extracted size: {off_df['extracted_size'].notna().sum()}")
        print(f"  Unique group keys: {off_df['group_key'].nunique()}")
        
        # Deduplicate within groups
        result = self._deduplicate(off_df)
        
        print(f"  After deduplication: {len(result)} products")
        print(f"  Flagged suspicious: {len(self.suspicious_products)}")
        
        return result
    
    def _create_group_key(self, row) -> str:
        """Create a grouping key for deduplication."""
        brand = str(row.get('final_brand', '')).lower().strip()
        product = str(row.get('clean_product_name', row.get('product_name', ''))).lower()
        
        # Remove size info from product name for grouping
        product = re.sub(r'\d+(?:\.\d+)?\s*(kg|g|ml|l|pint)\b', '', product)
        product = re.sub(r'\s+', ' ', product).strip()
        
        return f"{brand}::{product}"
    
    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Deduplicate products, keeping max N sizes per group.
        """
        result_rows = []
        
        for group_key, group in df.groupby('group_key'):
            sizes = group['size_grams'].dropna().unique()
            
            if len(sizes) > self.max_sizes:
                # Flag as suspicious
                self.suspicious_products.append({
                    'group_key': group_key,
                    'num_sizes': len(sizes),
                    'sizes': list(sizes),
                    'sample_products': group['product_name'].head(3).tolist()
                })
                
                # Keep only top N most common sizes
                size_counts = group['size_grams'].value_counts()
                top_sizes = size_counts.head(self.max_sizes).index.tolist()
                group = group[group['size_grams'].isin(top_sizes)]
            
            # For each size, keep one representative product
            for size in group['size_grams'].unique():
                size_group = group[group['size_grams'] == size]
                if len(size_group) > 0:
                    # Prefer products with more complete data
                    best = size_group.iloc[0]  # Could add smarter selection
                    result_rows.append(best)
            
            # Also keep products without size info (first one per group)
            no_size = group[group['size_grams'].isna()]
            if len(no_size) > 0 and len(result_rows) == 0:
                result_rows.append(no_size.iloc[0])
        
        return pd.DataFrame(result_rows)
    
    def match_to_canonical(self, off_products: pd.DataFrame, 
                          canonical_foods: List,
                          min_keyword_matches: int = 2) -> pd.DataFrame:
        """
        Match OFF products to canonical foods using keyword-based matching.
        
        Much faster than O(n²) string similarity - uses inverted keyword index.
        
        Args:
            off_products: DataFrame with 'product_name' column
            canonical_foods: List of FoodItem objects from FoodDatabase
            min_keyword_matches: Minimum keywords that must match
            
        Returns:
            DataFrame with matches: off_id, canonical_id, canonical_name, 
                                   keywords_matched, size, brand
        """
        # Build keyword index from canonical foods
        print("Building canonical food keyword index...")
        keyword_index = {}  # keyword -> list of (food_id, food_name)
        
        for food in canonical_foods:
            # Extract keywords from food name
            keywords = self._extract_keywords(food.name)
            for kw in keywords:
                if kw not in keyword_index:
                    keyword_index[kw] = []
                keyword_index[kw].append((food.id, food.name))
        
        print(f"  Index contains {len(keyword_index)} unique keywords")
        
        # Match OFF products
        print("Matching OFF products to canonical foods...")
        matches = []
        matched_count = 0
        
        for _, off_row in off_products.iterrows():
            off_name = str(off_row.get('product_name', ''))
            off_keywords = self._extract_keywords(off_name)
            
            # Find candidate matches based on keyword overlap
            candidates = {}  # canonical_id -> (name, keyword_count)
            for kw in off_keywords:
                if kw in keyword_index:
                    for food_id, food_name in keyword_index[kw]:
                        if food_id not in candidates:
                            candidates[food_id] = {'name': food_name, 'count': 0, 'keywords': []}
                        candidates[food_id]['count'] += 1
                        candidates[food_id]['keywords'].append(kw)
            
            # Find best match (most keyword overlap)
            if candidates:
                best_id = max(candidates.keys(), key=lambda x: candidates[x]['count'])
                best = candidates[best_id]
                
                if best['count'] >= min_keyword_matches:
                    matches.append({
                        'off_id': off_row.get('code', ''),
                        'off_name': off_name,
                        'canonical_id': best_id,
                        'canonical_name': best['name'],
                        'keywords_matched': best['count'],
                        'matching_keywords': best['keywords'],
                        'size': off_row.get('extracted_size', ''),
                        'brand': off_row.get('final_brand', ''),
                    })
                    matched_count += 1
        
        print(f"  Matched {matched_count} of {len(off_products)} products")
        return pd.DataFrame(matches)
    
    def _extract_keywords(self, text: str) -> List[str]:
        """
        Extract meaningful keywords from food name.
        Removes common filler words and normalizes.
        """
        if not text or pd.isna(text):
            return []
        
        # Normalize
        text = str(text).lower()
        
        # Remove size info
        text = re.sub(r'\d+(?:\.\d+)?\s*(kg|g|ml|l|pint|pack)\b', '', text)
        
        # Remove punctuation and split
        text = re.sub(r'[^\w\s]', ' ', text)
        words = text.split()
        
        # Filter out common filler words
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'with', 'without', 'in', 'on',
            'raw', 'cooked', 'fresh', 'frozen', 'canned', 'dried',
            'packed', 'drained', 'added', 'prepared', 'made', 'from',
            'whole', 'sliced', 'chopped', 'ground', 'minced',
            'organic', 'natural', 'free', 'range', 'british',
            'tesco', 'sainsbury', 'asda', 'morrisons', 'waitrose', 'aldi', 'lidl',
        }
        
        keywords = [w for w in words if len(w) > 2 and w not in stop_words]
        return keywords
    
    def get_suspicious_products(self) -> List[Dict]:
        """Return list of suspicious products for review."""
        return self.suspicious_products
    
    def fuzzy_match_canonical(
        self, 
        off_products: pd.DataFrame, 
        canonical_foods: List,
        config_path: str = "data/config/off_category_mapping.json",
        fuzzy_threshold: float = 70.0,
        min_keyword_matches: int = 1
    ) -> pd.DataFrame:
        """
        Match OFF products to canonical foods using improved strategy:
        1. Check manual mappings first (highest priority)
        2. Filter by category (reduces search space by ~95%)
        3. Use rapidfuzz token_set_ratio for fuzzy matching
        
        Args:
            off_products: DataFrame with OFF products
            canonical_foods: List of canonical FoodItem objects
            config_path: Path to category mapping config
            fuzzy_threshold: Minimum fuzzy match score (0-100)
            min_keyword_matches: Fallback to keyword matching if fuzzy fails
            
        Returns:
            DataFrame with matched products
        """
        # Load category config
        config = {}
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
        except Exception as e:
            print(f"Warning: Could not load category config: {e}")
        
        manual_mappings = config.get("_top_100_manual_mappings", {})
        
        # Build canonical name lookup
        canonical_by_name = {f.name.lower(): f for f in canonical_foods}
        canonical_list = [(f.id, f.name, f.name.lower()) for f in canonical_foods]
        
        matches = []
        matched_count = 0
        manual_match_count = 0
        fuzzy_match_count = 0
        
        for idx, row in off_products.iterrows():
            off_name = str(row.get('product_name', '')).lower()
            off_id = row.get('code', idx)
            off_brand = str(row.get('brands', ''))
            off_category = str(row.get('pnns_groups_2', ''))
            
            if not off_name or off_name == 'nan':
                continue
            
            best_match = None
            best_score = 0
            match_type = None
            
            # Strategy 1: Check manual mappings
            for canonical_name, aliases in manual_mappings.items():
                for alias in aliases:
                    if alias.lower() in off_name:
                        if canonical_name.lower() in canonical_by_name:
                            food = canonical_by_name[canonical_name.lower()]
                            best_match = (food.id, food.name)
                            best_score = 100
                            match_type = "manual"
                            break
                if best_match:
                    break
            
            # Strategy 2: Fuzzy matching (if no manual match)
            if not best_match:
                for can_id, can_name, can_name_lower in canonical_list:
                    # Use token_set_ratio - handles word order differences
                    score = fuzz.token_set_ratio(off_name, can_name_lower)
                    
                    if score > best_score and score >= fuzzy_threshold:
                        best_score = score
                        best_match = (can_id, can_name)
                        match_type = "fuzzy"
            
            # Record match
            if best_match:
                size = extract_size(row.get('product_name', ''))
                matches.append({
                    'off_id': off_id,
                    'off_name': row.get('product_name', ''),
                    'canonical_id': best_match[0],
                    'canonical_name': best_match[1],
                    'match_score': best_score,
                    'match_type': match_type,
                    'size': size,
                    'brand': off_brand
                })
                matched_count += 1
                if match_type == "manual":
                    manual_match_count += 1
                else:
                    fuzzy_match_count += 1
        
        print(f"  Matched {matched_count} of {len(off_products)} products")
        print(f"    - Manual mappings: {manual_match_count}")
        print(f"    - Fuzzy matches: {fuzzy_match_count}")
        return pd.DataFrame(matches)


def main():
    """Test the product matcher with a small sample."""
    # Test size extraction
    test_names = [
        "Heinz Baked Beans 400g",
        "Tesco Semi Skimmed Milk 2 Pints",
        "Coca Cola 1.5L",
        "Walkers Crisps 6 x 25g",
        "Organic Eggs Free Range",
        "Cathedral City Cheddar 350g",
    ]
    
    print("=== Size Extraction Test ===")
    for name in test_names:
        size = extract_size(name)
        grams = normalize_size_to_grams(size) if size else None
        print(f"  {name}")
        print(f"    -> Size: {size}, Grams: {grams}")
    
    print("\n=== Product Matcher Ready ===")
    print("Use ProductMatcher.process_off_products() to process OFF data")


if __name__ == '__main__':
    main()
