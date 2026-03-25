"""
Store Product Lookup - Maps canonical foods to store products.

Provides lookup functionality to find store-specific products
for canonical foods from the optimization output.
"""
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional


class StoreProductLookup:
    """
    Lookup store products for canonical foods.
    
    Usage:
        lookup = StoreProductLookup()
        products = lookup.get_store_products('Beans, baked')
        # Returns list of store products with brands/sizes
    """
    
    def __init__(self, mapping_file: str = None):
        if mapping_file is None:
            mapping_file = Path(__file__).parent.parent.parent / 'data' / 'processed' / 'store_products_mapping.parquet'
        
        self.mapping_file = Path(mapping_file)
        self._df = None
        self._canonical_index = None
    
    def _load(self):
        """Lazy load the mapping data."""
        if self._df is None:
            if self.mapping_file.exists():
                self._df = pd.read_parquet(self.mapping_file)
                # Build index by canonical_id
                self._canonical_index = self._df.groupby('canonical_id').apply(
                    lambda x: x.to_dict('records')
                ).to_dict()
            else:
                self._df = pd.DataFrame()
                self._canonical_index = {}
    
    def get_store_products(self, canonical_id: str) -> List[Dict]:
        """
        Get all store products matching a canonical food ID.
        
        Args:
            canonical_id: The canonical food ID from FooDB
            
        Returns:
            List of dicts with store product info (brand, name, size)
        """
        self._load()
        return self._canonical_index.get(canonical_id, [])
    
    def get_store_products_by_name(self, canonical_name: str) -> List[Dict]:
        """
        Get store products by searching canonical food name.
        
        Args:
            canonical_name: Partial name to search for
            
        Returns:
            List of matching store products
        """
        self._load()
        if self._df.empty:
            return []
        
        # Find matching canonical IDs
        mask = self._df['canonical_name'].str.contains(canonical_name, case=False, na=False)
        matches = self._df[mask]
        return matches.to_dict('records')
    
    def has_store_products(self, canonical_id: str) -> bool:
        """Check if a canonical food has any store products."""
        self._load()
        return canonical_id in self._canonical_index
    
    def get_covered_canonical_ids(self) -> List[str]:
        """Get list of all canonical IDs that have store products."""
        self._load()
        return list(self._canonical_index.keys())
    
    def get_stats(self) -> Dict:
        """Get statistics about the store product mapping."""
        self._load()
        if self._df.empty:
            return {'total_products': 0, 'unique_canonicals': 0}
        
        return {
            'total_products': len(self._df),
            'unique_canonicals': len(self._canonical_index),
            'avg_products_per_canonical': len(self._df) / len(self._canonical_index) if self._canonical_index else 0,
        }


def main():
    """Test the store product lookup."""
    lookup = StoreProductLookup()
    stats = lookup.get_stats()
    
    print("Store Product Lookup Stats")
    print("=" * 40)
    print(f"Total store products: {stats['total_products']}")
    print(f"Unique canonical foods: {stats['unique_canonicals']}")
    print(f"Avg products per canonical: {stats['avg_products_per_canonical']:.1f}")
    
    # Test search
    print("\nSearching for 'cheese' products:")
    cheese = lookup.get_store_products_by_name('cheese')
    for p in cheese[:5]:
        print(f"  {p['off_name'][:40]} -> {p['canonical_name'][:30]}")


if __name__ == '__main__':
    main()
