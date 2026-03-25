"""
Utility for resolving portion sizes for foods based on name/category patterns.
"""
import json
import os
from typing import Optional

DEFAULT_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
    "data", "config", "portion_sizes.json"
)

class PortionSizeResolver:
    """Resolves portion sizes for foods based on name matching patterns."""
    
    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH):
        self.config = self._load_config(config_path)
        self.default_portion = self.config.get("default_portion_size", 100)
        self.patterns = self.config.get("food_patterns", {})
    
    def _load_config(self, path: str) -> dict:
        if not os.path.exists(path):
            return {"default_portion_size": 100, "food_patterns": {}}
        with open(path, 'r') as f:
            return json.load(f)
    
    def get_portion_size(self, food_name: str) -> float:
        """
        Get the portion size for a food based on name pattern matching.
        Returns default (100g) if no pattern matches.
        """
        info = self.get_portion_info(food_name)
        return info["pack_size"]
    
    def get_portion_info(self, food_name: str) -> dict:
        """
        Get full portion info for a food.
        Returns: {"pack_size": float, "is_perishable": bool}
        
        Lookup order:
        1. Exact match in "foods" dict (for hardcoded special cases)
        2. Pattern match in "food_patterns" dict
        3. Default values
        
        - pack_size: grams per purchaseable unit (e.g., 500g chicken pack)
        - is_perishable: True = discrete mode (integer packs), False = continuous (grams)
        """
        food_name_lower = food_name.lower()
        default_is_perishable = self.config.get("default_is_perishable", False)
        
        # 1. Check exact match in "foods" dict first
        exact_foods = self.config.get("foods", {})
        if food_name in exact_foods:
            info = exact_foods[food_name]
            return {
                "pack_size": float(info.get("pack_size", self.default_portion)),
                "is_perishable": info.get("is_perishable", default_is_perishable)
            }
        
        # 2. Check pattern matching (longer patterns first for specificity)
        sorted_patterns = sorted(self.patterns.items(), key=lambda x: len(x[0]), reverse=True)
        
        for pattern, info in sorted_patterns:
            if pattern.lower() in food_name_lower:
                if isinstance(info, dict):
                    return {
                        "pack_size": float(info.get("pack_size", self.default_portion)),
                        "is_perishable": info.get("is_perishable", default_is_perishable)
                    }
                else:
                    # Legacy format: just a number (treat as continuous)
                    return {"pack_size": float(info), "is_perishable": False}
        
        # 3. Default
        return {"pack_size": float(self.default_portion), "is_perishable": default_is_perishable}

# Singleton instance for convenience
_resolver_instance: Optional[PortionSizeResolver] = None

def get_resolver() -> PortionSizeResolver:
    """Get or create the singleton PortionSizeResolver instance."""
    global _resolver_instance
    if _resolver_instance is None:
        _resolver_instance = PortionSizeResolver()
    return _resolver_instance

def resolve_portion_size(food_name: str) -> float:
    """Convenience function to get portion size for a food name."""
    return get_resolver().get_portion_size(food_name)
