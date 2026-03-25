from pydantic import BaseModel
from typing import Optional, List, Dict, Any

class UserProfile(BaseModel):
    age: int
    gender: str  # "male" or "female"
    activity_level: str  # e.g., "sedentary", "active"
    weight_kg: float
    height_cm: float
    goal: str = "maintenance" # "loss", "maintenance", "gain"

class NutrientRange(BaseModel):
    nutrient_name: str
    min_value: float
    max_value: Optional[float] = None
    unit: str

class CalculationResult(BaseModel):
    calories: float
    calories_min: float  # -5%
    calories_max: float  # +5%
    protein_g: float
    protein_min: float   # -10%
    protein_max: float   # +10%
    carbs_g: float
    carbs_min: float
    carbs_max: float
    fats_g: float
    fats_min: float
    fats_max: float
    micronutrients: List[NutrientRange]

# New model for nutrient analysis in optimization results
class NutrientAnalysis(BaseModel):
    """Analysis of a single nutrient's actual vs target values."""
    nutrient_name: str
    actual: float              # Daily average (weekly_total / days)
    daily_target: Optional[float] = None   # Target per day
    weekly_target: Optional[float] = None  # daily_target × days
    min_target: Optional[float] = None     # Min allowed (with tolerance)
    max_target: Optional[float] = None     # Max allowed (with tolerance)
    unit: str = ""
    status: str  # "in_range", "below_min", "above_max"
    error_percent: Optional[float] = None  # % error if out of range

class OptimizationResult(BaseModel):
    status: str
    days: int = 7                          # Number of days optimized for
    selected_foods: Any                    # Dict[Name, Amount] or List[Dict]
    selected_portions: Dict[str, Any] = {} # Food Name -> {packs/grams, mode, etc.}
    totals: Dict[str, Any]                 # {weekly: {...}, daily_average: {...}}
    nutrient_analysis: List[NutrientAnalysis] = []  # Full breakdown with range compliance
    warnings: List[str] = []
    mode: str = "hybrid"                   # "hybrid", "continuous", or "discrete"
    infeasibility_analysis: Optional[Dict[str, Any]] = None  # Why optimization failed
    supplement_recommended: List[Dict[str, Any]] = []  # Nutrients with insufficient data coverage
    
    # GA Specific fields
    daily_plans: List[Dict[str, Any]] = [] # Detailed day-by-day plan
    max_trio_repetitions: Optional[int] = None
    unique_foods: Optional[int] = None

