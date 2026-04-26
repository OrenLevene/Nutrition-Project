import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Setup paths to import shared utils
DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(DATA_PIPELINE_DIR / 'pipeline' / 'utils'))
from reference_cleaning import clean_nutrient_columns, standardize_column_names, sort_columns, enforce_uniform_schema

class BaseNutritionCleaner:
    """
    Base class for standardizing nutrition datasets across different data sources.
    Provides an inherited structure that avoids duplicate code across processing layers.
    """
    def __init__(self, source_name="Unknown"):
        self.source_name = source_name

    def extract_weights(self, df):
        """Override this method to extract 'Total Weight (g)' from the source-specific serving logic."""
        return df

    def apply_taxonomy(self, df):
        """Override this method to apply source-specific taxonomies to 'semantic_descriptor'."""
        df['semantic_descriptor'] = 'unknown | unknown | unknown'
        return df

    def pre_process(self, df):
        """General pre-processing hook for custom operations."""
        return df

    def execute(self, df):
        """
        The standardized execution pipeline.
        Runs all custom hooks then strictly enforces reference standards.
        """
        df = df.copy()
        
        # 1. Custom Dataset Overrides
        df = self.apply_taxonomy(df)
        df = self.extract_weights(df)
        df = self.pre_process(df)
        
        # 2. Universal Standardization
        df = standardize_column_names(df)
        df = enforce_uniform_schema(df)
        clean_nutrient_columns(df)
        df = sort_columns(df)
        
        return df
