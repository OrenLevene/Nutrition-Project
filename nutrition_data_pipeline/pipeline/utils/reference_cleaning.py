"""
Shared data cleaning and standardization functions for the Reference Stream.
These functions are used primarily at the Silver tier to enforce strict uniformity.
"""
import pandas as pd
import numpy as np

import re

def parse_weight_to_grams(quantity_str):
    """
    Heuristically parse a quantity string (e.g. '500g', '1kg', '12 x 330ml') into numeric grams.
    Treats ml as grams for nutritional consistency.
    """
    if pd.isna(quantity_str) or not str(quantity_str).strip():
        return np.nan
    
    q = str(quantity_str).lower()
    
    # Handle multipacks like "12 x 330ml" or "4x250g"
    multipack_match = re.search(r'(\d+)\s*[x*]\s*(\d+(?:\.\d+)?)\s*(g|kg|ml|l)', q)
    if multipack_match:
        count = float(multipack_match.group(1))
        amount = float(multipack_match.group(2))
        unit = multipack_match.group(3)
        total = count * amount
        if unit in ['kg', 'l']:
            total *= 1000
        return total

    # Handle standard single packs like "500g" or "1.5 kg"
    single_match = re.search(r'(\d+(?:\.\d+)?)\s*(g|kg|ml|l)', q)
    if single_match:
        amount = float(single_match.group(1))
        unit = single_match.group(2)
        if unit in ['kg', 'l']:
            amount *= 1000
        return amount
        
    return np.nan

    """Remove foods by their official category/group tag. Much safer than regex."""
    if not drop_groups or category_col not in df.columns:
        print(f"  No category filtering applied (col='{category_col}')")
        return df
    
    before = len(df)
    mask = df[category_col].str.lower().isin([g.lower() for g in drop_groups])
    df = df[~mask].reset_index(drop=True)
    removed = before - len(df)
    print(f"  Category filter: removed {removed} foods ({before} -> {len(df)})")
    for g in drop_groups:
        print(f"    - Dropped group: '{g}'")
    return df

def clean_nutrient_columns(df):
    """Enforce strict numeric type safety for nutrients and cap impossible values."""
    meta_cols = ['food_id', 'description', 'food_code', 'name_scientific', 
                 'food_group', 'food_subgroup', 'food_type', 'data_source',
                 'fdc_id', 'ndb_number', 'wikipedia_id', 'category',
                 'description.1', 'food_category', 'food_category_id', 
                 'data_type', 'publication_date',
                 'brand_owner', 'gtin_upc', 'ingredients', 
                 'serving_size', 'serving_size_unit', 'household_serving']
    meta_cols_set = set(c for c in meta_cols if c in df.columns)
    
    for col in df.columns:
        if col in meta_cols_set:
            df[col] = df[col].astype(str)
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Round the cleaned numeric data to exactly 3 significant figures
    def to_3sf(val):
        if pd.isna(val) or val == 0: return val
        try:
            return np.around(val, int(3 - 1 - np.floor(np.log10(np.abs(val)))))
        except:
            return val
    
    for col in df.columns:
        if col not in meta_cols_set:
            df[col] = df[col].apply(to_3sf)
            
    bad_types = []
    for col in df.columns:
        if col in meta_cols_set:
            if df[col].dtype.kind not in ('O', 'U', 'S'):
                bad_types.append(f"  {col}: expected str, got {df[col].dtype}")
        else:
            if not np.issubdtype(df[col].dtype, np.number):
                bad_types.append(f"  {col}: expected numeric, got {df[col].dtype}")
    
    if bad_types:
        raise TypeError(f"Type validation failed:\n" + "\n".join(bad_types))
    
    # Remove unused floating method that wasn't integrated
    

def standardize_column_names(df):
    """Standardize nutrient column names to beautiful human-readable format via exact matching."""
    
    # === STRICT PRIORITY RESOLUTION (Option B) ===
    # For nutrients that have multiple conflicting measurements in the raw data,
    # we enforce a strict hierarchy. The first one in the list wins.
    priority_rules = {
        'Folate (ug)': [
            'Folate, DFE (UG)', 'Folate, food (UG)', 'Folate, total (UG)', 
            'Folic acid (UG)', 'Folate (UG)', 'Folate'
        ],
        'Fiber (g)': [
            'Fiber, total dietary (G)', 'Total dietary fiber (AOAC 2011.25) (G)', 
            'Dietary fibre', 'AOAC fibre', 'Fibre (G)', 'Fibre', 
            'Non-starch polysaccharide'
        ],
        'Vitamin A (ug)': [
            'Vitamin A, RAE (UG)', 'Retinol equivalent (UG)', 'Total retinol equivalent', 
            'Vitamin A (UG)', 'Vitamin A'
        ],
        'Niacin (mg)': [
            'Niacin equivalent', 'Niacin (MG)', 'Niacin'
        ],
        'Carbohydrate (g)': [
            'Carbohydrate, by difference (G)', 'Carbohydrate, by summation (G)', 
            'Carbohydrate (G)', 'Carbohydrate'
        ],
        'Fat (g)': [
            'Total lipid (fat) (G)', 'Total fat (NLEA) (G)', 
            'Fat (G)', 'Fat'
        ],
        'Protein (g)': [
            'Protein (G)', 'Protein'
        ],
        'Calories (kcal)': [
            'Energy (KCAL)', 'Energy (Atwater General Factors) (KCAL)', 
            'Energy (Atwater Specific Factors) (KCAL)', 'kcal', 'Energy'
        ]
    }
    
    for target, ordered_originals in priority_rules.items():
        existing = [c for c in ordered_originals if c in df.columns]
        if existing:
            # Take the first non-null according to our strictly prioritized list
            df[target] = df[existing].bfill(axis=1).iloc[:, 0]
            df = df.drop(columns=existing)

    name_mappings = {
        # === MACRONUTRIENTS ===
        # NOTE: FooDB MG_100G/KCAL_100G columns are NOT mapped here.
        # They are handled by the auto-converter below which does proper unit conversion.
        'Energy (KCAL)': 'Calories (kcal)', 'kcal': 'Calories (kcal)', 'Energy': 'Calories (kcal)',
        'Energy (Atwater General Factors) (KCAL)': 'Calories (kcal)',
        'Energy (Atwater Specific Factors) (KCAL)': 'Calories (kcal)',
        'kJ': 'Energy (kJ)', 'Energy (kJ)': 'Energy (kJ)',
        'Protein (G)': 'Protein (g)', 'Protein': 'Protein (g)',
        'Total lipid (fat) (G)': 'Fat (g)', 'Total fat (NLEA) (G)': 'Fat (g)', 'Fat': 'Fat (g)',
        'Carbohydrate, by difference (G)': 'Carbohydrate (g)', 'Carbohydrate, by summation (G)': 'Carbohydrate (g)',
        'Carbohydrate (G)': 'Carbohydrate (g)', 'Carbohydrate': 'Carbohydrate (g)',
        'Fiber, total dietary (G)': 'Fiber (g)', 'Total dietary fiber (AOAC 2011.25) (G)': 'Fiber (g)',
        'Fibre (G)': 'Fiber (g)', 'Non-starch polysaccharide': 'Fiber (g)',
        'AOAC fibre': 'Fiber (g)', 'Dietary fibre': 'Fiber (g)', 'Fibre': 'Fiber (g)',
        'Fiber, soluble (G)': 'Soluble Fiber (g)', 'Fiber, insoluble (G)': 'Insoluble Fiber (g)',
        'High Molecular Weight Dietary Fiber (HMWDF) (G)': 'HMW Fiber (g)',
        'Low Molecular Weight Dietary Fiber (LMWDF) (G)': 'LMW Fiber (g)',
        'Sugars, Total (G)': 'Sugars (g)', 'Total Sugars (G)': 'Sugars (g)', 'Total sugars': 'Sugars (g)', 'Total sugars (G)': 'Sugars (g)',
        'Starch (G)': 'Starch (g)', 'Starch': 'Starch (g)', 'Resistant starch (G)': 'Resistant Starch (g)',
        'Glucose (G)': 'Glucose (g)', 'Glucose': 'Glucose (g)',
        'Fructose (G)': 'Fructose (g)', 'Fructose': 'Fructose (g)',
        'Galactose (G)': 'Galactose (g)', 'Galactose': 'Galactose (g)',
        'Sucrose (G)': 'Sucrose (g)', 'Sucrose': 'Sucrose (g)',
        'Lactose (G)': 'Lactose (g)', 'Lactose': 'Lactose (g)',
        'Maltose (G)': 'Maltose (g)', 'Maltose': 'Maltose (g)',
        'Raffinose (G)': 'Raffinose (g)', 'Stachyose (G)': 'Stachyose (g)', 'Verbascose (G)': 'Verbascose (g)',
        'Water (G)': 'Water (g)', 'Water': 'Water (g)',
        'Ash (G)': 'Ash (g)', 'Ash (MG_100 G)': 'Ash (g)',
        'Nitrogen (G)': 'Nitrogen (g)', 'Total nitrogen': 'Nitrogen (g)',
        'Alcohol, ethyl (G)': 'Alcohol (g)', 'Alcohol (G)': 'Alcohol (g)', 'Alcohol': 'Alcohol (g)',
        'Cholesterol (MG)': 'Cholesterol (mg)', 'Cholesterol': 'Cholesterol (mg)',
        
        # === MINERALS ===
        'Calcium, Ca (MG)': 'Calcium (mg)', 'Ca (MG)': 'Calcium (mg)', 'Calcium (MG)': 'Calcium (mg)', 'Calcium': 'Calcium (mg)', 'Ca': 'Calcium (mg)',
        'Iron, Fe (MG)': 'Iron (mg)', 'Fe (MG)': 'Iron (mg)', 'Iron (MG)': 'Iron (mg)', 'Iron': 'Iron (mg)', 'Fe': 'Iron (mg)',
        'Magnesium, Mg (MG)': 'Magnesium (mg)', 'Mg (MG)': 'Magnesium (mg)', 'Magnesium (MG)': 'Magnesium (mg)', 'Magnesium': 'Magnesium (mg)', 'Mg': 'Magnesium (mg)',
        'Phosphorus, P (MG)': 'Phosphorus (mg)', 'P (MG)': 'Phosphorus (mg)', 'Phosphorus (MG)': 'Phosphorus (mg)', 'Phosphorus': 'Phosphorus (mg)', 'P': 'Phosphorus (mg)',
        'Potassium, K (MG)': 'Potassium (mg)', 'K (MG)': 'Potassium (mg)', 'Potassium (MG)': 'Potassium (mg)', 'Potassium': 'Potassium (mg)', 'K': 'Potassium (mg)',
        'Sodium, Na (MG)': 'Sodium (mg)', 'Na (MG)': 'Sodium (mg)', 'Sodium (MG)': 'Sodium (mg)', 'Sodium': 'Sodium (mg)', 'Na': 'Sodium (mg)',
        'Zinc, Zn (MG)': 'Zinc (mg)', 'Zn (MG)': 'Zinc (mg)', 'Zinc (MG)': 'Zinc (mg)', 'Zinc': 'Zinc (mg)', 'Zn': 'Zinc (mg)',
        'Copper, Cu (MG)': 'Copper (mg)', 'Cu (MG)': 'Copper (mg)', 'Copper (MG)': 'Copper (mg)', 'Copper': 'Copper (mg)', 'Cu': 'Copper (mg)',
        'Manganese, Mn (MG)': 'Manganese (mg)', 'Mn (MG)': 'Manganese (mg)', 'Manganese (MG)': 'Manganese (mg)', 'Manganese': 'Manganese (mg)', 'Mn': 'Manganese (mg)',
        'Selenium, Se (UG)': 'Selenium (ug)', 'Se (MG)': 'Selenium (ug)', 'Selenium (MG)': 'Selenium (ug)', 'Selenium': 'Selenium (ug)', 'Se': 'Selenium (ug)',
        'Iodine, I (UG)': 'Iodine (ug)', 'I (MG)': 'Iodine (ug)', 'Iodine (MG)': 'Iodine (ug)', 'Iodine': 'Iodine (ug)', 'I': 'Iodine (ug)',
        'Chloride (MG)': 'Chloride (mg)', 'Cl (MG)': 'Chloride (mg)', 'Chloride': 'Chloride (mg)', 'Cl': 'Chloride (mg)',
        'Fluoride, F (UG)': 'Fluoride (ug)', 'Cobalt, Co (UG)': 'Cobalt (ug)',
        'Molybdenum, Mo (UG)': 'Molybdenum (ug)', 'Nickel, Ni (UG)': 'Nickel (ug)',
        'Boron, B (UG)': 'Boron (ug)', 'Sulfur, S (MG)': 'Sulfur (mg)',
        
        # === VITAMINS ===
        'Vitamin A, RAE (UG)': 'Vitamin A (ug)', 'Retinol equivalent (UG)': 'Vitamin A (ug)', 
        'Total retinol equivalent': 'Vitamin A (ug)', 'Vitamin A (UG)': 'Vitamin A (ug)', 'Vitamin A': 'Vitamin A (ug)',
        'Retinol (UG)': 'Retinol (ug)', 'All-trans-retinol': 'Retinol (ug)', 'Retinol': 'Retinol (ug)',
        '13-cis-retinol': '13-cis-Retinol (ug)',
        'Vitamin A, IU (IU)': 'Vitamin A (IU)',
        'Vitamin C, total ascorbic acid (MG)': 'Vitamin C (mg)', 'Vitamin C (MG)': 'Vitamin C (mg)', 
        'Vit C': 'Vitamin C (mg)', 'Vitamin C': 'Vitamin C (mg)',
        'Vitamin D (D2 + D3) (UG)': 'Vitamin D (ug)', 'Vitamin D (UG)': 'Vitamin D (ug)', 'Vitamin D': 'Vitamin D (ug)',
        'Vitamin D (D2 + D3), International Units (IU)': 'Vitamin D (IU)',
        'Vitamin D2 (ergocalciferol) (UG)': 'Vitamin D2 (ug)', 'Vitamin D3 (cholecalciferol) (UG)': 'Vitamin D3 (ug)',
        'Vitamin D4 (UG)': 'Vitamin D4 (ug)', '25-hydroxycholecalciferol (UG)': '25-OH Vitamin D3 (ug)',
        '25-hydroxy vitamin D3': '25-OH Vitamin D3 (ug)',
        'Vitamin E (alpha-tocopherol) (MG)': 'Vitamin E (mg)', 'Vitamin E (MG)': 'Vitamin E (mg)', 
        'Vitamin E': 'Vitamin E (mg)', 'Alpha-tocopherol': 'Vitamin E (mg)',
        'Vitamin E, added (MG)': 'Vitamin E added (mg)',
        'Vitamin K (phylloquinone) (UG)': 'Vitamin K (ug)', 'Vitamin K1 (UG)': 'Vitamin K (ug)', 
        'Vitamin K1': 'Vitamin K (ug)', 'Phylloquinone': 'Vitamin K (ug)',
        'Vitamin K (Dihydrophylloquinone) (UG)': 'Vitamin K2 dihydro (ug)',
        'Vitamin K (Menaquinone-4) (UG)': 'Vitamin K2 MK4 (ug)',
        'Thiamin (MG)': 'Thiamin (mg)', 'Thiamin': 'Thiamin (mg)',
        'Riboflavin (MG)': 'Riboflavin (mg)', 'Riboflavin': 'Riboflavin (mg)',
        'Niacin (MG)': 'Niacin (mg)', 'Niacin equivalent': 'Niacin (mg)', 'Niacin': 'Niacin (mg)',
        'Vitamin B-6 (MG)': 'Vitamin B6 (mg)', 'Vitamin B6 (MG)': 'Vitamin B6 (mg)', 'Vitamin B6': 'Vitamin B6 (mg)',
        'Vitamin B-12 (UG)': 'Vitamin B12 (ug)', 'Vitamin B12 (UG)': 'Vitamin B12 (ug)', 'Vitamin B12': 'Vitamin B12 (ug)',
        'Vitamin B-12, added (UG)': 'Vitamin B12 added (ug)',
        'Folate, total (UG)': 'Folate (ug)', 'Folate (UG)': 'Folate (ug)', 'Folate': 'Folate (ug)',
        'Folate, DFE (UG)': 'Folate (ug)', 'Folate, food (UG)': 'Folate (ug)', 
        'Folic acid (UG)': 'Folate (ug)', '5-methyl folate': 'Folate (ug)',
        '10-Formyl folic acid (10HCOFA) (UG)': '10-Formyl Folic Acid (ug)',
        '5-Formyltetrahydrofolic acid (5-HCOH4 (UG)': '5-Formyl THF (ug)',
        '5-methyl tetrahydrofolate (5-MTHF) (UG)': '5-Methyl THF (ug)',
        'Pantothenic acid (MG)': 'Pantothenic Acid (mg)', 'Pantothenate (MG)': 'Pantothenic Acid (mg)',
        'Biotin (UG)': 'Biotin (ug)', 'Biotin': 'Biotin (ug)',
        'Choline, total (MG)': 'Choline (mg)',
        'Betaine (MG)': 'Betaine (mg)',
        
        # === CAROTENOIDS ===
        'Carotene, beta (UG)': 'Beta-Carotene (ug)', 'Beta-carotene': 'Beta-Carotene (ug)',
        'Carotene, alpha (UG)': 'Alpha-Carotene (ug)', 'Alpha-carotene': 'Alpha-Carotene (ug)',
        'Carotene, gamma (UG)': 'Gamma-Carotene (ug)',
        'Lycopene (UG)': 'Lycopene (ug)', 'Lycopene': 'Lycopene (ug)',
        'Lutein + zeaxanthin (UG)': 'Lutein + Zeaxanthin (ug)', 'Lutein (UG)': 'Lutein (ug)', 'Lutein': 'Lutein (ug)',
        'Zeaxanthin (UG)': 'Zeaxanthin (ug)',
        'Cryptoxanthin, beta (UG)': 'Beta-Cryptoxanthin (ug)', 'Cryptoxanthin, alpha (UG)': 'Alpha-Cryptoxanthin (ug)',
        
        # === TOCOPHEROLS ===
        'Tocopherol, beta (MG)': 'Beta-Tocopherol (mg)', 'Beta-tocopherol': 'Beta-Tocopherol (mg)',
        'Tocopherol, gamma (MG)': 'Gamma-Tocopherol (mg)', 'Gamma-tocopherol': 'Gamma-Tocopherol (mg)',
        'Tocopherol, delta (MG)': 'Delta-Tocopherol (mg)', 'Delta-tocopherol': 'Delta-Tocopherol (mg)',
        'Tocotrienol, alpha (MG)': 'Alpha-Tocotrienol (mg)', 'Alpha-tocotrienol': 'Alpha-Tocotrienol (mg)',
        'Tocotrienol, beta (MG)': 'Beta-Tocotrienol (mg)',
        'Tocotrienol, gamma (MG)': 'Gamma-Tocotrienol (mg)',
        'Tocotrienol, delta (MG)': 'Delta-Tocotrienol (mg)',
        
        # === FAT TOTALS ===
        'Fatty acids, total saturated (G)': 'Saturated Fat (g)', 
        'Saturated fatty acids per 100g food': 'Saturated Fat (g)', 'Satd FA /100g FA': 'Saturated Fat % FA',
        'Fatty acids, total monounsaturated (G)': 'Monounsaturated Fat (g)',
        'Monounsaturated fatty acids per 100g food': 'Monounsaturated Fat (g)',
        'cis-Monounsaturated fatty acids /100g Food': 'Monounsaturated Fat (g)',
        'Fatty acids, total polyunsaturated (G)': 'Polyunsaturated Fat (g)',
        'Polyunsaturated fatty acids per 100g food': 'Polyunsaturated Fat (g)',
        'cis-Polyunsaturated fatty acids /100g Food': 'Polyunsaturated Fat (g)',
        'Fatty acids, total trans (G)': 'Trans Fat (g)',
        'Total Trans fatty acids per 100g food': 'Trans Fat (g)',
        'trans monounsaturated fatty acids per 100g food': 'Trans Monounsaturated Fat (g)',
        'trans polyunsaturated fatty acid per 100g food': 'Trans Polyunsaturated Fat (g)',
        'Total n-3 polyunsaturated fatty acids per 100g food': 'Omega-3 (g)',
        'Total n-6 polyunsaturated fatty acids per 100g food': 'Omega-6 (g)',
        'cis n-6': 'Omega-6 (g)',
        
        # === KEY OMEGA FATTY ACIDS ===
        'PUFA 18:3 n-3 c,c,c (ALA) (G)': 'ALA (g)',
        'cis n-3 Octadecatrienoic acid per 100g food': 'ALA (g)',
        'PUFA 20:5 n-3 (EPA) (G)': 'EPA (g)',
        'cis n-3 Eicosapentaenoic acid per 100g food': 'EPA (g)',
        'PUFA 22:6 n-3 (DHA) (G)': 'DHA (g)',
        'cis n-3 Docosahexaenoic acid (DHA) per 100g food': 'DHA (g)',
        'PUFA 22:5 n-3 (DPA) (G)': 'DPA (g)',
        'cis n-3 Docosapentaenoic acid per 100g food': 'DPA (g)',
        'PUFA 18:2 n-6 c,c (G)': 'Linoleic Acid (g)',
        'PUFA 20:4 n-6 (G)': 'Arachidonic Acid (g)',
        'PUFA 20:4 (G)': 'Arachidonic Acid (g)',
        
        # === OTHER ===
        'Caffeine (MG)': 'Caffeine (mg)', 'Caffeine': 'Caffeine (mg)',
        'Theobromine (MG)': 'Theobromine (mg)',
        'Phytosterols (MG)': 'Phytosterols (mg)',
        'Oxalic acid (MG)': 'Oxalic Acid (mg)',
        'Citric acid (MG)': 'Citric Acid (mg)',
        'Malic acid (MG)': 'Malic Acid (mg)',
        'Pyruvic acid (MG)': 'Pyruvic Acid (mg)',
        'Quinic acid (MG)': 'Quinic Acid (mg)',
        'Beta-glucan (G)': 'Beta-Glucan (g)',
        'Ergothioneine (MG)': 'Ergothioneine (mg)',
        'Glutathione (MG)': 'Glutathione (mg)',
        'Oligosaccharide': 'Oligosaccharides (g)', 'Oligosaccharides (G)': 'Oligosaccharides (g)',
        'Tryptophan divided by 60': 'Tryptophan/60 (mg)',
    }
    
    rename_dict = {}
    for old_name, new_name in name_mappings.items():
        if old_name in df.columns and new_name not in df.columns:
            rename_dict[old_name] = new_name
            
    if rename_dict:
        df = df.rename(columns=rename_dict)
    
    # === UNIT CONVERSION for FooDB MG_100G columns ===
    # FooDB stores nutrients as mg per 100g. We need to convert:
    #   - Macros (Protein, Fat, Carbs, Fiber etc.): divide by 1000 → grams
    #   - Micros (Calcium, Iron etc.): keep as-is → mg  
    #   - Energy: keep as-is → kcal
    mg100g_cols = [c for c in df.columns if 'MG_100G' in c or 'MG_100 G' in c or 'KCAL_100G' in c]
    
    for col in mg100g_cols:
        # Extract the nutrient name from patterns like "Calcium (MG_100G)" or "Protein (MG_100 G)"
        clean_name = col.replace(' (MG_100G)', '').replace(' (MG_100 G)', '').replace(' (KCAL_100G)', '')
        
        # Determine target name and conversion
        # Macros should be in grams, so divide by 1000
        macro_keywords = ['Protein', 'Fat', 'Carbohydrate', 'Fiber', 'Ash', 'Water', 'Alcohol']
        is_macro = any(kw.lower() in clean_name.lower() for kw in macro_keywords)
        is_energy = 'KCAL' in col
        
        if is_energy:
            target_name = 'Calories (kcal)'
            converted = df[col]  # already kcal
        elif is_macro:
            target_name = f'{clean_name} (g)'
            converted = df[col] / 1000
        else:
            target_name = f'{clean_name} (mg)'
            converted = df[col]  # already mg
        
        # Merge into existing column if it exists, otherwise create
        if target_name in df.columns:
            df[target_name] = df[target_name].fillna(converted)
        else:
            df[target_name] = converted
        
        df = df.drop(columns=[col])
        
    # Fix unit-converted names before whitelist/duplicate merging
    manual_fixes = {
        'Proteins (g)': 'Protein (g)',
        'Fiber (dietary) (g)': 'Fiber (g)',
        'Fatty acids (g)': 'Fat (g)',
        'Carbohydrates (g)': 'Carbohydrate (g)'
    }
    df = df.rename(columns=manual_fixes)
        
    # Standardizing into human-readable names may cause previously distinct columns 
    # (e.g., "Vit C" and "Vitamin C") to both become "Vitamin C (mg)".
    # We must consolidate these identical columns so Pandas doesn't crash.
    if df.columns.duplicated().any():
        # Group identically named columns and keep the first non-null value per row
        def first_valid(col):
            return col.bfill(axis=1).iloc[:, 0]
        
        # Get duplicate names
        dup_names = df.columns[df.columns.duplicated()].unique()
        for name in dup_names:
            combined = first_valid(df[name])
            df = df.drop(columns=[name])
            df[name] = combined

    # === IMPOSSIBLE VALUE CAPPING ===
    # Enforce physical reality on 100g basis: 
    # - Macros cannot exceed 100g 
    # - Energy cannot exceed 900kcal
    if 'Calories (kcal)' in df.columns:
        df.loc[df['Calories (kcal)'] > 900, 'Calories (kcal)'] = np.nan
    
    for col in df.columns:
        if '(g)' in col.lower():
            df.loc[df[col] > 100, col] = np.nan

    # === IMPUTE MISSING AGGREGATES ===
    # Automatically sum missing omega aggregations from their explicitly mapped sub-components.
    if 'Omega-3 (g)' not in df.columns:
        df['Omega-3 (g)'] = np.nan
    o3_parts = [c for c in ['ALA (g)', 'EPA (g)', 'DHA (g)'] if c in df.columns]
    if o3_parts:
        df['Omega-3 (g)'] = df['Omega-3 (g)'].fillna(df[o3_parts].sum(axis=1, min_count=1))
        
    if 'Omega-6 (g)' not in df.columns:
        df['Omega-6 (g)'] = np.nan
    o6_parts = [c for c in ['Linoleic Acid (g)', 'Arachidonic Acid (g)'] if c in df.columns]
    if o6_parts:
        df['Omega-6 (g)'] = df['Omega-6 (g)'].fillna(df[o6_parts].sum(axis=1, min_count=1))

    # === WHITELIST FILTERING ===
    # Drop all obscure, hyper-granular lipid/amino acid columns and keep only obvious top-level nutrients.
    APPROVED_NUTRIENTS = {
        'Calories (kcal)', 'Energy (kJ)', 'Protein (g)', 'Carbohydrate (g)', 'Fat (g)', 
        'Fiber (g)', 'Sugars (g)', 'Water (g)', 'Ash (g)', 'Alcohol (g)',
        'Starch (g)', 'Glucose (g)', 'Fructose (g)', 'Sucrose (g)', 'Lactose (g)', 'Maltose (g)',
        'Saturated Fat (g)', 'Monounsaturated Fat (g)', 'Polyunsaturated Fat (g)', 
        'Trans Fat (g)', 'Omega-3 (g)', 'Omega-6 (g)', 'EPA (g)', 'DHA (g)', 'ALA (g)', 
        'Cholesterol (mg)',
        'Vitamin A (ug)', 'Vitamin B6 (mg)', 'Vitamin B12 (ug)', 'Vitamin C (mg)', 
        'Vitamin D (ug)', 'Vitamin E (mg)', 'Vitamin K (ug)', 'Thiamin (mg)', 
        'Riboflavin (mg)', 'Niacin (mg)', 'Folate (ug)', 'Pantothenic Acid (mg)', 
        'Biotin (ug)', 'Choline (mg)',
        'Calcium (mg)', 'Iron (mg)', 'Magnesium (mg)', 'Phosphorus (mg)', 
        'Potassium (mg)', 'Sodium (mg)', 'Zinc (mg)', 'Copper (mg)', 
        'Manganese (mg)', 'Selenium (ug)', 'Iodine (ug)', 'Chloride (mg)',
        'Caffeine (mg)'
    }
    
    REFERENCE_META_COLS = {
        'food_id', 'description', 'food_code', 'name_scientific', 
        'food_group', 'food_subgroup', 'food_type', 'data_source',
        'fdc_id', 'ndb_number', 'wikipedia_id', 'category',
        'description.1', 'food_category', 'food_category_id',
        'data_type', 'publication_date',
        'brand_owner', 'gtin_upc', 'ingredients',
        'serving_size', 'serving_size_unit', 'household_serving',
        'quantity', 'main_category_en', 'countries_en', 'allergens',
        'traces', 'additives_tags', 'nutriscore_grade', 'nova_group',
        'image_url', 'image_small_url', 'image_ingredients_url', 'ingredients_text',
        'Total Weight (g)'
    }
    
    keep_cols = [c for c in df.columns if c in APPROVED_NUTRIENTS or c in REFERENCE_META_COLS]
    df = df[keep_cols]

    return df

def sort_columns(df):
    """Sort columns: metadata first, then nutrients alphabetically."""
    REFERENCE_META_COLS = [
        'food_id', 'description', 'food_code', 'name_scientific', 
        'food_group', 'food_subgroup', 'food_type', 'data_source',
        'fdc_id', 'ndb_number', 'wikipedia_id', 'category',
        'description.1', 'food_category', 'food_category_id',
        'data_type', 'publication_date',
        'brand_owner', 'gtin_upc', 'ingredients', 'ingredients_text',
        'serving_size', 'serving_size_unit', 'household_serving',
        'quantity', 'main_category_en', 'countries_en', 'allergens',
        'traces', 'additives_tags', 'nutriscore_grade', 'nova_group',
        'image_url', 'image_small_url', 'image_ingredients_url', 'Total Weight (g)'
    ]
    meta_cols = [c for c in REFERENCE_META_COLS if c in df.columns]
    nutrient_cols = sorted([c for c in df.columns if c not in meta_cols])
    return df[meta_cols + nutrient_cols]
