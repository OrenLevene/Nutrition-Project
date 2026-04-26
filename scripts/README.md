# Scripts (Reorganized)

This folder contains utility scripts for the data pipeline and general maintenance. 
Many previous diagnostic scripts have been archived or deleted to keep the project clean.

### 📁 `pipeline/`
*   **`rebuild_store_mapping.py`**: The bridge between the `ProductMatcher` and the processed database. Used to generate the mapping from barcodes to canonical foods.
*   **`generate_portions.py`**: Heuristic engine to guess portion sizes (e.g. 400g for a tin of canned beans) for 150+ categories. Use this for mass-populating portions.
*   **`extract_off_uk.py`**: Primary ingestion tool for standardizing the Open Food Facts UK data.

### 📁 `utils/`
*   **`md_to_pdf.py`**: General purpose converter for project documentation.
*   **`extract_words.py`**: Analyzes word frequency patterns in product names.
*   **`check_off_data.py`**: A quick health check for the OFF CSV dataset.
