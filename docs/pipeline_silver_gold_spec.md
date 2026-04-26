This comprehensive specification outlines the "Audit & Aggregate" model for the Silver (Final Phase) and Gold layers of the nutrition data pipeline. It is designed to ensure that both Reference (scientific) and Open Food Facts (OFF/retail) data streams reach a state of high-integrity, audited, and semantically unified data before the final Platinum merge.
📄 Technical Specification: Nutrition Data Pipeline (Silver & Gold)
1. Silver Tier (The Standardizer)

The goal of the Silver tier is to ensure every data source, regardless of origin, adopts a perfectly uniform structure and undergoes a rigorous physical audit.
1.1. Structural Uniformity & Whitelist

    Column Schema: Every Silver CSV must contain the exact same set of columns in the same order.

    Nutrient Whitelist: Enforce a strict list of ~53 nutrients (macros, vitamins, minerals).

    Null Filling: If a source lacks a specific whitelisted nutrient, create the column and populate it with null. Do not drop the column.

1.2. The Flexible Taxonomy Column

Each script must generate a semantic_taxonomy descriptor to act as a "blocking key" for matching.

    Format: [Origin] | [State] | [Process] (e.g., plant | raw | whole).

    Mapping: Map source-specific categories (e.g., CoFID "Food Group") to these universal semantic strings.

1.3. "Fail-Fast" Physics Audit

Apply a strict mass-balance check to catch unit conversion or data-entry errors.

    Calculation: Sum=Protein+Fat+Carbs+Fiber+Water+Ash+Alcohol.

    Hard Ceiling: If Sum>102g per 100g, the pipeline must stop and log a FATAL_ERROR.

    Correction: Errors must be fixed in the specific source's Silver script (e.g., clean_usda.py) rather than being masked or clipped.

1.4. OFF-Specific Silver Logic

    Size Extraction: Use regex to extract units (1L, 500g, 4x330ml) into structured unit_size and numeric_size_g columns.

    Name Placeholder: Replace the size in the description with a [SIZE] token for the residual name check in Gold.

2. Gold Tier (The Auditor)

The Gold tier focuses on merging (Reference) or grouping (OFF) data using a multi-factor proximity algorithm rather than a trust-based hierarchy.
2.1. Algorithm: Multi-Factor Audited Grouping (OFF)

Instead of destructive stripping, use a scoring matrix to consolidate store products.

    Blocking: Only compare items sharing the same Brand and Taxonomy Descriptor.

    Nutritional Proximity Check:

        Calculate the delta for Calories, Protein, Fat, and Carbs.

        Merge Condition: Only group if all four macros are within a 5-10% tolerance of each other.

    Residual Text Match:

        Compare descriptions using Token Sort Ratio (which reorders words alphabetically).

        High-similarity matches (>90%) that pass the Macro Check are grouped.

    Metadata Preservation:

        unified_name: The shortest descriptive name in the group.

        original_names: A JSON list of every unique description merged (preserving "varieties").

        available_sizes: A JSON list of all extracted sizes (e.g., ["1L", "2L"]).

2.2. Algorithm: Reference Stream Merge (Audit & Aggregate)

Merge FooDB, CoFID, and USDA by aggregating their differences.

    Block Match: Match descriptions using Token Sort Ratio within the same Taxonomy block.

    Disagreement Flagging (The 10% Rule):

        Compare every whitelisted nutrient across matching sources.

        If any value differs by >10%, set a data_quality_discrepancy flag to True.

    Statistical Aggregation: For every nutrient, the Gold record must store:

        [Nutrient]_avg: The mean value (primary optimization target).

        [Nutrient]_min: The lowest reported value.

        [Nutrient]_max: The highest reported value.

        source_count: Number of sources contributing to this record.

3. Data Flow Summary
Stage	Action	Verification
Silver (End)	Schema Whitelisting & Taxonomy Assignment	Stop if Physics Audit >102g.
Gold (OFF)	Group by Brand + Taxonomy + Macro Fingerprint	Store all original_names to preserve "difference."
Gold (Ref)	Merge Science Sources	Flag discrepancies >10% and store Min/Max/Avg.
