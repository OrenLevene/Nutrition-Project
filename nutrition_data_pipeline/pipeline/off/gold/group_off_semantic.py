"""
OFF Product Grouping - Phase 2: Semantic Grouping + Outlier Detection

Goal: Consolidate Phase 1 groups that are semantic synonyms (e.g., "Yogurt" vs "Yoghurt")
but keep distinct variants separate (e.g., "0%" vs "Full Fat").

Strategy:
1. Load Phase 1 groups.
2. Generate text embeddings for canonical names.
3. Cluster by nutrition FIRST to separate variants (Light vs Full Fat).
4. Within nutrition clusters, cluster by semantic text similarity.
5. Detect and handle outliers.
"""
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import AgglomerativeClustering
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
SIMILARITY_THRESHOLD = 0.90  # High threshold for text similarity
MIN_CLUSTER_SIZE = 2
NUTRITION_TOLERANCE_PCT = 0.15  # 15% tolerance for grouping nutrition profiles
NUTRITION_TOLERANCE_ABS_CAL = 10.0 # +/- 10 kcal absolute floor
NUTRITION_TOLERANCE_ABS_MACRO = 1.0 # +/- 1.0g absolute floor

def get_nutrition_key(row):
    """
    Create a coarse nutrition key for initial bucketing.
    Groups items with 'similar' nutrition together effectively.
    """
    # Rounding logic:
    # Calories: nearest 20
    # Macros: nearest 2
    cal = int(row['calories'] / 20) * 20
    prot = int(row['protein'] / 2) * 2
    carb = int(row['carbohydrate'] / 2) * 2
    fat = int(row['fat'] / 2) * 2
    return f"{cal}|{prot}|{carb}|{fat}"

def is_nutrition_match(row1, row2):
    """
    Strict check if two items match nutritionally.
    """
    # Calories
    diff_cal = abs(row1['calories'] - row2['calories'])
    limit_cal = max(NUTRITION_TOLERANCE_ABS_CAL, row1['calories'] * NUTRITION_TOLERANCE_PCT)
    if diff_cal > limit_cal:
        return False
        
    # Macros
    colors = ['protein', 'carbohydrate', 'fat']
    for nutrient in colors:
        diff = abs(row1[nutrient] - row2[nutrient])
        limit = max(NUTRITION_TOLERANCE_ABS_MACRO, row1[nutrient] * NUTRITION_TOLERANCE_PCT)
        if diff > limit:
            return False
            
    return True

def main():
    logger.info("Loading Phase 1 groups...")
    df = pd.read_parquet('data/processed/SILVER_OFF_grouped.parquet')
    logger.info(f"Loaded {len(df)} initial groups")
    
    # 1. Generate Embeddings (SentenceTransformer)
    logger.info("Loading SentenceTransformer model...")
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    logger.info("Generating semantic embeddings...")
    # Encode canonical names
    names = df['canonical_name'].fillna("").tolist()
    embeddings = model.encode(names, show_progress_bar=True, batch_size=64)
    
    # Add index for lookup
    df['emb_idx'] = range(len(df))
    
    # 2. Group by Nutrition Buckets First
    # This prevents "Light Yogurt" matching "Full Fat Yogurt" purely on text
    logger.info("Grouping by nutrition buckets...")
    df['nutrition_bucket'] = df.apply(get_nutrition_key, axis=1)
    
    new_groups = []
    discarded_outliers = []
    processed_indices = set()
    
    # Process each nutrition bucket
    buckets = df.groupby('nutrition_bucket')
    logger.info(f"Processing {len(buckets)} nutrition buckets...")
    
    for bucket_id, bucket_df in buckets:
        if len(bucket_df) < 2:
            # Single item bucket - keep as is
            new_groups.extend(bucket_df.to_dict('records'))
            continue
            
        # Within this nutrition bucket, cluster by Text Similarity
        bucket_indices = bucket_df['emb_idx'].values
        bucket_embeddings = embeddings[bucket_indices]
        
        # Calculate similarity matrix for this bucket
        sim_matrix = cosine_similarity(bucket_embeddings)
        
        # Cluster using Agglomerative Clustering
        # distance_threshold = 1 - similarity_threshold
        clustering = AgglomerativeClustering(
            n_clusters=None,
            distance_threshold=1 - SIMILARITY_THRESHOLD,
            metric='cosine',
            linkage='average'
        )
        labels = clustering.fit_predict(bucket_embeddings)
        
        # Process clusters
        bucket_df['cluster_label'] = labels
        
        for label, cluster in bucket_df.groupby('cluster_label'):
            # Convert cluster to records
            items = cluster.to_dict('records')
            
            if len(items) == 1:
                new_groups.append(items[0])
            else:
                # Merge semantic duplicates
                # 1. Canonical = item with most complete original data (already sorted in phase 1?)
                # We'll stick with the one that has the most members from Phase 1
                items.sort(key=lambda x: (-x['member_count'], -len(x['brands'])))
                canonical = items[0]
                
                # Merge details
                all_brands = set()
                all_supers = set()
                all_food_ids = []
                total_members = 0
                
                for item in items:
                    all_brands.update(item['brands'])
                    all_supers.update(item['supermarkets'])
                    all_food_ids.extend(item['member_food_ids'])
                    total_members += item['member_count']
                
                merged_group = canonical.copy()
                merged_group['brands'] = list(all_brands)
                merged_group['supermarkets'] = list(all_supers)
                merged_group['member_food_ids'] = all_food_ids
                merged_group['member_count'] = total_members
                
                # Note: Nutrition values kept from canonical (representative)
                
                new_groups.append(merged_group)
    
    # 3. Create Final Dataframes
    final_df = pd.DataFrame(new_groups)
    
    # Clean up temporary columns
    if 'emb_idx' in final_df.columns:
        del final_df['emb_idx']
    if 'nutrition_bucket' in final_df.columns:
        del final_df['nutrition_bucket']
    if 'cluster_label' in final_df.columns:
        del final_df['cluster_label']

    logger.info("="*60)
    logger.info("PHASE 2 RESULTS")
    logger.info("="*60)
    logger.info(f"Phase 1 Groups: {len(df)}")
    logger.info(f"Phase 2 Groups: {len(final_df)}")
    logger.info(f"Reduction: {len(df) - len(final_df)} groups merged")
    
    # Identify large merged groups
    merged_counts = final_df[final_df['member_count'] > 1]['member_count']
    logger.info(f"Total merged products: {merged_counts.sum()}")
    
    # Save
    output_path = 'data/processed/SILVER_OFF_semantic.parquet'
    final_df.to_parquet(output_path, index=False)
    logger.info(f"Saved to {output_path}")
    
    # Update mapping
    mapping = []
    for _, g in final_df.iterrows():
        for fid in g['member_food_ids']:
            mapping.append({'food_id': fid, 'group_id': g['group_id']})
    
    mapping_df = pd.DataFrame(mapping)
    mapping_df.to_parquet('data/processed/SILVER_OFF_group_map_semantic.parquet', index=False)
    logger.info("Saved updated mapping")

if __name__ == "__main__":
    main()
