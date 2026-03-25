import os
import sys
import logging
import requests
import tarfile
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
DATA_PIPELINE_DIR = Path(__file__).resolve().parent.parent.parent.parent
RAW_DIR = DATA_PIPELINE_DIR / 'data' / 'reference' / 'raw'

# URLs
FOODB_URL = "https://foodb.ca/public/system/downloads/foodb_2020_04_07_csv.tar.gz"
COFID_URL = "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1026600/McCance_and_Widdowsons_The_Composition_of_Foods_Integrated_Dataset_2021.xlsx"

def download_file(url, target_path):
    """Downloads a file with a progress tracker gracefully."""
    logger.info(f"Downloading {url}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024 * 1024 # 1 Megabyte
    downloaded = 0
    
    with open(target_path, 'wb') as f:
        for data in response.iter_content(block_size):
            f.write(data)
            downloaded += len(data)
            if total_size > 0:
                percent = int(50 * downloaded / total_size)
                sys.stdout.write(f"\r[{'=' * percent}{' ' * (50 - percent)}] {downloaded/(1024*1024):.1f} MB")
                sys.stdout.flush()
    sys.stdout.write("\n")
    logger.info(f"Saved to {target_path}")

def download_raw_foodb():
    foodb_dir = RAW_DIR / 'foodb'
    foodb_dir.mkdir(parents=True, exist_ok=True)
    
    tar_path = foodb_dir / 'foodb_2020_04_07_csv.tar.gz'
    if not tar_path.exists():
        download_file(FOODB_URL, tar_path)
    else:
        logger.info(f"FooDB tarball already exists at {tar_path}")
        
    logger.info("Extracting FooDB...")
    try:
        with tarfile.open(tar_path, "r:gz") as tar:
            tar.extractall(path=foodb_dir)
        logger.info("FooDB successfully extracted.")
    except Exception as e:
        logger.error(f"Failed to extract FooDB: {e}")

def download_raw_cofid():
    cofid_dir = RAW_DIR / 'cofid'
    cofid_dir.mkdir(parents=True, exist_ok=True)
    
    excel_path = cofid_dir / 'McCance_and_Widdowsons_The_Composition_of_Foods_Integrated_Dataset_2021.xlsx'
    
    if not excel_path.exists():
        download_file(COFID_URL, excel_path)
    else:
        logger.info(f"CoFID already exists at {excel_path}")

if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("Starting Raw Data Restoration Script")
    logger.info("=" * 50)
    
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    
    # We do not download USDA here because USDA is fetched dynamically via its API inside pipeline/usda
    
    download_raw_foodb()
    download_raw_cofid()
    
    logger.info("=" * 50)
    logger.info("Raw data download complete. You can now safely run build_bronze.py!")
    logger.info("=" * 50)
