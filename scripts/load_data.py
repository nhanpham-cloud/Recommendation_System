"""
Script to load Retail Rocket dataset into Redis.
Run this after starting Docker containers.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.data_loader import RetailRocketDataLoader
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def main():
    """Main function to load data."""
    try:
        logger.info("Starting data load script")
        
        # Initialize loader
        loader = RetailRocketDataLoader(data_dir='data')
        
        # Load all data
        loader.load_all()
        
        # Verify load
        loader.verify_load()
        
        logger.info("\n✓ Data load completed successfully!")
        logger.info("You can now start the Flink processor and APIs")
        
    except Exception as e:
        logger.error(f"Error loading data: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
