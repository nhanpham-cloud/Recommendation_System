"""
Data loader for Retail Rocket dataset.
Loads CSV files into Redis for fast access.
"""

import pandas as pd
from config.redis_config import get_redis_client, get_redis_key
import logging
import os
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RetailRocketDataLoader:
    """
    Loads Retail Rocket e-commerce dataset into Redis.
    """
    
    def __init__(self, data_dir='data'):
        """
        Initialize data loader.
        
        Args:
            data_dir (str): Directory containing CSV files
        """
        self.data_dir = Path(data_dir)
        self.redis_client = get_redis_client()
        logger.info(f"Data loader initialized for directory: {data_dir}")
    
    def load_all(self):
        """Load all dataset files."""
        logger.info("=" * 60)
        logger.info("Starting data load process")
        logger.info("=" * 60)
        
        self.load_item_properties()
        self.load_category_tree()
        self.load_sample_events()
        
        logger.info("=" * 60)
        logger.info("Data load completed!")
        logger.info("=" * 60)
    
    def load_item_properties(self):
        """
        Load item properties from CSV.
        
        Expected columns: itemid, timestamp, property, value
        """
        file_path = self.data_dir / 'item_properties.csv'
        
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            logger.info("Skipping item properties load")
            return
        
        logger.info(f"Loading item properties from: {file_path}")
        
        try:
            # Read CSV
            df = pd.read_csv(file_path)
            logger.info(f"Read {len(df)} rows from CSV")
            
            # Group by item_id
            unique_items = df['itemid'].nunique()
            logger.info(f"Found {unique_items} unique items")
            
            # Store properties in Redis
            stored_count = 0
            for item_id, group in df.groupby('itemid'):
                # Convert to dict for storage
                properties = {}
                for _, row in group.iterrows():
                    prop_name = row['property']
                    prop_value = row['value']
                    
                    if prop_name not in properties:
                        properties[prop_name] = []
                    properties[prop_name].append(str(prop_value))
                
                # Store in Redis as JSON
                key = get_redis_key('item_properties', item_id=str(item_id))
                self.redis_client.set(key, str(properties))
                self.redis_client.expire(key, 604800)  # 7 days
                
                stored_count += 1
                
                if stored_count % 1000 == 0:
                    logger.info(f"Stored properties for {stored_count} items...")
            
            logger.info(f"✓ Successfully loaded properties for {stored_count} items")
            
        except Exception as e:
            logger.error(f"Error loading item properties: {e}", exc_info=True)
    
    def load_category_tree(self):
        """
        Load category tree from CSV.
        
        Expected columns: categoryid, parentid
        """
        file_path = self.data_dir / 'category_tree.csv'
        
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            logger.info("Skipping category tree load")
            return
        
        logger.info(f"Loading category tree from: {file_path}")
        
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Read {len(df)} categories from CSV")
            
            # Store category relationships
            for _, row in df.iterrows():
                category_id = str(row['categoryid'])
                parent_id = str(row['parentid'])
                
                # Store category -> parent mapping
                key = get_redis_key('category', category_id=category_id)
                self.redis_client.hset(key, 'parent_id', parent_id)
                self.redis_client.expire(key, 604800)  # 7 days
            
            logger.info(f"✓ Successfully loaded {len(df)} categories")
            
        except Exception as e:
            logger.error(f"Error loading category tree: {e}", exc_info=True)
    
    def load_sample_events(self, sample_size=10000):
        """
        Load sample events for testing.
        
        Args:
            sample_size (int): Number of sample events to load
        """
        file_path = self.data_dir / 'events.csv'
        
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            logger.info("Skipping events load")
            return
        
        logger.info(f"Loading sample events from: {file_path}")
        
        try:
            # Read first N rows
            df = pd.read_csv(file_path, nrows=sample_size)
            logger.info(f"Read {len(df)} sample events from CSV")
            
            # Expected columns: timestamp, visitorid, event, itemid, transactionid
            
            # Store sample user histories
            user_groups = df.groupby('visitorid')
            
            for user_id, user_events in user_groups:
                # Get items this user interacted with
                items = user_events['itemid'].dropna().unique().tolist()
                
                if len(items) > 0:
                    key = get_redis_key('user_history', user_id=str(user_id))
                    
                    # Store only first 20 items
                    items_to_store = [str(item) for item in items[:20]]
                    
                    self.redis_client.delete(key)
                    self.redis_client.rpush(key, *items_to_store)
                    self.redis_client.expire(key, 86400)  # 24 hours
            
            logger.info(f"✓ Successfully loaded sample histories for {len(user_groups)} users")
            
        except Exception as e:
            logger.error(f"Error loading sample events: {e}", exc_info=True)
    
    def verify_load(self):
        """Verify that data was loaded correctly."""
        logger.info("\nVerifying data load...")
        
        # Count keys by pattern
        patterns = {
            'Item Properties': 'item:*:properties',
            'Categories': 'category:*',
            'User Histories': 'user:*:history'
        }
        
        for name, pattern in patterns.items():
            count = len(self.redis_client.keys(pattern))
            logger.info(f"{name}: {count} entries")
        
        # Show sample data
        logger.info("\nSample data:")
        
        # Sample item property
        sample_item_keys = self.redis_client.keys('item:*:properties')
        if sample_item_keys:
            sample_key = sample_item_keys[0]
            sample_data = self.redis_client.get(sample_key)
            logger.info(f"Sample item property: {sample_key} = {sample_data[:100]}...")
        
        # Sample user history
        sample_user_keys = self.redis_client.keys('user:*:history')
        if sample_user_keys:
            sample_key = sample_user_keys[0]
            sample_data = self.redis_client.lrange(sample_key, 0, 5)
            logger.info(f"Sample user history: {sample_key} = {sample_data}")