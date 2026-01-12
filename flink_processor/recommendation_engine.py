"""
Recommendation algorithms and engines.
Contains both simple and advanced recommendation strategies.
"""

import json
from collections import defaultdict, Counter
from config.redis_config import get_redis_client, get_redis_key
import logging

logger = logging.getLogger(__name__)


class SimpleRecommendationEngine:
    """
    Simple collaborative filtering recommendation engine.
    Uses item co-occurrence: "Users who viewed X also viewed Y"
    """
    
    def __init__(self):
        """Initialize the recommendation engine."""
        self.redis_client = get_redis_client()
        
        # In-memory co-occurrence matrix
        # Format: {item_id: {related_item_id: count}}
        self.item_co_occurrence = defaultdict(Counter)
        
        logger.info("Simple Recommendation Engine initialized")
    
    def process_event(self, event_str):
        """
        Process a single event and generate recommendations.
        
        Args:
            event_str (str): JSON string of event data
            
        Returns:
            str: JSON string of recommendations
        """
        try:
            # Parse event
            event = json.loads(event_str)
            user_id = event['user_id']
            item_id = event['item_id']
            event_type = event['event_type']
            
            logger.info(
                f"Processing: User={user_id}, "
                f"Action={event_type}, "
                f"Item={item_id}"
            )
            
            # Get user's viewing history
            history = self._get_user_history(user_id)
            
            # Update co-occurrence matrix
            self._update_co_occurrence(item_id, history)
            
            # Generate recommendations
            recommendations = self._generate_recommendations(item_id, top_n=5)
            
            # Store recommendations in Redis
            self._store_recommendations(user_id, item_id, recommendations)
            
            # Update user history
            self._update_user_history(user_id, item_id)
            
            # Return result
            result = {
                'user_id': user_id,
                'item_id': item_id,
                'event_type': event_type,
                'recommendations': recommendations,
                'recommendation_count': len(recommendations)
            }
            
            logger.debug(f"Generated {len(recommendations)} recommendations")
            
            return json.dumps(result)
            
        except Exception as e:
            logger.error(f"Error processing event: {e}", exc_info=True)
            return json.dumps({
                'error': str(e),
                'event': event_str
            })
    
    def _get_user_history(self, user_id, max_items=20):
        """
        Get user's viewing history from Redis.
        
        Args:
            user_id (str): User ID
            max_items (int): Maximum items to retrieve
            
        Returns:
            list: List of item IDs
        """
        try:
            key = get_redis_key('user_history', user_id=user_id)
            history = self.redis_client.lrange(key, 0, max_items - 1)
            return history if history else []
        except Exception as e:
            logger.warning(f"Error getting user history: {e}")
            return []
    
    def _update_user_history(self, user_id, item_id, max_history=50):
        """
        Update user's viewing history in Redis.
        
        Args:
            user_id (str): User ID
            item_id (str): Item ID to add
            max_history (int): Maximum history size
        """
        try:
            key = get_redis_key('user_history', user_id=user_id)
            
            # Remove item if already exists (to avoid duplicates)
            self.redis_client.lrem(key, 0, item_id)
            
            # Add to front of list (most recent)
            self.redis_client.lpush(key, item_id)
            
            # Trim to max size
            self.redis_client.ltrim(key, 0, max_history - 1)
            
            # Set expiration (24 hours)
            self.redis_client.expire(key, 86400)
            
        except Exception as e:
            logger.warning(f"Error updating user history: {e}")
    
    def _update_co_occurrence(self, current_item, history):
        """
        Update item co-occurrence matrix.
        
        Args:
            current_item (str): Current item ID
            history (list): User's viewing history
        """
        for past_item in history:
            if past_item != current_item:
                # Increment co-occurrence count in both directions
                self.item_co_occurrence[current_item][past_item] += 1
                self.item_co_occurrence[past_item][current_item] += 1
    
    def _generate_recommendations(self, item_id, top_n=5):
        """
        Generate top N recommendations for an item.
        
        Args:
            item_id (str): Item ID
            top_n (int): Number of recommendations to generate
            
        Returns:
            list: List of recommended item IDs
        """
        if item_id not in self.item_co_occurrence:
            return []
        
        # Get items sorted by co-occurrence count
        similar_items = self.item_co_occurrence[item_id].most_common(top_n)
        
        # Extract just the item IDs
        recommendations = [item for item, count in similar_items]
        
        return recommendations
    
    def _store_recommendations(self, user_id, item_id, recommendations):
        """
        Store recommendations in Redis for fast retrieval.
        
        Args:
            user_id (str): User ID
            item_id (str): Item ID
            recommendations (list): List of recommended item IDs
        """
        if not recommendations:
            return
        
        try:
            # Store item-based recommendations
            item_key = get_redis_key('item_recommendations', item_id=item_id)
            self.redis_client.delete(item_key)
            self.redis_client.rpush(item_key, *recommendations)
            self.redis_client.expire(item_key, 3600)  # 1 hour
            
            # Store user recommendations (personalized)
            user_key = get_redis_key('user_recommendations', user_id=user_id)
            self.redis_client.delete(user_key)
            self.redis_client.rpush(user_key, *recommendations)
            self.redis_client.expire(user_key, 3600)  # 1 hour
            
            logger.debug(
                f"Stored {len(recommendations)} recommendations "
                f"for User={user_id}, Item={item_id}"
            )
            
        except Exception as e:
            logger.warning(f"Error storing recommendations: {e}")


class AdvancedRecommendationEngine:
    """
    Advanced recommendation engine with multiple strategies.
    Combines collaborative filtering with content-based filtering.
    """
    
    def __init__(self):
        """Initialize the advanced engine."""
        self.redis_client = get_redis_client()
        self.item_co_occurrence = defaultdict(Counter)
        self.item_categories = {}  # Cache for item categories
        
        logger.info("Advanced Recommendation Engine initialized")
    
    def generate_with_history(self, user_id, current_item, history, top_n=10):
        """
        Generate recommendations using full user history.
        
        Args:
            user_id (str): User ID
            current_item (str): Current item being viewed
            history (list): User's viewing history
            top_n (int): Number of recommendations
            
        Returns:
            list: Recommended item IDs
        """
        # Collaborative filtering recommendations
        collab_recs = self._collaborative_filtering(current_item, history, top_n)
        
        # Content-based recommendations
        content_recs = self._content_based_filtering(current_item, top_n)
        
        # Hybrid: Combine both strategies (weighted)
        hybrid_recs = self._hybrid_recommendations(
            collab_recs, 
            content_recs, 
            collab_weight=0.7,
            content_weight=0.3,
            top_n=top_n
        )
        
        # Store in Redis
        self._store_advanced_recommendations(user_id, hybrid_recs)
        
        return hybrid_recs
    
    def _collaborative_filtering(self, item_id, history, top_n):
        """Collaborative filtering based on co-occurrence."""
        # Update co-occurrence with history
        for hist_item in history:
            if hist_item != item_id:
                self.item_co_occurrence[item_id][hist_item] += 1
        
        # Get recommendations
        if item_id in self.item_co_occurrence:
            similar = self.item_co_occurrence[item_id].most_common(top_n)
            return [item for item, count in similar]
        return []
    
    def _content_based_filtering(self, item_id, top_n):
        """Content-based filtering using item properties."""
        # This is a simplified version
        # In production, you'd load actual item properties from Redis
        recommendations = []
        
        try:
            # Get item category (if available)
            category_key = get_redis_key('item_category', item_id=item_id)
            category = self.redis_client.get(category_key)
            
            if category:
                # Find other items in same category
                # This is a placeholder - implement based on your data structure
                pass
                
        except Exception as e:
            logger.debug(f"Content-based filtering skipped: {e}")
        
        return recommendations
    
    def _hybrid_recommendations(self, collab_recs, content_recs, 
                                collab_weight, content_weight, top_n):
        """
        Combine collaborative and content-based recommendations.
        
        Args:
            collab_recs (list): Collaborative filtering recommendations
            content_recs (list): Content-based recommendations
            collab_weight (float): Weight for collaborative filtering
            content_weight (float): Weight for content-based
            top_n (int): Number of final recommendations
            
        Returns:
            list: Hybrid recommendations
        """
        # Score each item
        scores = defaultdict(float)
        
        # Add collaborative scores
        for i, item in enumerate(collab_recs):
            scores[item] += collab_weight * (len(collab_recs) - i)
        
        # Add content-based scores
        for i, item in enumerate(content_recs):
            scores[item] += content_weight * (len(content_recs) - i)
        
        # Sort by score and return top N
        sorted_items = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [item for item, score in sorted_items[:top_n]]
    
    def _store_advanced_recommendations(self, user_id, recommendations):
        """Store advanced recommendations in Redis."""
        if not recommendations:
            return
        
        try:
            key = get_redis_key('user_recommendations', user_id=user_id)
            self.redis_client.delete(key)
            self.redis_client.rpush(key, *recommendations)
            self.redis_client.expire(key, 3600)
        except Exception as e:
            logger.warning(f"Error storing advanced recommendations: {e}")