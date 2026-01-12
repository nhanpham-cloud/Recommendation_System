"""
Unit tests for Flink processor.
"""

import pytest
import json
from flink_processor.recommendation_engine import SimpleRecommendationEngine
from unittest.mock import Mock, patch


class TestSimpleRecommendationEngine:
    """Test cases for SimpleRecommendationEngine."""
    
    @patch('flink_processor.recommendation_engine.get_redis_client')
    def test_engine_initialization(self, mock_redis):
        """Test engine initializes correctly."""
        engine = SimpleRecommendationEngine()
        
        assert engine.redis_client is not None
        assert isinstance(engine.item_co_occurrence, dict)
    
    @patch('flink_processor.recommendation_engine.get_redis_client')
    def test_process_event(self, mock_redis):
        """Test event processing."""
        # Setup mock Redis
        mock_redis.return_value.lrange.return_value = []
        mock_redis.return_value.lrem.return_value = 0
        mock_redis.return_value.lpush.return_value = 1
        
        engine = SimpleRecommendationEngine()
        
        event = {
            'user_id': 'user123',
            'item_id': 'item456',
            'event_type': 'view'
        }
        
        result_str = engine.process_event(json.dumps(event))
        result = json.loads(result_str)
        
        assert result['user_id'] == 'user123'
        assert result['item_id'] == 'item456'
        assert 'recommendations' in result
    
    @patch('flink_processor.recommendation_engine.get_redis_client')
    def test_generate_recommendations(self, mock_redis):
        """Test recommendation generation."""
        engine = SimpleRecommendationEngine()
        
        # Manually set up co-occurrence data
        engine.item_co_occurrence['item1']['item2'] = 5
        engine.item_co_occurrence['item1']['item3'] = 3
        engine.item_co_occurrence['item1']['item4'] = 1
        
        recommendations = engine._generate_recommendations('item1', top_n=2)
        
        assert len(recommendations) == 2
        assert recommendations[0] == 'item2'  # Highest count
        assert recommendations[1] == 'item3'  # Second highest