"""
Unit tests for recommendation API.
"""

import pytest
from recommendation_api.app import app
from unittest.mock import Mock, patch


@pytest.fixture
def client():
    """Create test client."""
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


class TestRecommendationAPI:
    """Test cases for Recommendation API."""
    
    def test_health_check(self, client):
        """Test health check endpoint."""
        with patch('recommendation_api.app.redis_client') as mock_redis:
            mock_redis.ping.return_value = True
            
            response = client.get('/health')
            
            assert response.status_code == 200
            data = response.get_json()
            assert data['status'] == 'healthy'
    
    def test_get_user_recommendations(self, client):
        """Test getting user recommendations."""
        with patch('recommendation_api.app.recommendation_service') as mock_service:
            mock_service.get_user_recommendations.return_value = ['item1', 'item2', 'item3']
            
            response = client.get('/recommendations/user123?limit=5')
            
            assert response.status_code == 200
            data = response.get_json()
            assert data['user_id'] == 'user123'
            assert len(data['recommendations']) == 3
    
    def test_get_user_recommendations_empty(self, client):
        """Test getting recommendations when none exist."""
        with patch('recommendation_api.app.recommendation_service') as mock_service:
            mock_service.get_user_recommendations.return_value = []
            
            response = client.get('/recommendations/user123')
            
            assert response.status_code == 200
            data = response.get_json()
            assert data['recommendations'] == []
            assert 'message' in data
    
    def test_get_item_recommendations(self, client):
        """Test getting item recommendations."""
        with patch('recommendation_api.app.recommendation_service') as mock_service:
            mock_service.get_item_recommendations.return_value = ['item2', 'item3']
            
            response = client.get('/recommendations/item/item1')
            
            assert response.status_code == 200
            data = response.get_json()
            assert data['item_id'] == 'item1'
            assert len(data['similar_items']) == 2
    
    def test_batch_recommendations(self, client):
        """Test batch recommendations endpoint."""
        with patch('recommendation_api.app.recommendation_service') as mock_service:
            mock_service.get_user_recommendations.side_effect = [
                ['item1', 'item2'],
                ['item3', 'item4']
            ]
            
            payload = {
                'user_ids': ['user1', 'user2'],
                'limit': 5
            }
            
            response = client.post(
                '/recommendations/batch',
                json=payload,
                content_type='application/json'
            )
            
            assert response.status_code == 200
            data = response.get_json()
            assert 'results' in data
            assert 'user1' in data['results']
            assert 'user2' in data['results']
    
    def test_batch_recommendations_invalid_input(self, client):
        """Test batch endpoint with invalid input."""
        response = client.post(
            '/recommendations/batch',
            json={'invalid': 'data'},
            content_type='application/json'
        )
        
        assert response.status_code == 400
        data = response.get_json()
        assert 'error' in data
    
    def test_get_user_history(self, client):
        """Test getting user history."""
        with patch('recommendation_api.app.recommendation_service') as mock_service:
            mock_service.get_user_history.return_value = ['item1', 'item2', 'item3']
            
            response = client.get('/user/user123/history')
            
            assert response.status_code == 200
            data = response.get_json()
            assert data['user_id'] == 'user123'
            assert len(data['history']) == 3
    
    def test_404_error(self, client):
        """Test 404 error handling."""
        response = client.get('/nonexistent')
        
        assert response.status_code == 404
        data = response.get_json()
        assert 'error' in data
        assert 'available_endpoints' in data
