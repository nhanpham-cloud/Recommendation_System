"""
Pytest configuration and fixtures.
"""

import pytest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture(scope='session')
def mock_redis():
    """Mock Redis client for testing."""
    from unittest.mock import Mock
    
    redis_mock = Mock()
    redis_mock.ping.return_value = True
    redis_mock.lrange.return_value = []
    redis_mock.get.return_value = None
    redis_mock.set.return_value = True
    redis_mock.delete.return_value = 1
    redis_mock.rpush.return_value = 1
    redis_mock.lpush.return_value = 1
    redis_mock.lrem.return_value = 0
    redis_mock.ltrim.return_value = True
    redis_mock.expire.return_value = True
    redis_mock.keys.return_value = []
    
    return redis_mock


@pytest.fixture(scope='session')
def sample_events():
    """Sample events for testing."""
    return [
        {
            'user_id': 'user_1',
            'item_id': 'item_100',
            'event_type': 'view',
            'timestamp': 1234567890
        },
        {
            'user_id': 'user_1',
            'item_id': 'item_101',
            'event_type': 'view',
            'timestamp': 1234567891
        },
        {
            'user_id': 'user_2',
            'item_id': 'item_100',
            'event_type': 'addtocart',
            'timestamp': 1234567892
        },
    ]