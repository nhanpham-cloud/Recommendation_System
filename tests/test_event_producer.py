"""
Unit tests for event producer.
"""

import pytest
import json
from event_producer.producer import EventProducer
from unittest.mock import Mock, patch


class TestEventProducer:
    """Test cases for EventProducer class."""
    
    @patch('event_producer.producer.KafkaProducer')
    def test_producer_initialization(self, mock_kafka_producer):
        """Test that producer initializes correctly."""
        producer = EventProducer()
        
        assert producer.topic == 'user-events'
        assert producer.stats['total_sent'] == 0
        assert producer.stats['total_failed'] == 0
        mock_kafka_producer.assert_called_once()
    
    @patch('event_producer.producer.KafkaProducer')
    def test_send_event_success(self, mock_kafka_producer):
        """Test successful event sending."""
        # Setup mock
        mock_future = Mock()
        mock_future.get.return_value = Mock(topic='user-events', partition=0, offset=123)
        mock_kafka_producer.return_value.send.return_value = mock_future
        
        # Create producer and send event
        producer = EventProducer()
        event = {
            'user_id': 'test_user',
            'item_id': 'test_item',
            'event_type': 'view'
        }
        
        result = producer.send_event(event)
        
        assert result is True
        assert producer.stats['total_sent'] == 1
        assert producer.stats['total_failed'] == 0
    
    @patch('event_producer.producer.KafkaProducer')
    def test_send_event_adds_timestamp(self, mock_kafka_producer):
        """Test that timestamp is added if not present."""
        mock_future = Mock()
        mock_future.get.return_value = Mock(topic='user-events', partition=0, offset=123)
        mock_kafka_producer.return_value.send.return_value = mock_future
        
        producer = EventProducer()
        event = {
            'user_id': 'test_user',
            'item_id': 'test_item',
            'event_type': 'view'
        }
        
        producer.send_event(event)
        
        # Check that timestamp was added
        assert 'timestamp' in event
        assert isinstance(event['timestamp'], int)
    
    @patch('event_producer.producer.KafkaProducer')
    def test_send_batch(self, mock_kafka_producer):
        """Test batch event sending."""
        mock_future = Mock()
        mock_future.get.return_value = Mock(topic='user-events', partition=0, offset=123)
        mock_kafka_producer.return_value.send.return_value = mock_future
        
        producer = EventProducer()
        events = [
            {'user_id': 'user1', 'item_id': 'item1', 'event_type': 'view'},
            {'user_id': 'user2', 'item_id': 'item2', 'event_type': 'view'},
        ]
        
        results = producer.send_batch(events)
        
        assert results['total'] == 2
        assert results['successful'] == 2
        assert results['failed'] == 0