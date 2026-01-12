"""
Kafka producer for user events.
Handles sending events to Kafka with error handling and retries.
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from config.kafka_config import get_kafka_bootstrap_servers, get_topic_name, get_producer_config
import json
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class EventProducer:
    """
    Kafka producer for user events.
    Manages connection, sending, and error handling.
    """
    
    def __init__(self):
        """Initialize the Kafka producer."""
        self.topic = get_topic_name()
        self.stats = {
            'total_sent': 0,
            'total_failed': 0,
            'last_sent_time': None,
            'start_time': datetime.now()
        }
        
        try:
            # Get producer config
            config = get_producer_config()
            
            # Create producer
            self.producer = KafkaProducer(
                bootstrap_servers=config['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks=config['acks'],
                retries=config['retries'],
                max_in_flight_requests_per_connection=config['max_in_flight_requests_per_connection'],
                compression_type=config.get('compression_type', 'gzip'),
                request_timeout_ms=30000,
                linger_ms=10,  # Batch messages for 10ms
                batch_size=16384  # 16KB batch size
            )
            
            logger.info(f"Kafka producer initialized successfully for topic: {self.topic}")
            logger.info(f"Connected to brokers: {config['bootstrap_servers']}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    def send_event(self, event_data, key=None):
        """
        Send event to Kafka topic.
        
        Args:
            event_data (dict): Event data to send
            key (str, optional): Kafka message key (for partitioning)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Add timestamp if not present
            if 'timestamp' not in event_data:
                event_data['timestamp'] = int(time.time() * 1000)
            
            # Add event ID for tracking
            if 'event_id' not in event_data:
                event_data['event_id'] = f"{event_data['user_id']}_{event_data['timestamp']}"
            
            # Use user_id as key for partitioning (keeps user events in order)
            if key is None:
                key = event_data.get('user_id')
            
            # Send to Kafka
            future = self.producer.send(
                self.topic,
                value=event_data,
                key=key
            )
            
            # Wait for acknowledgment (with timeout)
            record_metadata = future.get(timeout=10)
            
            # Update stats
            self.stats['total_sent'] += 1
            self.stats['last_sent_time'] = datetime.now()
            
            logger.debug(
                f"Event sent successfully: "
                f"Topic={record_metadata.topic}, "
                f"Partition={record_metadata.partition}, "
                f"Offset={record_metadata.offset}"
            )
            
            return True
            
        except KafkaTimeoutError as e:
            logger.error(f"Kafka timeout error: {e}")
            self.stats['total_failed'] += 1
            return False
            
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
            self.stats['total_failed'] += 1
            return False
            
        except Exception as e:
            logger.error(f"Unexpected error sending event: {e}", exc_info=True)
            self.stats['total_failed'] += 1
            return False
    
    def send_batch(self, events):
        """
        Send multiple events in batch.
        
        Args:
            events (list): List of event dictionaries
            
        Returns:
            dict: Results with success/failure counts
        """
        results = {
            'total': len(events),
            'successful': 0,
            'failed': 0
        }
        
        for event in events:
            if self.send_event(event):
                results['successful'] += 1
            else:
                results['failed'] += 1
        
        # Flush to ensure all messages are sent
        self.producer.flush()
        
        return results
    
    def get_stats(self):
        """
        Get producer statistics.
        
        Returns:
            dict: Statistics dictionary
        """
        uptime = (datetime.now() - self.stats['start_time']).total_seconds()
        
        return {
            'total_sent': self.stats['total_sent'],
            'total_failed': self.stats['total_failed'],
            'success_rate': (
                self.stats['total_sent'] / 
                (self.stats['total_sent'] + self.stats['total_failed'])
                if (self.stats['total_sent'] + self.stats['total_failed']) > 0
                else 0
            ),
            'last_sent_time': (
                self.stats['last_sent_time'].isoformat() 
                if self.stats['last_sent_time'] 
                else None
            ),
            'uptime_seconds': uptime,
            'topic': self.topic
        }
    
    def flush(self):
        """Flush any pending messages."""
        try:
            self.producer.flush()
            logger.debug("Producer flushed successfully")
        except Exception as e:
            logger.error(f"Error flushing producer: {e}")
    
    def close(self):
        """Close the producer and cleanup resources."""
        try:
            logger.info("Closing Kafka producer...")
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed successfully")
        except Exception as e:
            logger.error(f"Error closing producer: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Example usage
if __name__ == '__main__':
    # Setup logging for testing
    logging.basicConfig(level=logging.DEBUG)
    
    # Test the producer
    with EventProducer() as producer:
        # Send test event
        test_event = {
            'user_id': 'test_user_123',
            'item_id': 'test_item_456',
            'event_type': 'view'
        }
        
        success = producer.send_event(test_event)
        print(f"Event sent: {success}")
        
        # Get stats
        stats = producer.get_stats()
        print(f"Producer stats: {stats}")