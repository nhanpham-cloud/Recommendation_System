"""
End-to-end system test.
Tests event tracking and recommendation retrieval.
"""

import requests
import time
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Service URLs
EVENT_PRODUCER_URL = "http://localhost:5000"
RECOMMENDATION_API_URL = "http://localhost:5001"


def check_services():
    """Check if all services are running."""
    logger.info("Checking if services are running...")
    
    services = {
        'Event Producer': f"{EVENT_PRODUCER_URL}/health",
        'Recommendation API': f"{RECOMMENDATION_API_URL}/health"
    }
    
    all_running = True
    
    for name, url in services.items():
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                logger.info(f"✓ {name} is running")
            else:
                logger.error(f"✗ {name} returned status {response.status_code}")
                all_running = False
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ {name} is not accessible: {e}")
            all_running = False
    
    return all_running


def test_event_tracking():
    """Test event tracking functionality."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing Event Tracking")
    logger.info("=" * 60)
    
    # Test events simulating a user shopping session
    events = [
        {"user_id": "test_user_1", "item_id": "item_100", "event_type": "view"},
        {"user_id": "test_user_1", "item_id": "item_101", "event_type": "view"},
        {"user_id": "test_user_1", "item_id": "item_102", "event_type": "view"},
        {"user_id": "test_user_1", "item_id": "item_101", "event_type": "addtocart"},
        {"user_id": "test_user_2", "item_id": "item_100", "event_type": "view"},
        {"user_id": "test_user_2", "item_id": "item_101", "event_type": "view"},
        {"user_id": "test_user_2", "item_id": "item_103", "event_type": "view"},
        {"user_id": "test_user_3", "item_id": "item_100", "event_type": "view"},
        {"user_id": "test_user_3", "item_id": "item_102", "event_type": "view"},
    ]
    
    successful = 0
    failed = 0
    
    for event in events:
        try:
            response = requests.post(
                f"{EVENT_PRODUCER_URL}/track-event",
                json=event,
                timeout=5
            )
            
            if response.status_code == 200:
                logger.info(
                    f"✓ Event tracked: User {event['user_id']} "
                    f"-> {event['event_type']} -> Item {event['item_id']}"
                )
                successful += 1
            else:
                logger.error(f"✗ Failed to track event: {response.text}")
                failed += 1
                
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ Error sending event: {e}")
            failed += 1
        
        time.sleep(0.5)  # Small delay between events
    
    logger.info(f"\nResults: {successful} successful, {failed} failed")
    return successful > 0


def test_batch_tracking():
    """Test batch event tracking."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing Batch Event Tracking")
    logger.info("=" * 60)
    
    batch_data = {
        "events": [
            {"user_id": "batch_user_1", "item_id": "item_200", "event_type": "view"},
            {"user_id": "batch_user_1", "item_id": "item_201", "event_type": "view"},
            {"user_id": "batch_user_2", "item_id": "item_200", "event_type": "view"},
        ]
    }
    
    try:
        response = requests.post(
            f"{EVENT_PRODUCER_URL}/track-batch",
            json=batch_data,
            timeout=5
        )
        
        if response.status_code in [200, 207]:
            result = response.json()
            logger.info(f"✓ Batch processed: {result['results']['successful']} successful")
            return True
        else:
            logger.error(f"✗ Batch failed: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"✗ Error in batch request: {e}")
        return False


def test_recommendations():
    """Test recommendation retrieval."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing Recommendation Retrieval")
    logger.info("=" * 60)
    
    # Wait for Flink to process events
    logger.info("Waiting 5 seconds for Flink to process events...")
    time.sleep(5)
    
    # Test user recommendations
    test_users = ["test_user_1", "test_user_2", "test_user_3"]
    
    for user_id in test_users:
        try:
            response = requests.get(
                f"{RECOMMENDATION_API_URL}/recommendations/{user_id}",
                params={'limit': 5},
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                recs = data.get('recommendations', [])
                
                if recs:
                    logger.info(
                        f"✓ {user_id}: Got {len(recs)} recommendations: {recs}"
                    )
                else:
                    logger.warning(
                        f"⚠ {user_id}: No recommendations yet "
                        "(may need more events or processing time)"
                    )
            else:
                logger.error(f"✗ Failed to get recommendations: {response.text}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ Error getting recommendations: {e}")
        
        time.sleep(0.5)
    
    return True


def test_item_recommendations():
    """Test item-based recommendations."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing Item Recommendations")
    logger.info("=" * 60)
    
    test_items = ["item_100", "item_101"]
    
    for item_id in test_items:
        try:
            response = requests.get(
                f"{RECOMMENDATION_API_URL}/recommendations/item/{item_id}",
                params={'limit': 5},
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                similar_items = data.get('similar_items', [])
                
                if similar_items:
                    logger.info(
                        f"✓ {item_id}: Similar items: {similar_items}"
                    )
                else:
                    logger.warning(f"⚠ {item_id}: No similar items found yet")
            else:
                logger.error(f"✗ Failed: {response.text}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ Error: {e}")


def test_user_history():
    """Test user history retrieval."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing User History")
    logger.info("=" * 60)
    
    try:
        response = requests.get(
            f"{RECOMMENDATION_API_URL}/user/test_user_1/history",
            timeout=5
        )
        
        if response.status_code == 200:
            data = response.json()
            history = data.get('history', [])
            logger.info(f"✓ User history: {history}")
        else:
            logger.error(f"✗ Failed: {response.text}")
            
    except requests.exceptions.RequestException as e:
        logger.error(f"✗ Error: {e}")


def main():
    """Run all tests."""
    logger.info("\n" + "=" * 60)
    logger.info("E-COMMERCE RECOMMENDATION SYSTEM - END-TO-END TEST")
    logger.info("=" * 60)
    
    # Check services
    if not check_services():
        logger.error("\n✗ Not all services are running!")
        logger.error("Please start all services before running tests:")
        logger.error("  1. docker-compose up -d")
        logger.error("  2. python -m event_producer.app")
        logger.error("  3. python -m flink_processor.simple_job")
        logger.error("  4. python -m recommendation_api.app")
        return
    
    # Run tests
    test_event_tracking()
    test_batch_tracking()
    test_recommendations()
    test_item_recommendations()
    test_user_history()
    
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUITE COMPLETED!")
    logger.info("=" * 60)
    logger.info("\nNotes:")
    logger.info("- If recommendations are empty, send more events and wait")
    logger.info("- Check Flink processor logs for processing confirmation")
    logger.info("- Use Redis Commander (http://localhost:8081) to inspect data")


if __name__ == '__main__':
    main()