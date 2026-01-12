"""
Simple Flink job for real-time recommendations.
Good starting point for beginners.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from flink_processor.recommendation_engine import SimpleRecommendationEngine
from config.kafka_config import get_kafka_bootstrap_servers, get_topic_name
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """
    Main Flink job for processing user events and generating recommendations.
    """
    logger.info("=" * 60)
    logger.info("Starting Flink Recommendation Job")
    logger.info("=" * 60)
    
    try:
        # Create execution environment
        env = StreamExecutionEnvironment.get_execution_environment()
        
        # Set parallelism (start with 1 for simplicity)
        env.set_parallelism(1)
        
        # Enable checkpointing for fault tolerance (every 60 seconds)
        env.enable_checkpointing(60000)
        
        logger.info(f"Execution environment created with parallelism=1")
        
        # Build Kafka source
        kafka_bootstrap = ','.join(get_kafka_bootstrap_servers())
        kafka_topic = get_topic_name()
        
        logger.info(f"Connecting to Kafka: {kafka_bootstrap}")
        logger.info(f"Consuming from topic: {kafka_topic}")
        
        kafka_source = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap) \
            .set_topics(kafka_topic) \
            .set_group_id('recommendation-group') \
            .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()
        
        # Create data stream from Kafka
        events = env.from_source(
            kafka_source,
            watermark_strategy=None,
            source_name='Kafka User Events Source'
        )
        
        logger.info("Kafka source created successfully")
        
        # Create recommendation engine
        engine = SimpleRecommendationEngine()
        
        # Process events and generate recommendations
        recommendations = events.map(engine.process_event)
        
        # Print recommendations (for monitoring)
        recommendations.print()
        
        # Execute the Flink job
        logger.info("Executing Flink job...")
        logger.info("Waiting for events... (Press Ctrl+C to stop)")
        logger.info("=" * 60)
        
        env.execute("Real-time Recommendation System - Simple Job")
        
    except Exception as e:
        logger.error(f"Error in Flink job: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()