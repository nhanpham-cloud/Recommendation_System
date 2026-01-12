"""
Advanced Flink job with stateful processing.
Uses KeyedProcessFunction for more sophisticated recommendations.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.state import ValueStateDescriptor
from flink_processor.recommendation_engine import AdvancedRecommendationEngine
from config.kafka_config import get_kafka_bootstrap_servers, get_topic_name
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StatefulRecommendationFunction(KeyedProcessFunction):
    """
    Stateful processing function that maintains user state.
    """
    
    def __init__(self):
        self.engine = None
        self.user_state = None
    
    def open(self, runtime_context: RuntimeContext):
        """Initialize state when function starts."""
        logger.info("Initializing stateful recommendation function")
        
        # Initialize recommendation engine
        self.engine = AdvancedRecommendationEngine()
        
        # Create state descriptor for user history
        state_descriptor = ValueStateDescriptor(
            "user_history",  # state name
            list  # type
        )
        
        # Get state from runtime context
        self.user_state = runtime_context.get_state(state_descriptor)
        
        logger.info("State initialized successfully")
    
    def process_element(self, value, ctx):
        """
        Process each event with state management.
        
        Args:
            value: The input value (JSON string)
            ctx: Process context
        """
        try:
            # Parse event
            event = json.loads(value)
            user_id = event['user_id']
            item_id = event['item_id']
            
            # Get current user state
            history = self.user_state.value()
            if history is None:
                history = []
            
            # Update history
            if item_id not in history:
                history.append(item_id)
                # Keep only last 20 items
                history = history[-20:]
                self.user_state.update(history)
            
            # Generate recommendations using history
            recommendations = self.engine.generate_with_history(
                user_id, 
                item_id, 
                history
            )
            
            # Yield result
            result = {
                'user_id': user_id,
                'item_id': item_id,
                'history_size': len(history),
                'recommendations': recommendations
            }
            
            yield json.dumps(result)
            
        except Exception as e:
            logger.error(f"Error processing event: {e}")


def main():
    """Main advanced Flink job with stateful processing."""
    logger.info("Starting Advanced Flink Recommendation Job")
    
    # Create environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)  # Increased parallelism
    env.enable_checkpointing(30000)  # Checkpoint every 30 seconds
    
    # Kafka source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(','.join(get_kafka_bootstrap_servers())) \
        .set_topics(get_topic_name()) \
        .set_group_id('recommendation-group-advanced') \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    # Create stream
    events = env.from_source(kafka_source, None, 'Kafka Source')
    
    # Key by user_id and apply stateful processing
    recommendations = events \
        .map(lambda x: (json.loads(x)['user_id'], x)) \
        .key_by(lambda x: x[0]) \
        .process(StatefulRecommendationFunction())
    
    # Print results
    recommendations.print()
    
    # Execute
    logger.info("Executing advanced Flink job...")
    env.execute("Advanced Recommendation System")


if __name__ == '__main__':
    main()