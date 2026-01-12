from kafka import KafkaProducer
from flask import Flask, request, jsonify
import json
import time

app = Flask(__name__)

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/track-event', methods=['POST'])
def track_event():
    """
    Endpoint to receive user actions from frontend
    Expected format:
    {
        "user_id": "123",
        "item_id": "456",
        "event_type": "view",  # or "addtocart", "purchase"
        "timestamp": 1234567890
    }
    """
    event_data = request.json
    
    # Add timestamp if not present
    if 'timestamp' not in event_data:
        event_data['timestamp'] = int(time.time() * 1000)
    
    # Send to Kafka
    producer.send('user-events', value=event_data)
    producer.flush()
    
    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    app.run(port=5000)