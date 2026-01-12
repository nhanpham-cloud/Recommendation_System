# Setup Kafka topics for the recommendation system

echo "================================================"
echo "Setting up Kafka topics"
echo "================================================"

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 15

# Create user-events topic
echo "Creating 'user-events' topic..."
docker exec kafka kafka-topics --create \
  --topic user-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists \
  --config retention.ms=604800000

if [ $? -eq 0 ]; then
    echo "✓ Topic 'user-events' created successfully"
else
    echo "✗ Failed to create topic 'user-events'"
fi

echo ""
echo "Listing all topics:"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

echo ""
echo "Topic details:"
docker exec kafka kafka-topics --describe \
  --topic user-events \
  --bootstrap-server localhost:9092

echo ""
echo "================================================"
echo "Kafka setup complete!"
echo "================================================"