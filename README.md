# 🚀 Complete Setup & Run Guide

## Step-by-Step Instructions for Beginners

---

## 📋 Prerequisites

Before starting, make sure you have:

1. **Python 3.8 or higher**
   ```bash
   python --version
   # Should show 3.8 or higher
   ```

2. **Docker & Docker Compose**
   ```bash
   docker --version
   docker-compose --version
   ```

3. **Git** (optional, for version control)
   ```bash
   git --version
   ```

4. **VS Code** (recommended) or any text editor

---

## 🏗️ Part 1: Create Project Structure

### Option B: Using Git

```bash
# If you have a repository
git clone <your-repo-url>
cd ecommerce-recommendation-system

# Or initialize new repo
git init ecommerce-recommendation-system
cd ecommerce-recommendation-system
```

---

## 🔧 Part 2: Setup Environment

### 1. Create Virtual Environment

```bash
# Create virtual environment
python -m venv venv

# Activate it
# On macOS/Linux:
source venv/bin/activate

# On Windows:
venv\Scripts\activate

# Your prompt should change to show (venv)
```

### 2. Install Python Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

This will install:
- Flask (web framework)
- Kafka-python (Kafka client)
- Apache-Flink (PyFlink)
- Redis (Redis client)
- Pandas, NumPy (data processing)
- Pytest (testing)

**Note**: Installation might take 5-10 minutes.

### 3. Download Dataset (Optional but Recommended)

1. Go to: https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset
2. Download these files:
   - `events.csv`
   - `item_properties.csv`
   - `category_tree.csv`
3. Place them in the `data/` folder

---

## 🐳 Part 3: Start Infrastructure

### 1. Start Docker Services

```bash
# Make sure Docker is running first
docker ps

# Start all services (Kafka, Zookeeper, Redis)
docker-compose up -d

# Verify containers are running
docker ps

# You should see:
# - zookeeper
# - kafka
# - redis
# - redis-commander
```

### 2. Wait for Services to Initialize

```bash
# Wait 30 seconds for Kafka to be ready
sleep 30
```

### 3. Create Kafka Topics

```bash
# Make script executable (on macOS/Linux)
chmod +x scripts/setup_kafka.sh

# Run setup script
bash scripts/setup_kafka.sh

# You should see: "✓ Topic 'user-events' created successfully"
```

**Windows users**: Run these commands instead:
```bash
docker exec kafka kafka-topics --create \
  --topic user-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists
```

### 4. Load Dataset (Optional)

```bash
# Make sure virtual environment is activated
python scripts/load_data.py

# You should see progress messages
```

---

## ▶️ Part 4: Run the System

You need **4 terminal windows** (all in the project directory):

### Terminal 1: Event Producer API

```bash
# Activate virtual environment
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Run event producer
python -m event_producer.app

# You should see:
# "Starting Event Producer API on http://0.0.0.0:5000"
# Leave this running!
```

### Terminal 2: Flink Processor

```bash
# Activate virtual environment
source venv/bin/activate

# Run Flink processor
python -m flink_processor.simple_job

# You should see:
# "Starting Flink Recommendation Job"
# "Waiting for events..."
# Leave this running!
```

### Terminal 3: Recommendation API

```bash
# Activate virtual environment
source venv/bin/activate

# Run recommendation API
python -m recommendation_api.app

# You should see:
# "Starting Recommendation API on http://0.0.0.0:5001"
# Leave this running!
```

### Terminal 4: Testing

```bash
# Activate virtual environment
source venv/bin/activate

# Run test script
python scripts/test_system.py

# You should see test results
```

---

## 🧪 Part 5: Test the System

### Manual Testing with curl

```bash
# 1. Track a user event
curl -X POST http://localhost:5000/track-event \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user_123",
    "item_id": "item_456",
    "event_type": "view"
  }'

# Should return: {"status": "success"}

# 2. Send more events to build recommendations
curl -X POST http://localhost:5000/track-event \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user_123",
    "item_id": "item_457",
    "event_type": "view"
  }'

# 3. Wait 2-3 seconds for processing

# 4. Get recommendations
curl http://localhost:5000/recommendations/user_123

# Should return: {"user_id": "user_123", "recommendations": [...]}
```

### Using Python Script

```python
import requests

# Send event
response = requests.post('http://localhost:5000/track-event', json={
    'user_id': 'test_user',
    'item_id': 'test_item',
    'event_type': 'view'
})
print(response.json())

# Get recommendations
response = requests.get('http://localhost:5001/recommendations/test_user')
print(response.json())
```

---

## 📊 Part 6: Monitor the System

### 1. Redis Commander (Web UI)

```bash
# Open in browser:
http://localhost:8081

# You can see:
# - user:*:history (user viewing history)
# - user:*:recommendations (user recommendations)
# - item:*:recommendations (item similarities)
```

### 2. Check Kafka Messages

```bash
# View messages in Kafka topic
docker exec kafka kafka-console-consumer \
  --topic user-events \
  --bootstrap-server localhost:9092 \
  --from-beginning

# Press Ctrl+C to stop
```

### 3. Check Logs

All running services show logs in their terminals. Watch for:
- Event Producer: "Event tracked"
- Flink Processor: "Processing: User X -> view -> Item Y"
- Recommendation API: "Served N recommendations"

---

## 🐛 Troubleshooting

### Problem: "Connection refused" errors

**Solution**: Make sure Docker containers are running
```bash
docker ps
docker-compose restart
```

### Problem: No recommendations returned

**Reasons**:
1. Not enough events sent (send at least 5-10 events)
2. Flink processor not running (check Terminal 2)
3. Need to wait 2-3 seconds for processing

**Solution**:
```bash
# Send multiple events
for i in {1..10}; do
  curl -X POST http://localhost:5000/track-event \
    -H "Content-Type: application/json" \
    -d "{\"user_id\": \"user_1\", \"item_id\": \"item_$i\", \"event_type\": \"view\"}"
  sleep 0.5
done

# Wait 5 seconds
sleep 5

# Check recommendations
curl http://localhost:5001/recommendations/user_1
```

### Problem: Kafka topic errors

**Solution**: Recreate topics
```bash
docker exec kafka kafka-topics --delete --topic user-events --bootstrap-server localhost:9092
bash scripts/setup_kafka.sh
```

### Problem: "Module not found" errors

**Solution**: Make sure virtual environment is activated
```bash
# Check if (venv) appears in prompt
# If not:
source venv/bin/activate  # macOS/Linux
venv\Scripts\activate     # Windows

# Reinstall dependencies
pip install -r requirements.txt
```

---

## 🎯 Quick Reference

### Start Everything
```bash
# 1. Start Docker
docker-compose up -d

# 2. Setup (first time only)
bash scripts/setup_kafka.sh
python scripts/load_data.py

# 3. Run services (4 terminals)
python -m event_producer.app
python -m flink_processor.simple_job
python -m recommendation_api.app
python scripts/test_system.py
```

### Stop Everything
```bash
# Stop Python services: Press Ctrl+C in each terminal

# Stop Docker
docker-compose down
```

### Check Status
```bash
# Docker containers
docker ps

# Redis data
docker exec redis redis-cli KEYS "*"

# Kafka topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

---

## 📚 Next Steps

1. **Experiment**:
   - Send different types of events
   - Try batch requests
   - Check item recommendations

2. **Customize**:
   - Modify recommendation algorithms in `flink_processor/recommendation_engine.py`
   - Add new API endpoints in `recommendation_api/app.py`
   - Implement advanced features

3. **Scale**:
   - Increase Flink parallelism
   - Add more Kafka partitions
   - Deploy to production

4. **Learn More**:
   - Read Kafka documentation
   - Explore PyFlink features
   - Study recommendation algorithms

---

## 🆘 Getting Help

If you encounter issues:

1. **Check logs** in each terminal window
2. **View this guide** for troubleshooting section
3. **Verify setup**:
   ```bash
   # Check Python version
   python --version
   
   # Check Docker
   docker ps
   
   # Check virtual environment
   which python  # Should point to venv
   ```

---

## ✅ Success Checklist

- [ ] All Docker containers running (`docker ps` shows 4 containers)
- [ ] Virtual environment activated (prompt shows `(venv)`)
- [ ] All 3 Python services running (Event Producer, Flink, API)
- [ ] Can send events successfully
- [ ] Can retrieve recommendations
- [ ] Redis Commander accessible at http://localhost:8081

**Congratulations! 🎉 Your real-time recommendation system is now running!**
