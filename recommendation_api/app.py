"""Flask API for serving recommendations"""
from flask import Flask, jsonify, request
from config.redis_config import get_redis_client
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
redis_client = get_redis_client()

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy"}), 200

@app.route('/recommendations/', methods=['GET'])
def get_user_recommendations(user_id):
    """Get personalized recommendations for a user"""
    try:
        # Get limit from query params (default 10)
        limit = request.args.get('limit', default=10, type=int)
        
        # Fetch from Redis
        recommendations = redis_client.lrange(
            f"user:{user_id}:recommendations", 
            0, 
            limit - 1
        )
        
        if not recommendations:
            logger.warning(f"No recommendations found for user {user_id}")
            return jsonify({
                "user_id": user_id,
                "recommendations": [],
                "message": "No recommendations available yet"
            }), 200
        
        logger.info(f"Served {len(recommendations)} recommendations for user {user_id}")
        
        return jsonify({
            "user_id": user_id,
            "recommendations": recommendations,
            "count": len(recommendations)
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching recommendations: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/recommendations/item/', methods=['GET'])
def get_item_recommendations(item_id):
    """Get recommendations for an item (similar items)"""
    try:
        limit = request.args.get('limit', default=10, type=int)
        
        recommendations = redis_client.lrange(
            f"item:{item_id}:recommendations",
            0,
            limit - 1
        )
        
        return jsonify({
            "item_id": item_id,
            "recommendations": recommendations,
            "count": len(recommendations)
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching item recommendations: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)