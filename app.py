from flask import Flask, render_template, jsonify
from flask_cors import CORS
from routes.schema import schema_bp
from routes.ingestion import ingestion_bp
from routes.query import query_bp
import logging
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime

# Configure logging
if not os.path.exists('logs'):
    os.mkdir('logs')

file_handler = RotatingFileHandler('logs/app.log', maxBytes=10240000, backupCount=10)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
))
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

app = Flask(__name__, template_folder="templates", static_folder="static")
app.logger.addHandler(file_handler)
app.logger.addHandler(console_handler)
app.logger.setLevel(logging.INFO)
app.logger.info('NLP Query Engine startup')

# CORS configuration
CORS(app, resources={
    r"/api/*": {
        "origins": "*",
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# Configuration
app.config.update(
    MAX_CONTENT_LENGTH=50 * 1024 * 1024,  # 50MB max file size
    JSON_SORT_KEYS=False,
    JSONIFY_PRETTYPRINT_REGULAR=True
)

# Register blueprints
app.register_blueprint(schema_bp, url_prefix="/api")
app.register_blueprint(ingestion_bp, url_prefix="/api")
app.register_blueprint(query_bp, url_prefix="/api")

# Global metrics
app_metrics = {
    'start_time': datetime.now(),
    'total_requests': 0,
    'total_errors': 0,
    'active_connections': 0
}

@app.before_request
def before_request():
    """Track request metrics"""
    app_metrics['total_requests'] += 1

@app.errorhandler(404)
def not_found(error):
    return jsonify({"ok": False, "error": "not_found", "message": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    app.logger.error(f'Server Error: {error}')
    app_metrics['total_errors'] += 1
    return jsonify({"ok": False, "error": "internal_error", "message": "Internal server error"}), 500

@app.errorhandler(413)
def request_entity_too_large(error):
    return jsonify({"ok": False, "error": "file_too_large", "message": "File size exceeds 50MB limit"}), 413

# Serve frontend
@app.route("/")
def home():
    return render_template("index.html")

@app.route("/api/health", methods=["GET"])
def health_check():
    """Health check endpoint for monitoring"""
    uptime = (datetime.now() - app_metrics['start_time']).total_seconds()
    return jsonify({
        "ok": True,
        "status": "healthy",
        "uptime_seconds": int(uptime),
        "metrics": {
            "total_requests": app_metrics['total_requests'],
            "total_errors": app_metrics['total_errors'],
            "error_rate": round(app_metrics['total_errors'] / max(app_metrics['total_requests'], 1) * 100, 2)
        }
    })

@app.route("/api/metrics", methods=["GET"])
def get_metrics():
    """Get application metrics"""
    from routes.query import query_cache
    from routes.ingestion import get_ingestion_stats
    
    return jsonify({
        "ok": True,
        "metrics": {
            "app": app_metrics,
            "cache": query_cache.get_stats(),
            "ingestion": get_ingestion_stats()
        }
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True)