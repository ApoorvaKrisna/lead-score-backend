from flask import Flask, jsonify
from flask_cors import CORS
from Services.lead_score_service import lead
from Services.agent_allocation_service import agent


# Initialize Flask app
app = Flask(__name__)

app.register_blueprint(lead)
app.register_blueprint(agent)
'''
app.register_blueprint(notify)
'''

CORS(app)
        
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200
    
if __name__ == "__main__":
    # Run the app on the port Render expects
    app.run(host='0.0.0.0', port=5000)
