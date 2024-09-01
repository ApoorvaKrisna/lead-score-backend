from flask import Flask, request, jsonify
import joblib
import numpy as np
import pandas as pd
import requests
import io

# Initialize Flask app
app = Flask(__name__)

# URLs of the model file chunks on GitHub
MODEL_CHUNKS_URLS = [
    'https://github.com/ApoorvaKrisna/lead-score-backend/raw/main/lead_scoring_model.pkl.part0',
    'https://github.com/ApoorvaKrisna/lead-score-backend/raw/main/lead_scoring_model.pkl.part1',
    'https://github.com/ApoorvaKrisna/lead-score-backend/raw/main/lead_scoring_model.pkl.part2'
]
PREPROCESSOR_URL = 'https://github.com/ApoorvaKrisna/lead-score-backend/raw/main/preprocessor.pkl'

# Load model and preprocessor from GitHub
def load_file_from_github(url):
    response = requests.get(url)
    response.raise_for_status()
    return joblib.load(io.BytesIO(response.content))

# Assemble the model from chunks and load it
def download_and_assemble_model():
    model_file_path = 'lead_scoring_model.pkl'
    with open(model_file_path, 'wb') as f:
        for url in MODEL_CHUNKS_URLS:
            response = requests.get(url)
            response.raise_for_status()
            f.write(response.content)
    
    return joblib.load(model_file_path)

# Load the pre-trained model and preprocessor
model = download_and_assemble_model()
preprocessor = load_file_from_github(PREPROCESSOR_URL)

# Define the allocation function based on features
def allocate_team_based_on_features(row):
    if row['balance'] > 50000:
        return 1
    elif row['previous'] > 0:
        return 2
    elif row['poutcome'] == 'success':
        return 3
    elif row['job'] in ['management', 'entrepreneur', 'admin.']:
        return 4
    elif row['age'] < 30:
        return 5
    elif 30 <= row['age'] <= 50:
        return 6
    else:
        return 7

@app.route('/score', methods=['POST'])
def score_lead():
    try:
        lead_data = request.json
        lead_df = pd.DataFrame([lead_data])
        
        # Preprocess the input data
        processed_data = preprocessor.transform(lead_df)
        
        # Predict the probability
        probability = model.predict_proba(processed_data)[:, 1]
        
        # Scale the score
        scaled_score = np.interp(probability, (0, 1), (1, 1000))[0]
        
        # Determine the grade
        grade_thresholds = np.percentile(processed_data, [25, 50, 75])
        grade = np.digitize(scaled_score, grade_thresholds)
        
        # Allocate team
        team = allocate_team_based_on_features(lead_data)
        
        return jsonify({
            "score": scaled_score,
            "grade": int(grade + 1),  # Grades start from 1
            "team": team
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400

if __name__ == "__main__":
    # Run the app on the port Render expects
    app.run(host='0.0.0.0', port=5000)
