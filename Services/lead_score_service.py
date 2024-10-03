from flask import Blueprint, jsonify, request
import joblib
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer

lead = Blueprint('leadScore', __name__)

# URLs of the model file chunks on GitHub
MODEL_CHUNKS_URLS = [
    'lead_scoring_model.pkl.part0'
]
# To reload the dictionary from the file
scores_dict = joblib.load('scores_dict.pkl')

independent_features = [
    'leadsource', 'Custom_UTM_U', 'Brandname1', 'Age_Bucket', 'Income_Bucket',
    'isbirthday', 'Income_Flag', 'IsRepeat', 'IsSelfSelect', 'Previous_Booked',
    'compare_flag', 'limited_flag', 'trop_flag'
]

score_cols = ['leadsource_score', 'Custom_UTM_U_score', 'Brandname1_score', 'Age_Bucket_score', 
              'Income_Bucket_score', 'isbirthday_score', 'Income_Flag_score', 'IsRepeat_score', 
              'IsSelfSelect_score', 'Previous_Booked_score', 'compare_flag_score', 
              'limited_flag_score', 'trop_flag_score']


def preprocess_data(data, scores_dict):
    # Handle invalid(0 and negative Income) values in 'AnnualIncome' if the column exists
    if 'AnnualIncome' in data.columns:
        data['AnnualIncome'].replace([0, -float('inf')], 'other', inplace=True)

    # Bucketing Column 'Age'
    if 'Age' in data.columns:
        data['Age_Bucket'] = pd.cut(
            data['Age'],
            bins=[0, 25, 30, 40, float('inf')],
            labels=['less than 25', '25-30', '30-40', '40 and above'],
            right=False
        )
        # Update categories to include 'other'
        data['Age_Bucket'] = data['Age_Bucket'].cat.add_categories(['other'])
        data['Age_Bucket'].fillna('other', inplace=True)

    for idx, value in enumerate(data['AnnualIncome']):
        if pd.isna(value):
            data.at[idx, 'AnnualIncome'] = np.nan
        else:
            value = str(value).lower()
            # Remove any non-numeric characters except for decimal points
            value = ''.join(c for c in value if c.isdigit() or c in ['.', ','])
            if 'lakh' in value:
                data.at[idx, 'AnnualIncome'] = float(value.replace('lakh', '').replace(',', '').strip()) * 100000
            elif 'crore' in value:
                data.at[idx, 'AnnualIncome'] = float(value.replace('crore', '').replace(',', '').strip()) * 10000000
            else:
                try:
                    data.at[idx, 'AnnualIncome'] = float(value)
                except ValueError:
                    data.at[idx, 'AnnualIncome'] = np.nan

    # Bucketing 'AnnualIncome'
    if 'AnnualIncome' in data.columns:
        data['Income_Bucket'] = pd.cut(
            data['AnnualIncome'].astype(float),
            bins=[-float('inf'), 0, 200000, 500000, 1500000, float('inf')],
            labels=['other', 'less than 2 lacs', '2-5 lacs', '5-15 lacs', '15 lacs and above'],
            right=False
        )
        # Handle any potential issues with adding categories
        if not data['Income_Bucket'].cat.categories.isin(['other']).any():
            data['Income_Bucket'] = data['Income_Bucket'].cat.add_categories(['other'])
        data['Income_Bucket'].fillna('other', inplace=True)

    # Dropping original Age and AnnualIncome columns if they exist
    data.drop(columns=[col for col in ['Age', 'AnnualIncome'] if col in data.columns], inplace=True)

    # print("scoring started")
    # Apply mean APE scores to the categorical columns
    for column, scores in scores_dict.items():
        if column in data.columns:
            # Ensure the column is treated as categorical for merging
            data[column] = data[column].astype(str)
            scores[column] = scores[column].astype(str)
            data = pd.merge(data, scores, on=column, how='left', suffixes=('', '_score'))
            print(column, scores)
    # print("data merged")
    # Drop original categorical columns and keep only the score columns
    categorical_scores = [f'{col}_score' for col in scores_dict.keys()]
    data = data[categorical_scores]
    print(data.columns)

    return data

# Define the preprocessing pipeline
def create_preprocessor_pipeline(independent_features, scores_dict):
    def preprocess_transformer(data):
        return preprocess_data(data, scores_dict)

    return Pipeline(steps=[
        ('preprocess', FunctionTransformer(preprocess_transformer, validate=False)),
        ('scaler', MinMaxScaler())
    ])

# Assemble the model from chunks and load it
def download_and_assemble_model():
    model_file_path = 'lead_scoring_model.pkl'
    with open(model_file_path, 'wb') as f:
        for chunk in MODEL_CHUNKS_URLS:
            with open(chunk, 'rb') as chunk_file:
                f.write(chunk_file.read())
    
    # Load the complete model after assembling
    return joblib.load(model_file_path)

# Load the pre-trained model and preprocessor
model = download_and_assemble_model()

pipeline = create_preprocessor_pipeline(independent_features, scores_dict)

# Define the allocation function based on features
def allocate_team_based_on_features(row):
    if row['AnnualIncome'] > 1500000:
        return "HNITeam"
    elif row['IsRepeat'] > 0:
        return "RepeatTeam"
    elif row['isbirthday'] == 1:
        return "BirthDayTeam"
    elif row['isctc'] == 1:
        return "CTCTeam"
    elif row['Age'] < 30:
        return "AgeBelow30Team"
    elif 30 <= row['Age'] <= 50:
        return "AgeAbove30Team"
    else:
        return 7

@lead.route('/score', methods=['POST'])
def score_lead():
    try:
        lead_data = request.json
        if not lead_data:
            raise ValueError("Invalid input data")
        lead_df = pd.DataFrame([lead_data])
        print(lead_df.head())
        
        # Preprocess the input data
        processed_data_array = pipeline.fit_transform(lead_df)
        processed_data = pd.DataFrame(processed_data_array, columns=score_cols)
        print("preprocess_data")
        
        # Predict the probability
        probability = model.predict(processed_data)[0]
        print(probability)
        
        # Scale the score
        scaled_score = np.interp(probability, (0, 1), (1, 1000))[0]
        print(scaled_score)
        
        # Determine the grade
        # grade_thresholds = np.percentile(scaled_score, [250, 500, 750])
        grade = 1#np.digitize(scaled_score, grade_thresholds)
        
        # Allocate team
        team = allocate_team_based_on_features(lead_data)
        
        #send_to_allocate(new_lead,score)
        
        return jsonify({
            "score": scaled_score,
            "grade": int(grade + 1),  # Grades start from 1
            "team": team
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400
