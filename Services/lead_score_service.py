from flask import Blueprint, jsonify, request
import joblib
import numpy as np
import pandas as pd
import json
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
from pymongo import MongoClient
from Services.agent_allocation_service import agent_allocation_helper

lead = Blueprint('leadScore', __name__)

# Initialize MongoDB client
url='mongodb+srv://user:leadSc0re@cluster0.2cfpn.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
mongo_client = MongoClient(url)
db = mongo_client['LeadDB'] 
collection = db['LeadRawData']

# URLs of the model file chunks on GitHub
MODEL_CHUNKS_URLS = [
    'lead_scoring_model.pkl.part0'
]
# To reload the dictionary from the file
scores_dict = joblib.load('scores_dict.pkl')

scaler = joblib.load('scaler.pkl')

# Updating the list of columns to consider
independent_features = [
    'leadsource', 'Custom_UTM_U', 'Brandname1', 'Age_Bucket', 'Income_Bucket',
    'isbirthday', 'Income_Flag', 'IsRepeat', 'IsSelfSelect', 'Previous_Booked',
    'compare_flag', 'limited_flag', 'trop_flag','TobaccoUser','ProfessionType','educationQualificationId'
]


def preprocess_data(data, scores_dict):
    data = data.reindex(
        columns=data.columns.union(
            ['Booking', 'APE', 'educationQualificationId', 'TobaccoUser', 'ProfessionType'], sort=False)).fillna(
                {'Booking': 0, 'APE': 0, 'educationQualificationId': 0, 'TobaccoUser': 0, 'ProfessionType': 0})

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
            # Convert values to string for consistent lookups
            data[column] = data[column].astype(str)
            scores[column] = scores[column].astype(str)
            
            # Perform the initial merge
            data = pd.merge(data, scores, on=column, how='left', suffixes=('', '_score'))
            
            # Identify rows where the score is NaN
            nan_rows = data[f'{column}_score'].isna()
            
            # Go back to the original column and handle NaN cases
            for idx in data[nan_rows].index:
                original_value = data.loc[idx, column]
                
                # Check if the original value is an integer (like '0' or '1') or a float-like string ('0.0', '1.0')
                try:
                    numeric_value = float(original_value)
                    if numeric_value.is_integer():
                        int_value_str = str(int(numeric_value))  # Integer form without .0
                        float_value_str = f"{int(numeric_value)}.0"  # Float form with .0
            
                        # Try to find a match in scores_dict (either as '0' or '0.0')
                        if int_value_str in scores[column].values:
                            data.loc[idx, f'{column}_score'] = scores.loc[scores[column] == int_value_str, f'{column}_score'].values[0]
                        elif float_value_str in scores[column].values:
                            data.loc[idx, f'{column}_score'] = scores.loc[scores[column] == float_value_str, f'{column}_score'].values[0]
                except ValueError:
                    # If it's not a number, continue without changes
                    pass
    
    categorical_scores = [f'{col}_score' for col in scores_dict.keys()]
    data = data[categorical_scores]

    return data

# Define the preprocessing pipeline
def create_preprocessor_pipeline(independent_features, scores_dict):
    def preprocess_transformer(data):
        return preprocess_data(data, scores_dict)

    return Pipeline(steps=[
        ('preprocess', FunctionTransformer(preprocess_transformer, validate=False))
    ])

# Assemble the model from chunks and load it
def assemble_model():
    model_file_path = 'lead_scoring_model.pkl'
    with open(model_file_path, 'wb') as f:
        for chunk in MODEL_CHUNKS_URLS:
            with open(chunk, 'rb') as chunk_file:
                f.write(chunk_file.read())
    
    # Load the complete model after assembling
    return joblib.load(model_file_path)

# Load the pre-trained model and preprocessor
model = assemble_model()

# Create the preprocessing pipeline
pipeline = create_preprocessor_pipeline(independent_features, scores_dict)

def lead_grade(lead_score):
    if lead_score > 1832:
        return 1
    elif lead_score > 933:
        return 2
    elif lead_score > 455:
        return 3
    else:
        return 4

# Define the allocation function based on features
def allocate_team_based_on_features(row):
    if row['AnnualIncome'] > 1500000:
        return "HNI"
    elif row['IsRepeat'] > 0:
        return "BKGS"
    elif row['isbirthday'] == 1:
        return "BDAY"
    elif row['isctc'] == 1:
        return "Marathi-APE"
    elif row['Age'] < 30:
        return "APE Salaried"
    elif 30 <= row['Age'] <= 50:
        return "HNI"
    else:
        return "OTHER"


@lead.route('/testMongo', methods=['GET'])
def testMongo():
    collection.insert_one({"name": "test", "value": 123})
    return "Connected to MongoDB!"

@lead.route('/score', methods=['POST'])
def score_lead():
    try:
        lead_data = request.json
        if not lead_data:
            raise ValueError("Invalid input data")
        
        # Store raw lead data in LeadRawData collection
        lead_id = lead_data.get('leadid')
        if not lead_id:
            raise ValueError("Lead ID is missing")
        
        # Upsert into LeadRawData collection
        result = collection.update_one(
            {'_id': lead_id},
            {'$set': lead_data},  # Use $set to update fields in the document
            upsert=True  # Insert the document if it doesn't exist
        )
        
        lead_df = pd.DataFrame([lead_data])
        print(lead_df.head())
        
        # Preprocess the input data
        processed_data = pipeline.fit_transform(lead_df)
        processed_data_df = processed_data.rename(columns=lambda col: col.replace('_score', ''))
        processed_data_scaled = scaler.transform(processed_data_df)
        print(processed_data_scaled)
        
        # Predict the probability
        lead_score = float(model.predict(processed_data_scaled)[0][0])
        print(lead_score)
        print(type(lead_score))
        
        # Determine the grade
        grade = lead_grade(lead_score)
        print(grade)
        
        # Allocate team
        team = allocate_team_based_on_features(lead_data)
        print(team)
        
        allocationResponse = "allocation response"
        try:
            jsonRespose={
                "grade":grade,
                "team":team,
                "leadid":lead_id
            }
            allocationResponse = json.dumps(agent_allocation_helper(jsonRespose)[0].get_json())
        except:
            allocationResponse = "allocation failed"

        response = jsonify({
            "score": lead_score,
            "grade": grade,
            "team": team,
            "status": allocationResponse
        })
        print(jsonRespose)
        
        return response
    except Exception as e:
        return jsonify({"error": str(e)}), 400
