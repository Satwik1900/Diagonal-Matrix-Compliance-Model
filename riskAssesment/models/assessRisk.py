import psycopg2
import json
from flask import request
from datetime import datetime
from numpy import nan
import joblib
import pandas as pd
import numpy as np
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(parent_dir)

#load label encoders
label_encoders_path = os.path.join(current_dir, 'label_encoders.joblib')
with open(label_encoders_path, 'rb') as file:
    loaded_label_encoders = joblib.load(file)

# load risk assessment model
risk_model_path = os.path.join(current_dir, 'risk.sav')
with open(risk_model_path, 'rb') as file:
    model = joblib.load(file)

feature_names = ['country_of_incorporation', 'country_of_operation',
        'country_of_government', 'country_of_nationality',
        'country_of_residence', 'legal_structure', 'industry',
        'regulatory_county', 'source_of_wealth', 'product_name',
        'purpose_of_account', 'high_risk_transactions', 'authorized_signers',
        'management', 'owners', 'government_income']

def assessKycRisk():
    target_id = request.headers.get('ID')

    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="complimatrix",
        user="nocobase",
        password="nocobase"
    )

    cursor = conn.cursor()

    cursor.execute("SELECT nationality, current_country, source_of_wealth, purpose_of_account, high_risk_transactions FROM kyc WHERE id=%s;", (target_id,))
    columns = [column[0] for column in cursor.description]
    data = cursor.fetchall()

    dict_data = {}
    for i, value in enumerate(data[0]):
        if isinstance(value, datetime):
            value = value.isoformat()
        dict_data[columns[i]] = value

    df = pd.DataFrame.from_dict([dict_data])
    df.rename(columns = {'nationality':'country_of_nationality','current_country':'country_of_residence'}, inplace = True)
    for col in feature_names:
        if col not in df.columns:
            df[col] = nan
    df = df.reindex(columns=feature_names)
    df = df.fillna('Not Available')
    for column in df.columns:
        loaded_label_encoder = loaded_label_encoders[column]
        try:
            df[column] = loaded_label_encoder.transform(df[column])
        except ValueError:
            df[column] = "Not Available"

    x = model.predict(df)
    output = 'High' if x==2 else 'Medium' if x==1 else 'Low'

    current_date = datetime.now()
    formatted_date = current_date.strftime('%d/%m/%Y')

    cursor.execute("UPDATE kyc SET risk = %s, last_assessment_date = %s WHERE id = %s", (output, formatted_date, target_id))
    conn.commit()

def assessKybRisk():
    target_id = request.headers.get('ID')

    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="complimatrix",
        user="nocobase",
        password="nocobase"
    )

    cursor = conn.cursor()

    cursor.execute("SELECT * FROM kyb WHERE id=%s;", (target_id,))
    columns = [column[0] for column in cursor.description]
    data = cursor.fetchall()

    dict_data = {}
    for i, value in enumerate(data[0]):
        if isinstance(value, datetime):
            value = value.isoformat()
        dict_data[columns[i]] = value

    df = pd.DataFrame.from_dict([dict_data])
    df.rename(columns = {'countries_of_operation':'country_of_operation','Nature_of_business':'industry'}, inplace = True)
    for col in feature_names:
        if col not in df.columns:
            df[col] = nan
    df = df.reindex(columns=feature_names)
    df = df.fillna('Not Available')
    for column in df.columns:
        loaded_label_encoder = loaded_label_encoders[column]
        if(column == 'country_of_operation'):
            df[column] = df[column][0]
        try:
            df[column] = loaded_label_encoder.transform(df[column])
        except ValueError:
            df[column] = "Not Available"

    x = model.predict(df)
    output = 'High' if x==2 else 'Medium' if x==1 else 'Low'

    current_date = datetime.now()
    formatted_date = current_date.strftime('%d/%m/%Y')

    cursor.execute("UPDATE kyb SET risk = %s, last_assessment_date = %s WHERE id = %s", (output, formatted_date, target_id))
    conn.commit()
