import psycopg2
import json
from flask import Flask, request
import joblib
import pandas as pd
import os
import sys
from sklearn.preprocessing import LabelEncoder

le = LabelEncoder()
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(parent_dir)

file_path = os.path.join(current_dir, 'Transaction_Monitoring.sav')
with open(file_path, 'rb') as file:
    model = joblib.load(file)

def Transaction():
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="complimatrix",
        user="nocobase",
        password="nocobase"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM transaction_monitoring WHERE output IS NULL AND transaction_amount IS NOT NULL")
    columns = [column[0] for column in cursor.description]
    data = cursor.fetchall()
    json_data = []
    features = []
    column_names = ['f_transactions_kyc','f_kyb_transactiondetails','transaction_amount', 'employment_status', 'yearly_income_level', 'credit_score', 'transaction_location', 'transaction_currency', 'transaction_destination', 'transaction_type', 'transaction_channel', 'yearly_transaction_frequency', 'card_usage_type', 'card_network', 'card_usage_history', 'card_usage_frequency', 'card_limits', 'card_usage_category', 'yearly_maximum_amount', 'yearly_average_amount']
    
    for row in data:
        item_data = {}
        json_row = dict(zip(columns, row))
        json_data.append(json_row)

        for column in column_names:
            feature = json_row.get(column)
            item_data[column] = feature if feature is not None else 0.0

        df = pd.DataFrame(item_data, index=[0])
        kyc_alert=int(df['f_transactions_kyc'].iloc[0])
        kyb_alert=int(df['f_kyb_transactiondetails'].iloc[0])
        
        drop_columns = ['f_transactions_kyc','f_kyb_transactiondetails','transaction_location', 'transaction_currency', 'transaction_destination', 'transaction_type', 'transaction_channel', 'card_usage_history', 'card_usage_type', 'card_network', 'card_usage_category']
        df = df.drop(columns=[col for col in drop_columns if col in df.columns], axis=1)
        df['yearly_income_level'] = df['yearly_income_level'].replace({'High': 2, 'Medium': 1, 'Low': 0})
        df['employment_status'] = df['employment_status'].map({'Employed': 2, 'Self-Employed': 1, 'Unemployed': 0}).astype(float)

        Output = model.predict(df)
        if (df['transaction_amount'] > 1.5 * df['yearly_maximum_amount']).any():
            Alert = "Transaction amount much higher than the maximum amount in the year"
        elif (df['credit_score'] < 600).any():
            Alert = "Credit score too low"
        else:
            Alert = "No alerts"

        if Output == 1:
            risk = "This is a fraudulent transaction"
        else:
            risk = "This is a non-fraudulent transaction"

        Id = str(json_row.get('id'))
        cursor.execute("UPDATE transaction_monitoring SET alert = %s WHERE id = %s", (Alert, int(Id)))
        cursor.execute("UPDATE transaction_monitoring SET output = %s WHERE id = %s", (risk, int(Id)))

        if Alert != "No alerts":
            cursor.execute("SELECT id FROM alert_management ORDER BY id DESC LIMIT 1")
            row = cursor.fetchone()
            max_id = row[0] if row else 0

            next_id = int(max_id) + 1

            cursor.execute("INSERT INTO alert_management (id, f_transaction_alert) VALUES (%s, %s)", (next_id, int(Id)))
            if kyc_alert is not None:
                cursor.execute("UPDATE alert_management SET alert_kyc = %s WHERE id = %s", (kyc_alert, next_id))
            if kyb_alert is not None:
                cursor.execute("UPDATE alert_management SET alert_kyb = %s WHERE id = %s", (kyb_alert, next_id))
        conn.commit()

