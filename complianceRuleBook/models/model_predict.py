import psycopg2
import json
from flask import request
from datetime import datetime
from numpy import nan
import joblib
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import precision_recall_fscore_support, matthews_corrcoef
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(parent_dir)
from complianceRuleBook.data import sampleDataSets
from complianceRuleBook.models import nlp

# load label encoders
label_encoders_path = os.path.join(current_dir, 'label_encoders.joblib')
with open(label_encoders_path, 'rb') as file:
    loaded_label_encoders = joblib.load(file)

# load compliance model
compliance_model_path = os.path.join(current_dir, 'compliance_le.sav')
with open(compliance_model_path, 'rb') as file:
    model = joblib.load(file)

feature_names = ['full_cust_names',
                'residential_address',
                'source_of_funds',
                'occ_business_act',
                'purpose_of_transaction',
                'country_of_citizenship',
                'country_of_residence',
                'address_of_company',
                'principal_place_of_operation',
                'type_of_company',
                'type_of_trust',
                'country_of_establishment',
                'full_address_of_head_office',
                'State_Country_Territory_of_incorporation',
                'date_of_incorporation',
                'objects_of_entity',
                'name_of_chairman',
                'info_in_official_exchange',
                'info_in_domestic_exchange',
                'nature_of_business_by_the_company',
                'any_trustee_is_individual_or_company']

# This is for Adding/Updating/Deleting Rules in complinace Rule-Book
def modifyRuleBook():
    target_id = request.headers.get('ID')

    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="complimatrix",
        user="nocobase",
        password="nocobase"
    )

    cursor = conn.cursor()

    cursor.execute("SELECT * FROM compliance;")
    columns = [column[0] for column in cursor.description]
    data = cursor.fetchall()

    json_data = []
    for row in data:
        converted_row = []
        for item in row:
            if isinstance(item, datetime):
                converted_row.append(item.isoformat())
            else:
                converted_row.append(item)
        json_row = dict(zip(columns, converted_row))
        json_data.append(json_row)

    for item in json_data:
        if item.get('id') == int(target_id):
            text = item['compliance_rule']
            nlpOutput = nlp.extract_rules(text)
            is_rule = nlpOutput[0]
            if is_rule == 1:  # input is of type Add new rule <rule> or delete rule <rule> or update rule <rule>
                rule = nlpOutput[1]
                action = nlpOutput[2]
                if action == 'ADD':
                    cursor.execute("UPDATE compliance SET compliance_rule = %s WHERE id = %s", (rule, target_id))
                    conn.commit()
                elif action == 'DELETE':
                    rule = rule.lower()
                    cursor.execute("DELETE FROM compliance WHERE LOWER(compliance_rule) = %s", (rule,))
                    cursor.execute("DELETE FROM compliance WHERE id = %s", (target_id,))
                    conn.commit()
                else:
                    condition = nlpOutput[3]
                    amount = nlpOutput[4]
                    if amount and amount > 0:  # Rule contains amount
                        comparision = nlpOutput[5]  # above or below
                        for data in json_data:
                            oldRuleId = data.get('id')
                            oldRule = data['compliance_rule']
                            if oldRule and condition.lower() and comparision.lower() in oldRule:
                                oldRuleAmount = nlp.extract_amount(oldRule)
                                if amount == oldRuleAmount:
                                    cursor.execute("UPDATE compliance SET compliance_rule = %s WHERE id = %s",(rule, oldRuleId))
                                    cursor.execute("DELETE FROM compliance WHERE id = %s", (target_id,))
                                    conn.commit()
                                    break
                    else:  # Rule does not contain amount
                        for data in json_data:
                            oldRuleId = data.get('id')
                            oldRule = data['compliance_rule']
                            if condition.lower() in oldRule:
                                cursor.execute("UPDATE compliance SET compliance_rule = %s WHERE id = %s",(rule, oldRuleId))
                                cursor.execute("DELETE FROM compliance WHERE id = %s", (target_id,))
                                conn.commit()
                                break

            elif is_rule == 0:  # transaction details are given as input
                rules = nlpOutput[1]
                name = nlpOutput[2]

                cursor.execute("DELETE FROM compliance WHERE id = %s", (target_id,))
                conn.commit()

                df = pd.DataFrame.from_dict(sampleDataSets.cust_data[name])
                cust_type = df['CUSTOMER TYPE'].iloc[0]
                for col in df.columns:
                    if col not in rules:
                        df[col] = nan
                df.drop(columns=['unique_identification_number', 'VALUE OF TRANSACTION', 'MEASURES REQUIRED',
                                     'CUSTOMER TYPE', 'CUSTOMER ACTIVITY'], axis=1, inplace=True)
                df = df.fillna('Not Available')
                df = df.reindex(columns=feature_names)

                for column in df.columns:
                    loaded_label_encoder = loaded_label_encoders[column]
                    df[column] = loaded_label_encoder.transform(df[column])

                x = model.predict(df)
                if x:  # transaction is fraud
                    status = "Potential Fraud"
                    amount = "$" + str(nlp.extract_amount(text))
                    for item in json_data:
                        complianceRule = item.get('compliance_rule')
                        id = item.get('id')
                        if int(id) == int(target_id):
                            continue
                        newRules = nlp.extract_rules(complianceRule)[1]
                        if newRules == rules:
                            output = "This transaction will not be processed until the client produces all the documents specified in rule ID-" + str(id)
                            cursor.execute("INSERT INTO alert_management (customer_name, customer_type, flag, transaction_amount, alert_type) VALUES (%s, %s, %s, %s, %s)",(name, cust_type, output, amount, status))
                            conn.commit()
                            break

# Predict a transaction if it's a Fraud or Authentic
def fraudAlert():
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="complimatrix",
        user="nocobase",
        password="nocobase"
    )

    cursor = conn.cursor()

    cursor.execute("SELECT * FROM transaction_monitoring WHERE is_compliance_checked=false OR is_compliance_checked IS NULL;")
    columns = [column[0] for column in cursor.description]
    data = cursor.fetchall()

    customer_data = []
    for row in data:
        converted_row = []
        for item in row:
            if isinstance(item, datetime):
                converted_row.append(item.isoformat())
            else:
                converted_row.append(item)
        json_row = dict(zip(columns, converted_row))
        customer_data.append(json_row)

    cursor.execute("SELECT * FROM compliance;")
    columns = [column[0] for column in cursor.description]
    data = cursor.fetchall()

    json_data = []
    for row in data:
        converted_row = []
        for item in row:
            if isinstance(item, datetime):
                converted_row.append(item.isoformat())
            else:
                converted_row.append(item)
        json_row = dict(zip(columns, converted_row))
        json_data.append(json_row)

    for item in customer_data:
        df = pd.DataFrame.from_dict([item])
        item_id = df['id'].iloc[0]
        kyc_id = df['f_transactions_kyc'].iloc[0]
        kyb_id = df['f_kyb_transactiondetails'].iloc[0]
        df.rename(columns={'customer_name': 'full_cust_names',
                           'trustee_is_individual_or_company': 'any_trustee_is_individual_or_company',
                           'info_in_oficial_exchange': 'info_in_official_exchange',
                           'company_or_business_address': 'address_of_company',
                           'state_country_territory_of_incorporation': 'State_Country_Territory_of_incorporation'},
                  inplace=True)
        name = df['full_cust_names'].iloc[0]
        cust_type = df['customer_type'].iloc[0]
        cust_activity = df['customer_activity'].iloc[0]
        amount = df['value_of_transaction'].iloc[0]
        if name==None or cust_type==None or cust_activity==None or amount==None:
            continue
        nlpInput = name + " " + cust_type + " " + cust_activity + " " + amount
        nlpOutput = nlp.extract_rules(nlpInput)[1]
        for col in df.columns:
            if col not in nlpOutput or df[col].iloc[0] == "NA":
                df[col] = nan
        # df.drop(columns=['createdAt','updatedAt','sort','createdById','updatedById','id', 'unique_identification_number', 'value_of_transaction', 'measures_required', 'customer_type', 'customer_activity'], axis=1, inplace=True)
        df = df.fillna('Not Available')
        df = df.reindex(columns=feature_names)
        for column in df.columns:
            try:
                loaded_label_encoder = loaded_label_encoders[column]
                df[column] = loaded_label_encoder.transform(df[column])
            except ValueError:
                df[column]=0

        x = model.predict(df)
        if x:  # transaction is fraud
            for rule in json_data:
                complianceRule = rule.get('compliance_rule')
                id = rule.get('id')
                rules = nlp.extract_rules(complianceRule)[1]
                if nlpOutput == rules:
                    status = "Potential Fraud"
                    output = "This transaction will not be processed until the client produces all the documents specified in rule ID-" + str(id)
                    if kyc_id is not None:
                        cursor.execute("INSERT INTO alert_management (f_transaction_alert, alert_kyc, flag, alert_type) VALUES (%s, %s, %s, %s)",(int(item_id),int(kyc_id), output, status))
                    else:
                        cursor.execute("INSERT INTO alert_management (f_transaction_alert, alert_kyb, flag, alert_type) VALUES (%s, %s, %s, %s)",(int(item_id), int(kyb_id), output, status))
        cursor.execute("UPDATE transaction_monitoring SET is_compliance_checked=true WHERE id=%s", (int(item_id),))
        conn.commit()

# Training the model with datsets directly from the Complimatrix application
def trainModel():
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="complimatrix",
        user="nocobase",
        password="nocobase"
    )

    cursor = conn.cursor()

    cursor.execute("SELECT * FROM train_model;")
    columns = [column[0] for column in cursor.description]
    data = cursor.fetchall()

    training_data = []
    for row in data:
        converted_row = []
        for item in row:
            if isinstance(item, datetime):
                converted_row.append(item.isoformat())
            else:
                converted_row.append(item)
        json_row = dict(zip(columns, converted_row))
        training_data.append(json_row)

    df = pd.DataFrame.from_dict(training_data)
    df.drop(columns=['createdAt', 'updatedAt', 'sort', 'createdById', 'updatedById', 'id', 'unique_id_number',
                     'value_of_transaction', 'measures_required', 'customer_type', 'customer_activity'], axis=1,
            inplace=True)
    df.columns = df.columns.str.strip()
    df['is_fraud'] = df['is_fraud'].map({'YES': 1, 'NO': 0})
    df.replace(to_replace=['NA', None], value='Not Available', inplace=True)

    le = LabelEncoder()
    for col in df.columns:
        df[col] = le.fit_transform(df[col])

    y = df['is_fraud']
    df.drop(columns=['is_fraud'], axis=1, inplace=True)
    X = df
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=101)

    model = DecisionTreeClassifier(max_depth=4)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    metrics = precision_recall_fscore_support(y_test, y_pred, average='macro')
    mcc = matthews_corrcoef(y_test, y_pred)
    cursor.execute("INSERT INTO model_metrix (precision, recall, fscore, mcc) VALUES (%s, %s, %s, %s)",
                   (metrics[0], metrics[1], metrics[2], mcc))
    conn.commit()
