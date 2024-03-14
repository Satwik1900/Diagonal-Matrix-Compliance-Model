import spacy
import re

nlp = spacy.load('en_core_web_sm')

def extract_amount(text):
    doc = nlp(text)
    for token in doc.ents:
        if token.label_ == "MONEY":
            amount_text = re.findall(r'\d+(?:,\d+)?', token.text)
            if amount_text:
                amount = float(amount_text[0].replace(",", ""))
            return amount

def extract_rules(text):
    doc = nlp(text)

    str = ""
    name = ""
    is_person = False
    amount = None
    
    for token in doc.ents:
        
        if token.label_ == "PERSON":
            name = token.text
            is_person = True
            
        if token.label_ == "ORG":
            name = token.text

        if token.label_ == "GPE":
            country = token.text
            # print(country)
        
        if token.label_ == "MONEY":
            amount_text = re.findall(r'\d+(?:,\d+)?', token.text)
            if amount_text:
                amount = float(amount_text[0].replace(",", ""))
            # print(amount)
            break
        
    comparision = "" 
    for token in doc:
        if token.text.upper() == "ABOVE" or token.text.upper() == "BELOW":
            comparision = token.text.upper()

    for token in doc:

        if token.text.upper() == "INDIVIDUAL":
            str = "INDIVIDUAL"
            break
        
        if token.text.upper() == "RECURRING":
            str = "RECURRING"
            break
        
        if token.text.upper() == "TRADER":
            str = "TRADER"
            break
        
        if token.text.upper() == "TRUST":
            str = "TRUST"
            break
        
        if token.text.upper() == "INCORPORATED":
            str = "INCORPORATED"
            break
        
        if token.text.upper() == "COOPERATIVE":
            str = "COOPERATIVE"
            break
        
        if token.text.upper() == "DOMESTIC":
            str = "DOMESTIC"
            break
        
        if token.text.upper() == "REGISTERED":
            str = "REGISTERED"
            break
        
        if token.text.upper() == "UNREGISTERED":
            str = "UNREGISTERED"
            break

    is_rule = 0
    
    for token in doc:

        if token.text.upper() == "ADD":
            is_rule=1
            rule_info = doc.text.lower().replace("add new rule", "").strip()
            return (is_rule, rule_info, token.text.upper())
        
        if token.text.upper() == "DELETE":
            is_rule=1
            rule_info = doc.text.lower().replace("delete rule", "").strip()
            return (is_rule, rule_info, token.text.upper())
        
        if token.text.upper() == "UPDATE":
            is_rule=1
            rule_info = doc.text.lower().replace("update rule", "").strip()
            return (is_rule, rule_info, token.text.upper(), str, amount, comparision)
    
    if str == "RECURRING":
        return (is_rule, ["occ_business_act", "purpose_of_transaction", "source_of_funds"], name)
    
    elif str == "TRADER":
         return (is_rule, ["full_cust_names",
                 "address_of_company", "principal_place_of_operation", 
                 "nature_of_business_by_the_company", "type_of_company"], name)
                 
    elif str == "TRUST":
        return (is_rule,["full_cust_names", "type_of_trust", "country_of_establishment",
                "any_trustee_is_individual_or_company"], name)
                 
    elif str == "INCORPORATED":
        return (is_rule, ["full_cust_names", "full_address_of_the_head_office",
                "unique_identification_number", "State_Country_Territory_of_incorporation", 
                "date_of_incorporation", "name_of_chairman", "objects_of_entity"], name)
    
    elif str == "COOPERATIVE":
        return (is_rule, ["full_cust_names", "full_address_of_the_head_office", 
                "unique identification number", "State_Country_Territory_of_incorporation",
                "date_of_incorporation", "name_of_chairman", "objects_of_entity"], name)
    
    elif str == "DOMESTIC":
        return (is_rule, ["full_cust_names", "info_in_domestic_exchange"], name)
    
    elif str == "REGISTERED":
        return (is_rule, ["full_cust_names", "info_in_domestic_exchange", "info_in_official_exchange"], name)
    
    elif str == "UNREGISTERED":
        return (is_rule, ["info_in_official_exchange"], name)
    
    elif (amount and amount < 10000) or comparision=="BELOW":
        return (is_rule, ["full_cust_names", "residential_address", "country_of_citizenship",
                "customer_of_residence","occ_business_act", "purpose_of_transaction", 
                "source_of_funds", "nature of the customer's business with the reporting entity", "income or assets available to the customer",
                "customer's financial position"], name)
        
    elif amount and amount >= 10000 or comparision=="ABOVE":
        return (is_rule,["full_cust_names", "residential_address", "source_of_funds",
                "occ_business_act", "purpose_of_transaction", "country_of_citizenship",
                "country_of_residence"], name)
    
    # else :
    #     return(is_rule, ["full_cust_names", "country_of_establishment", "occ_business_act", "purpose_of_transaction", "source_of_funds"], name)
    
# text = "Verma Industries transferred $2,000 trader"
# print(extract_rules(text.lower()))
    