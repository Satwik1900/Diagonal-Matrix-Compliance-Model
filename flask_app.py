from flask import Flask, request
from complianceRuleBook.models import model_predict
from riskAssesment.models import assessRisk
from transactionMonitoring import transaction_monitoring

app = Flask(__name__)

# Calling Compliance RuleBook
def ruleBook():

    @app.route('/web', methods=['POST'])
    def web():
        model_predict.modifyRuleBook()
        return 'successful'

    # Predict a transaction if it's a Fraud or Authentic
    @app.route('/root', methods=['POST'])
    def root():
        model_predict.fraudAlert()
        return 'successful'
    
    # Training the model with datsets directly from the Complimatrix application
    @app.route('/train', methods=['POST'])
    def train():
        model_predict.trainModel()
        return 'successful' 

# Calling Risk Assesment
def riskAssess():
    @app.route('/kycrisk', methods=['POST'])
    def kycrisk():
        assessRisk.assessKycRisk()
        return 'successful'
    
    @app.route('/kybrisk', methods=['POST'])
    def kybrisk():
        assessRisk.assessKybRisk()
        return 'successful'

# Calling Transaction Monitoring Rules.
def transMonitoring():
    @app.route('/func', methods=['POST'])
    def func():
        transaction_monitoring.Transaction()
        return 'Successful'

if __name__ == '__main__':
    ruleBook()
    riskAssess()  
    transMonitoring()
    app.run(debug=True, host='0.0.0.0')       


