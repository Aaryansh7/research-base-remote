from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import os
import sys # Import sys for path manipulation
from datetime import datetime

# Import the sec_edgar_endpoint class from headers.edgarAPI
from headers.edgarAPI import sec_edgar_endpoint
# Import the xbrl_data_processor function
from headers.xbrlprocessing import xbrl_data_processor

# Import the new function from src/profitabilityratios.py
from src.profitabilityratio import get_netmargin, get_operatingmargin
# Import the new function from src/liquidityratio.py
from src.liquidityratio import get_currentratio, get_cashratio
# Import the new function from src/solvencyratio.py
from src.solvencyratio import get_debtequityratio, get_debtassetratio

app = Flask(__name__)
CORS(app)

# Define the path to company_data.csv relative to app.py
COMPANY_DATA_FILEPATH = 'company_data.csv'

# API ENDPOINT: Receive Ticker and Compute Company Data
@app.route('/api/company-info/<ticker>', methods=['GET'])
def get_company_info(ticker):
    """
    Receives a company ticker, fetches EDGAR data, processes XBRL,
    saves it to company_data.csv, and returns a success message.
    """
    print(f"Backend received request for ticker: {ticker}")

    try:
        # Step 1: Create an instance of sec_edgar_endpoint
        edgar_api = sec_edgar_endpoint()
        # Step 2: Call the main_execution function to get reporting data
        reportings_data = edgar_api.main_execution(ticker)
        print(f"Reportings data obtained for {ticker}:\n{reportings_data.head()}")

        # Step 3: Process the financial data using xbrl_data_processor
        # This function is expected to return a processed pandas DataFrame
        processed_financial_data = xbrl_data_processor(reportings_data, ticker)
        print(f"Processed financial data for {ticker}:\n{processed_financial_data.head()}")

        # Step 4: Save the processed DataFrame to 'company_data.csv'
        # This CSV will be used by other endpoints to retrieve data
        processed_financial_data.to_csv('company_data.csv', index=False)
        print(f"Data for {ticker} saved to company_data.csv")

        # Return a success message. The actual data processing is done
        # and stored for subsequent calls.
        return jsonify({"status": "success", "message": "Company's Latest Financial data obtained and saved!", "ticker": ticker})

    except ValueError as e:
        print(f"ValueError in get_company_info: {e}")
        return jsonify({"status": "error", "message": str(e)}), 404
    except Exception as e:
        print(f"Unexpected error in get_company_info: {e}")
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {str(e)}"}), 500

# API ENDPOINT: Compute Profitability Ratio: NetMargin 
@app.route('/api/profitability/net-margin', methods=['GET'])
def get_profitability_netmargin():
    """
    Calls the get_netmargin function from src/profitabilityratios.py
    and returns its result as a JSON response.
    """
    print(f"Backend received request for Net Margin and Revenue data.")
    # Call the outsourced function
    response_data = get_netmargin(COMPANY_DATA_FILEPATH)
    return response_data

# API ENDPOINT: Compute Profitability Ratio: Operating Margin
@app.route('/api/profitability/operating-margin', methods=['GET'])
def get_profitability_operatingmargin():
    """
    Calls the get_operatingmargin function from src/profitabilityratios.py
    and returns its result as a JSON response.
    """
    print(f"Backend received request for Operating Margin and Revenue data.")
    # Call the outsourced function
    response_data = get_operatingmargin(COMPANY_DATA_FILEPATH)
    return response_data

# API ENDPOINT: Compute Liquidity Ratio: Current Ratio
@app.route('/api/liquidity/current-ratio', methods=['GET'])
def get_liquidity_currentratio():
    """
    Calls the get_currentratio function from src/liquidityratios.py
    and returns its result as a JSON response.
    """
    print(f"Backend received request for CurrentRatio.")
    # Call the outsourced function
    response_data = get_currentratio(COMPANY_DATA_FILEPATH)
    return response_data

# API ENDPOINT: Compute Liquidity Ratio: Cash Ratio
@app.route('/api/liquidity/cash-ratio', methods=['GET'])
def get_liquidity_cashratio():
    """
    Calls the get_cashratio function from src/liquidityratios.py
    and returns its result as a JSON response.
    """
    print(f"Backend received request for CashRatio.")
    # Call the outsourced function
    response_data = get_cashratio(COMPANY_DATA_FILEPATH)
    return response_data

# API ENDPOINT: Compute Solvency Ratio: Debt to Equity Ratio
@app.route('/api/solvency/debtequity-ratio', methods=['GET'])
def get_solvency_debtequityratio():
    """
    Calls the get_debtequtiyratio function from src/solvencyratio.py
    and returns its result as a JSON response.
    """
    print(f"Backend received request for Debt to Equity.")
    # Call the outsourced function
    response_data = get_debtequityratio(COMPANY_DATA_FILEPATH)
    return response_data

# API ENDPOINT: Compute Solvency Ratio: Debt to Asset Ratio
@app.route('/api/solvency/debtasset-ratio', methods=['GET'])
def get_solvency_debtassetratio():
    """
    Calls the get_debtassetratio function from src/solvencyratio.py
    and returns its result as a JSON response.
    """
    print(f"Backend received request for Debt to Asset.")
    # Call the outsourced function
    response_data = get_debtassetratio(COMPANY_DATA_FILEPATH)
    return response_data




if __name__ == '__main__':
    # When running locally, ensure the correct path to headers is in sys.path
    script_dir = os.path.dirname(__file__)
    # Add the current script's directory to sys.path if it's not already there
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)
    # If 'headers' is a sibling directory to where app.py resides, you might need:
    # parent_dir = os.path.abspath(os.path.join(script_dir, os.pardir))
    # if parent_dir not in sys.path:
    #     sys.path.insert(0, parent_dir)


    app.run(debug=True, port=5000)