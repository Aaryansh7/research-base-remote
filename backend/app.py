from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import os
import sys # Import sys for path manipulation
from datetime import datetime
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Import load_dotenv to load environment variables from .env file
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

# Import S3 utility functions
from headers.s3_utils import read_csv_from_s3, write_df_to_csv_s3
# Import the sec_edgar_endpoint class from headers.edgarAPI
from headers.edgarAPI import sec_edgar_endpoint
# Import the xbrl_data_processor function
# Make sure xbrl_data_processor also accepts s3_bucket_name
from headers.xbrlprocessor_check import get_company_cik, fetch_historical_10k_filings_api_get, xbrl_data_processor 

# Import the new function from src/profitabilityratios.py
from src.profitabilityratio import get_netmargin, get_operatingmargin
# Import the new function from src/liquidityratio.py
from src.liquidityratio import get_currentratio, get_cashratio
# Import the new function from src/solvencyratio.py
from src.solvencyratio import get_debtequityratio, get_debtassetratio
# Import the new function from src/efficiencyratio.py
from src.efficiencyratio import get_inventoryturnoverratio, get_assetturnoverratio

app = Flask(__name__)
CORS(app, origins=["https://effortless-kringle-511233.netlify.app", "http://localhost:3000"])

# S3 Bucket Name - retrieved from environment variable
# This will be used across all S3 operations
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
if not S3_BUCKET_NAME:
    raise ValueError("S3_BUCKET_NAME environment variable is not set.")

# Define the base S3 path for company CSV data
# This acts like your 'data/' folder, but in S3
S3_COMPANY_CSV_PREFIX = 'company-csv-data/' 


# API ENDPOINT: Receive Ticker and Compute Company Data
@app.route('/api/company-info/<ticker>', methods=['GET'])
def get_company_info(ticker):
    """
    Receives a company ticker, fetches EDGAR data, processes XBRL,
    saves it to S3, and returns a success message.
    Only processes data if new data is more recent than existing data or if no existing data.
    """
    print(f"Backend received request for ticker: {ticker}")

    # Dynamically set the S3 file key based on the ticker
    s3_file_key = f"{S3_COMPANY_CSV_PREFIX}{ticker.lower()}.csv"
    print(f"Using S3 file key: s3://{S3_BUCKET_NAME}/{s3_file_key}")

    try:
        # Step 1: Create an instance of sec_edgar_endpoint
        #edgar_api = sec_edgar_endpoint()
        # Step 2: Call the main_execution function to get reporting data
        #reportings_data, cik = edgar_api.main_execution(ticker)
        cik = get_company_cik(ticker)
        reportings_data = fetch_historical_10k_filings_api_get(cik, ticker)
        print(f"Reportings data obtained for {ticker}:\n{reportings_data.head()}")

        # Ensure 'reporting_date' is in datetime format for proper comparison
        reportings_data['reporting_date'] = pd.to_datetime(reportings_data['reporting_date'])

        # Get the latest report date from the newly fetched data
        latest_fetched_date = reportings_data['reporting_date'].max()
        print(f"Latest fetched report date from accessionNumber datatable: {latest_fetched_date}")

        latest_stored_date = None
        
        # Check if the ticker-specific CSV exists in S3 and load it
        try:
            existing_df = read_csv_from_s3(file_key=s3_file_key, bucket_name=S3_BUCKET_NAME)
            
            # Identify date columns by excluding 'Accounting Variable'
            date_columns = [col for col in existing_df.columns if col != 'Accounting Variable']
            
            # Convert date column names to datetime objects and find the maximum date
            if date_columns:
                latest_stored_date = pd.to_datetime(date_columns).max()
                print(f"Latest stored report date for {ticker} in S3: {latest_stored_date}")
            else:
                print(f"No date columns found in existing S3 file {s3_file_key}. Will process new data.")

        except FileNotFoundError:
            print(f"No existing data file found for {ticker} in S3: {s3_file_key}")
        except pd.errors.EmptyDataError:
            print(f"Existing S3 file {s3_file_key} is empty. Will process new data.")
        except Exception as e:
            print(f"Error reading existing CSV from S3 {s3_file_key}: {e}. Will process new data.")

        # Conditionally process and save data
        if latest_stored_date is None or latest_fetched_date > latest_stored_date:
            print("Newer data available or no existing data. Processing financial data...")
            # Step 3: Process the financial data using xbrl_data_processor
            # Ensure xbrl_data_processor handles S3 writes (as per previous modifications)
            processed_financial_data = xbrl_data_processor(reportings_data, ticker, cik)
            print(f"Processed financial data for {ticker}:\n{processed_financial_data.head()}")

            # Step 4: Save the processed DataFrame to S3
            write_df_to_csv_s3(processed_financial_data, file_key=s3_file_key, bucket_name=S3_BUCKET_NAME)
            print(f"Data for {ticker} saved to s3://{S3_BUCKET_NAME}/{s3_file_key}")

            message = "Company's Latest Financial data obtained and saved to S3!"
            if latest_stored_date is None:
                message = "Company's financial data obtained and saved to S3 for the first time."
            elif latest_fetched_date > latest_stored_date:
                 message = "Company's financial data updated with more recent information in S3."

            return jsonify({"status": "success", "message": message, "ticker": ticker})
        else:
            print("Existing data in S3 is already up to date. No new processing needed.")
            return jsonify({"status": "success", "message": "Company's financial data in S3 is already up to date.", "ticker": ticker})

    except ValueError as e:
        print(f"ValueError in get_company_info: {e}")
        return jsonify({"status": "error", "message": str(e)}), 404
    except Exception as e:
        print(f"Unexpected error in get_company_info: {e}")
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {str(e)}"}), 500
    
# API ENDPOINT: Compute Profitability Ratio: NetMargin 
@app.route('/api/profitability/net-margin/<ticker>', methods=['GET'])
def get_profitability_netmargin(ticker):
    """
    Calls the get_netmargin function from src/profitabilityratios.py
    and returns its result as a JSON response.
    """
    print(f"Backend received request for Net Margin and Revenue data.")
    # Call the outsourced function
    # Dynamically set the S3 file key based on the ticker
    s3_file_key = f"{S3_COMPANY_CSV_PREFIX}{ticker.lower()}.csv"
    df = read_csv_from_s3(file_key=s3_file_key, bucket_name=S3_BUCKET_NAME)
    response_data = get_netmargin(df)
    return response_data

# API ENDPOINT: Compute Profitability Ratio: Operating Margin
@app.route('/api/profitability/operating-margin/<ticker>', methods=['GET'])
def get_profitability_operatingmargin(ticker):
    """
    Calls the get_operatingmargin function from src/profitabilityratios.py
    and returns its result as a JSON response.
    """
    print(f"Backend received request for Operating Margin and Revenue data.")

    # Call the outsourced function
    # Dynamically set the S3 file key based on the ticker
    s3_file_key = f"{S3_COMPANY_CSV_PREFIX}{ticker.lower()}.csv"
    df = read_csv_from_s3(file_key=s3_file_key, bucket_name=S3_BUCKET_NAME)
    response_data = get_operatingmargin(df)
    return response_data

# API ENDPOINT: Compute Liquidity Ratio: Current Ratio
@app.route('/api/liquidity/current-ratio/<ticker>', methods=['GET'])
def get_liquidity_currentratio(ticker):
    """
    Calls the get_currentratio function from src/liquidityratios.py
    and returns its result as a JSON response.
    """
    print(f"Backend received request for CurrentRatio.")

    # Call the outsourced function
    # Dynamically set the S3 file key based on the ticker
    s3_file_key = f"{S3_COMPANY_CSV_PREFIX}{ticker.lower()}.csv"
    df = read_csv_from_s3(file_key=s3_file_key, bucket_name=S3_BUCKET_NAME)
    response_data = get_currentratio(df)
    return response_data

# API ENDPOINT: Compute Liquidity Ratio: Cash Ratio
@app.route('/api/liquidity/cash-ratio/<ticker>', methods=['GET'])
def get_liquidity_cashratio(ticker):
    """
    Calls the get_cashratio function from src/liquidityratios.py
    and returns its result as a JSON response.
    """
    print(f"Backend received request for CashRatio.")

    # Call the outsourced function
    # Dynamically set the S3 file key based on the ticker
    s3_file_key = f"{S3_COMPANY_CSV_PREFIX}{ticker.lower()}.csv"
    df = read_csv_from_s3(file_key=s3_file_key, bucket_name=S3_BUCKET_NAME)
    response_data = get_cashratio(df)
    return response_data

# API ENDPOINT: Compute Solvency Ratio: Debt to Equity Ratio
@app.route('/api/solvency/debtequity-ratio/<ticker>', methods=['GET'])
def get_solvency_debtequityratio(ticker):
    """
    Calls the get_debtequtiyratio function from src/solvencyratio.py
    and returns its result as a JSON response.
    """
    print(f"Backend received request for Debt to Equity.")
    # Call the outsourced function
    # Dynamically set the S3 file key based on the ticker
    s3_file_key = f"{S3_COMPANY_CSV_PREFIX}{ticker.lower()}.csv"
    df = read_csv_from_s3(file_key=s3_file_key, bucket_name=S3_BUCKET_NAME)
    response_data = get_debtequityratio(df)
    return response_data

# API ENDPOINT: Compute Solvency Ratio: Debt to Asset Ratio
@app.route('/api/solvency/debtasset-ratio/<ticker>', methods=['GET'])
def get_solvency_debtassetratio(ticker):
    """
    Calls the get_debtassetratio function from src/solvencyratio.py
    and returns its result as a JSON response.
    """
    print(f"Backend received request for Debt to Asset.")

    # Call the outsourced function
    # Dynamically set the S3 file key based on the ticker
    s3_file_key = f"{S3_COMPANY_CSV_PREFIX}{ticker.lower()}.csv"
    df = read_csv_from_s3(file_key=s3_file_key, bucket_name=S3_BUCKET_NAME)
    response_data = get_debtassetratio(df)
    return response_data

# API ENDPOINT: Compute Efficiency Ratio: Inventory Turnover
@app.route('/api/efficiency/inventoryturnover-ratio/<ticker>', methods=['GET'])
def get_efficiency_inventoryturnoverratio(ticker):
    """
    Calls the get_inventoryturnoverratio function from src/efficiencyratio.py
    and returns its result as a JSON response.
    """
    print(f"Backend received request for Inventory Tunrover.")

    # Call the outsourced function
    # Dynamically set the S3 file key based on the ticker
    s3_file_key = f"{S3_COMPANY_CSV_PREFIX}{ticker.lower()}.csv"
    df = read_csv_from_s3(file_key=s3_file_key, bucket_name=S3_BUCKET_NAME)
    response_data = get_inventoryturnoverratio(df)
    return response_data

# API ENDPOINT: Compute Efficiency Ratio: Asset Turnover
@app.route('/api/efficiency/assetturnover-ratio/<ticker>', methods=['GET'])
def get_efficiency_assetturnoverratio(ticker):
    """
    Calls the get_assetturnoverratio function from src/efficiencyratio.py
    and returns its result as a JSON response.
    """
    print(f"Backend received request for Asset Tunrover.")

    # Call the outsourced function
    # Dynamically set the S3 file key based on the ticker
    s3_file_key = f"{S3_COMPANY_CSV_PREFIX}{ticker.lower()}.csv"
    df = read_csv_from_s3(file_key=s3_file_key, bucket_name=S3_BUCKET_NAME)
    response_data = get_assetturnoverratio(df)
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


    app.run(debug = True, port=5000)