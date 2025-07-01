import requests
import json
import pandas as pd
from datetime import datetime
import os
import sys
from flask import Flask, jsonify, request
import time


# Add the parent directory of 'validation' (RESEARCH-BASE) to the Python path
# This allows for relative imports from 'backend.headers'
current_dir = os.path.dirname(os.path.abspath(__file__))
research_base_dir = os.path.join(current_dir, '..', '..')
print(research_base_dir)
sys.path.append(research_base_dir)

try:
    #from get_ticker_list import get_sec_tickers
    #from xbrlprocessing import xbrl_data_processor
    from backend.headers.xbrlprocessor_check import xbrl_data_processor, get_company_cik, fetch_historical_10k_filings_api_get
    from backend.headers.s3_utils import read_csv_from_s3, write_df_to_csv_s3

except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Please ensure your PYTHONPATH is configured correctly or that files are in expected locations.")
    print(f"Current sys.path: {sys.path}")
    sys.exit(1)

S3_BUCKET_NAME = "backend-datarepo"
if not S3_BUCKET_NAME:
    raise ValueError("S3_BUCKET_NAME environment variable is not set.")

# Define the base S3 path for company CSV data
# This acts like your 'data/' folder, but in S3
S3_COMPANY_CSV_PREFIX = 'company-csv-data/' 

def get_sec_tickers():
    """
    Fetches US company tickers from the SEC EDGAR company_tickers_exchange.json file
    and returns them in a list of dictionaries with 'name' and 'ticker'.
    """
    url = "https://www.sec.gov/files/company_tickers_exchange.json"
    
    # IMPORTANT: Replace with your actual identifying information.
    # The SEC requires a User-Agent header to identify your requests.
    # Failing to do so may result in your requests being blocked.
    # Example: YourAppName/1.0 (YourEmail@example.com)
    headers = {
        "User-Agent": "FinancialDataValidator/1.0 (contact@example.com)" 
    }

    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        
        data = response.json()

        # The JSON structure is:
        # {
        #   "fields": ["cik", "name", "ticker", "exchange"],
        #   "data": [
        #     [cik1, name1, ticker1, exchange1],
        #     [cik2, name2, ticker2, exchange2],
        #     ...
        #   ]
        # }
        
        # We need to extract 'name' (index 1) and 'ticker' (index 2) from each sublist in 'data'.
        
        all_companies_list = []
        for company_info_list in data['data']:
            # Ensure the list has enough elements to avoid IndexError
            if len(company_info_list) >= 3:
                name = company_info_list[1]
                ticker = company_info_list[2]
                
                # Only add if both name and ticker are present and not empty
                if name and ticker:
                    all_companies_list.append({"name": name, "ticker": ticker})
        
        return all_companies_list

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from SEC EDGAR: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []
    
def get_company_info(ticker):
    """
    Receives a company ticker, fetches EDGAR data, processes XBRL,
    saves it to S3, and returns a success message.
    Only processes data if new data is more recent than existing data or if no existing data.
    """
    print(f"Get company info received request for ticker: {ticker}")

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

            #return jsonify({"status": "success", "message": message, "ticker": ticker})
            print(f"New Data successfully added - {message}")
        else:
            print("Existing data in S3 is already up to date. No new processing needed.")
            #return jsonify({"status": "success", "message": "Company's financial data in S3 is already up to date.", "ticker": ticker})

    except ValueError as e:
        print(f"ValueError in get_company_info: {e}")
        #return jsonify({"status": "error", "message": str(e)}), 404
    except Exception as e:
        print(f"Unexpected error in get_company_info: {e}")
        #return jsonify({"status": "error", "message": f"An unexpected error occurred: {str(e)}"}), 500
    
def run_dataloader():
    print("Fetching all US company tickers...")

    all_companies = get_sec_tickers()
    all_companies = all_companies[99:300]

    if not all_companies:
        print("No companies found or an error occurred while fetching tickers. Exiting.")
        return

    print(f"Successfully fetched {len(all_companies)} companies.")
    print("--------------------------------------------------")

    '''
    for iter in all_companies:
        time.sleep(1)
        ticker = iter['ticker']
        print(f" --------- Starting loading for {ticker} ---------------")
        get_company_info(ticker)
    '''

    get_company_info("IBM")
    print("Successfully iterations completed !")
       

if __name__ == "__main__":
    run_dataloader()

    