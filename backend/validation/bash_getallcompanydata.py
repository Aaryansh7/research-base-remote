import requests
import json
import pandas as pd
from datetime import datetime
import os
import sys
import time
import argparse # Import argparse for command-line arguments

# Add the parent directory of 'validation' (RESEARCH-BASE) to the Python path
# This allows for relative imports from 'backend.headers'
current_dir = os.path.dirname(os.path.abspath(__file__))
research_base_dir = os.path.join(current_dir, '..', '..')
print(f"Adding {research_base_dir} to sys.path")
sys.path.append(research_base_dir)

try:
    from backend.headers.xbrlprocesscheck import xbrl_data_processor, get_company_cik, fetch_historical_10k_filings_api_get
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
S3_COMPANY_CSV_PREFIX = 'company-csv-data/' 

def get_sec_tickers():
    """
    Fetches US company tickers from the SEC EDGAR company_tickers_exchange.json file
    and returns them in a list of dictionaries with 'name' and 'ticker'.
    """
    url = "https://www.sec.gov/files/company_tickers_exchange.json"
    
    headers = {
        "User-Agent": "FinancialDataValidator/1.0 (contact@example.com)" 
    }

    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        
        data = response.json()
        
        all_companies_list = []
        for company_info_list in data['data']:
            if len(company_info_list) >= 3:
                name = company_info_list[1]
                ticker = company_info_list[2]
                
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
    
def get_company_info(ticker, update_all):
    """
    Receives a company ticker, fetches EDGAR data, processes XBRL,
    saves it to S3, and returns a success message.
    Only processes data if new data is more recent than existing data or if no existing data.
    """
    print(f"Get company info received request for ticker: {ticker}")

    s3_file_key = f"{S3_COMPANY_CSV_PREFIX}{ticker.lower()}.csv"
    print(f"Using S3 file key: s3://{S3_BUCKET_NAME}/{s3_file_key}")

    try:
        cik = get_company_cik(ticker)
        if not cik:
            print(f"Could not retrieve CIK for ticker {ticker}. Skipping.")
            return

        reportings_data = fetch_historical_10k_filings_api_get(cik, ticker)
        if reportings_data.empty:
            print(f"No reporting data found for {ticker}. Skipping.")
            return

        print(f"Reportings data obtained for {ticker}:\n{reportings_data.head()}")

        reportings_data['reporting_date'] = pd.to_datetime(reportings_data['reporting_date'])
        latest_fetched_date = reportings_data['reporting_date'].max()
        print(f"Latest fetched report date from accessionNumber datatable: {latest_fetched_date}")

        latest_stored_date = None
        
        try:
            existing_df = read_csv_from_s3(file_key=s3_file_key, bucket_name=S3_BUCKET_NAME)
            
            date_columns = [col for col in existing_df.columns if col != 'Accounting Variable']
            
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

        if latest_stored_date is None or latest_fetched_date > latest_stored_date or update_all == True:
            print("Newer data available or no existing data. Processing financial data...")
            processed_financial_data = xbrl_data_processor(reportings_data, ticker, cik)
            
            if processed_financial_data.empty:
                print(f"No financial data processed for {ticker}. Skipping S3 write.")
                return

            print(f"Processed financial data for {ticker}:\n{processed_financial_data.head()}")

            write_df_to_csv_s3(processed_financial_data, file_key=s3_file_key, bucket_name=S3_BUCKET_NAME)
            print(f"Data for {ticker} saved to s3://{S3_BUCKET_NAME}/{s3_file_key}")

            message = "Company's Latest Financial data obtained and saved to S3!"
            if latest_stored_date is None:
                message = "Company's financial data obtained and saved to S3 for the first time."
            elif latest_fetched_date > latest_stored_date:
                 message = "Company's financial data updated with more recent information in S3."

            print(f"New Data successfully added - {message}")
            
        else:
            print("Existing data in S3 is already up to date. No new processing needed.")

    except ValueError as e:
        print(f"ValueError in get_company_info: {e}")
        # When a ValueError occurs, we want the script to exit with an error code
        # so the bash script can retry the same batch.
        sys.exit(1) 
    except Exception as e:
        print(f"Unexpected error in get_company_info: {e}")
        # For any other unexpected error, also exit with an error code.
        sys.exit(1)
    
def run_dataloader(start_index, end_index):
    """
    Fetches all US company tickers and processes them in batches.
    """
    print("Fetching all US company tickers...")

    all_companies = get_sec_tickers()
    
    if not all_companies:
        print("No companies found or an error occurred while fetching tickers. Exiting.")
        sys.exit(1) # Exit with error if no companies are found

    # Apply the batching based on start_index and end_index
    # Ensure indices are within bounds
    start = max(0, start_index)
    end = min(len(all_companies), end_index)
    
    companies_to_process = all_companies[start:end]

    if not companies_to_process:
        print(f"No companies to process in batch [{start_index}:{end_index}]. Exiting successfully.")
        sys.exit(0) # Exit successfully if no companies in this batch

    print(f"Successfully fetched {len(all_companies)} companies. Processing batch from index {start} to {end-1}.")
    print("--------------------------------------------------")

    for iter_company in companies_to_process:
        time.sleep(1)
        ticker = iter_company['ticker']
        print(f" --------- Starting loading for {ticker} ---------------")
        get_company_info(ticker, update_all=True)

    print(f"Successfully completed processing batch [{start_index}:{end_index}]!")
       

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the company data loader in batches.")
    parser.add_argument('--start_index', type=int, default=0,
                        help='Starting index for the company list batch.')
    parser.add_argument('--end_index', type=int, default=100,
                        help='Ending index (exclusive) for the company list batch.')
    
    args = parser.parse_args()
    
    run_dataloader(args.start_index, args.end_index)
