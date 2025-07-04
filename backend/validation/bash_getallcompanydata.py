import requests
import json
import pandas as pd
from datetime import datetime
import os
import sys
import time
import argparse
import logging

# Add the parent directory of 'validation' (RESEARCH-BASE) to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
research_base_dir = os.path.join(current_dir, '..', '..')
sys.path.append(research_base_dir)

# --- Configure Logging ---
LOG_FILE_PATH = os.path.join(current_dir, 'dataloader_run.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(LOG_FILE_PATH)
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)
# --- End Configure Logging ---


try:
    from backend.headers.xbrlprocesscheck import xbrl_data_processor, get_company_cik, fetch_historical_10k_filings_api_get
    from backend.headers.s3_utils import read_csv_from_s3, write_df_to_csv_s3
except ImportError as e:
    logger.error(f"Error importing modules: {e}")
    logger.error("Please ensure your PYTHONPATH is configured correctly or that files are in expected locations.")
    logger.error(f"Current sys.path: {sys.path}")
    sys.exit(1)

S3_BUCKET_NAME = "backend-datarepo"
if not S3_BUCKET_NAME:
    raise ValueError("S3_BUCKET_NAME environment variable is not set.")

S3_COMPANY_CSV_PREFIX = 'company-csv-data/'

# REMOVE or comment out the original get_sec_tickers function as it will no longer be used.
# def get_sec_tickers():
#     """
#     Fetches US company tickers from the SEC EDGAR company_tickers_exchange.json file
#     and returns them in a list of dictionaries with 'name' and 'ticker'.
#     """
#     url = "https://www.sec.gov/files/company_tickers_exchange.json"
#     headers = {
#         "User-Agent": "FinancialDataValidator/1.0 (contact@example.com)"
#     }
#     try:
#         response = requests.get(url, headers=headers, verify=False)
#         response.raise_for_status()
#         data = response.json()
#         all_companies_list = []
#         for company_info_list in data['data']:
#             if len(company_info_list) >= 3:
#                 name = company_info_list[1]
#                 ticker = company_info_list[2]
#                 if name and ticker:
#                     all_companies_list.append({"name": name, "ticker": ticker})
#         return all_companies_list
#     except requests.exceptions.RequestException as e:
#         logger.error(f"Error fetching data from SEC EDGAR: {e}")
#         return []
#     except json.JSONDecodeError as e:
#         logger.error(f"Error decoding JSON response: {e}")
#         return []
#     except Exception as e:
#         logger.error(f"An unexpected error occurred: {e}")
#         return []

def get_sp500_tickers_from_json(json_file_path):
    """
    Reads company tickers from a local JSON file.
    The JSON file is expected to be a list of dictionaries, each with 'name' and 'ticker' keys.
    """
    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        logger.info(f"Successfully loaded tickers from {json_file_path}")
        return data
    except FileNotFoundError:
        logger.error(f"Error: JSON file not found at {json_file_path}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {json_file_path}: {e}")
        return []
    except Exception as e:
        logger.error(f"An unexpected error occurred while reading {json_file_path}: {e}")
        return []

def get_company_info(ticker, update_all):
    """
    Receives a company ticker, fetches EDGAR data, processes XBRL,
    saves it to S3, and returns a success message.
    Only processes data if new data is more recent than existing data or if no existing data.
    """
    logger.info(f"Get company info received request for ticker: {ticker}")

    s3_file_key = f"{S3_COMPANY_CSV_PREFIX}{ticker.lower()}.csv"
    logger.info(f"Using S3 file key: s3://{S3_BUCKET_NAME}/{s3_file_key}")

    try:
        cik = get_company_cik(ticker)
        if not cik:
            logger.warning(f"Could not retrieve CIK for ticker {ticker}. Skipping.")
            return

        reportings_data = fetch_historical_10k_filings_api_get(cik, ticker)
        if reportings_data.empty:
            logger.info(f"No reporting data found for {ticker}. Skipping.")
            return

        logger.info(f"Reportings data obtained for {ticker}:\n{reportings_data.head()}")

        reportings_data['reporting_date'] = pd.to_datetime(reportings_data['reporting_date'])
        latest_fetched_date = reportings_data['reporting_date'].max()
        logger.info(f"Latest fetched report date from accessionNumber datatable: {latest_fetched_date}")

        latest_stored_date = None

        try:
            existing_df = read_csv_from_s3(file_key=s3_file_key, bucket_name=S3_BUCKET_NAME)

            date_columns = [col for col in existing_df.columns if col != 'Accounting Variable']

            if date_columns:
                latest_stored_date = pd.to_datetime(date_columns).max()
                logger.info(f"Latest stored report date for {ticker} in S3: {latest_stored_date}")
            else:
                logger.info(f"No date columns found in existing S3 file {s3_file_key}. Will process new data.")

        except FileNotFoundError:
            logger.info(f"No existing data file found for {ticker} in S3: {s3_file_key}")
        except pd.errors.EmptyDataError:
            logger.info(f"Existing S3 file {s3_file_key} is empty. Will process new data.")
        except Exception as e:
            logger.error(f"Error reading existing CSV from S3 {s3_file_key}: {e}. Will process new data.")

        if latest_stored_date is None or latest_fetched_date > latest_stored_date or update_all == True:
            logger.info("Newer data available or no existing data. Processing financial data...")
            processed_financial_data = xbrl_data_processor(reportings_data, ticker, cik)

            if processed_financial_data.empty:
                logger.info(f"No financial data processed for {ticker}. Skipping S3 write.")
                return

            logger.info(f"Processed financial data for {ticker}:\n{processed_financial_data.head()}")

            write_df_to_csv_s3(processed_financial_data, file_key=s3_file_key, bucket_name=S3_BUCKET_NAME)
            logger.info(f"Data for {ticker} saved to s3://{S3_BUCKET_NAME}/{s3_file_key}")

            message = "Company's Latest Financial data obtained and saved to S3!"
            if latest_stored_date is None:
                message = "Company's financial data obtained and saved to S3 for the first time."
            elif latest_fetched_date > latest_stored_date:
                 message = "Company's financial data updated with more recent information in S3."

            logger.info(f"New Data successfully added - {message}")

        else:
            logger.info("Existing data in S3 is already up to date. No new processing needed.")

    except ValueError as e:
        logger.error(f"ValueError in get_company_info: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error in get_company_info: {e}")
        sys.exit(1)

def run_dataloader(start_index, end_index): # Removed json_file argument
    """
    Fetches all US company tickers and processes them in batches.
    """
    logger.info("Fetching all US company tickers...")

    # Hardcoded JSON file path
    # Assuming sp500_company_tickers.json is in the same directory as bash_getallcompanydata.py
    json_file_path = os.path.join(current_dir, 'sp500_company_tickers.json')
    all_companies = get_sp500_tickers_from_json(json_file_path) # Directly use the hardcoded path
    all_companies = all_companies[:100]

    if not all_companies:
        logger.error("No companies found or an error occurred while fetching tickers. Exiting.")
        sys.exit(1)

    start = max(0, start_index)
    end = min(len(all_companies), end_index)

    companies_to_process = all_companies[start:end]

    if not companies_to_process:
        logger.info(f"No companies to process in batch [{start_index}:{end_index}]. Exiting successfully.")
        sys.exit(0)

    logger.info(f"Successfully loaded {len(all_companies)} companies. Processing batch from index {start} to {end-1}.")
    logger.info("--------------------------------------------------")

    for iter_company in companies_to_process:
        time.sleep(1)
        ticker = iter_company['ticker']
        logger.info(f" --------- Starting loading for {ticker} ---------------")
        get_company_info(ticker, update_all=False)

    logger.info(f"Successfully completed processing batch [{start_index}:{end_index}]!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the company data loader in batches.")
    parser.add_argument('--start_index', type=int, default=0,
                        help='Starting index for the company list batch.')
    parser.add_argument('--end_index', type=int, default=100,
                        help='Ending index (exclusive) for the company list batch.')

    args = parser.parse_args()

    # Call run_dataloader without the json_file argument
    run_dataloader(args.start_index, args.end_index)