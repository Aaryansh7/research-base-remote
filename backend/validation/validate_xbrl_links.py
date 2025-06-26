import sys
import os
import json
from tqdm import tqdm
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed # Import for concurrency

# Add the parent directory of 'validation' (RESEARCH-BASE) to the Python path
# This allows for relative imports from 'backend.headers'
current_dir = os.path.dirname(os.path.abspath(__file__))
research_base_dir = os.path.join(current_dir, '..', '..')
sys.path.append(research_base_dir)

try:
    from get_ticker_list import get_sec_tickers
    from edgarAPI import sec_edgar_endpoint
    from xbrlprocessing import xbrl_data_processor
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Please ensure your PYTHONPATH is configured correctly or that files are in expected locations.")
    print(f"Current sys.path: {sys.path}")
    sys.exit(1)

def process_company(company):
    """
    Function to encapsulate the logic for processing a single company.
    This will be executed in a separate thread.
    """
    ticker = company.get("ticker")
    company_name = company.get("name")
    result = {
        "ticker": ticker,
        "name": company_name,
        "working_links": 0,
        "status": ""
    }

    if not ticker:
        result["status"] = "Skipped: No ticker"
        return result

    try:
        edgar_api = sec_edgar_endpoint()
        trailing_data, cik = edgar_api.main_execution(ticker)

        if trailing_data is None or trailing_data.empty:
            result["status"] = "No trailing data"
            return result

        num_working_links = xbrl_data_processor(trailing_data, ticker, cik)
        result["working_links"] = num_working_links
        result["status"] = "Success"

    except ValueError as ve:
        result["status"] = f"Error: {str(ve)}"
    except Exception as e:
        result["status"] = f"Unexpected Error: {str(e)}"
    return result


def run_xbrl_link_validation():
    warnings.filterwarnings("ignore")

    print("Starting XBRL link validation process...")
    print("Fetching all US company tickers...")

    all_companies = get_sec_tickers()
    all_companies = all_companies[:1000]

    if not all_companies:
        print("No companies found or an error occurred while fetching tickers. Exiting.")
        return

    print(f"Successfully fetched {len(all_companies)} companies.")
    print("--------------------------------------------------")

    validation_results = []
    # Determine the number of workers (threads). A good starting point is the number of CPU cores,
    # or higher for I/O-bound tasks. Adjust as needed.
    # Be mindful of the API rate limits of EDGAR.
    MAX_WORKERS = 5 # You can adjust this number based on your system's capabilities and API limits

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit tasks to the executor
        future_to_company = {executor.submit(process_company, company): company for company in all_companies}

        # Use tqdm to show progress for completed futures
        for future in tqdm(as_completed(future_to_company), total=len(all_companies), desc="Processed companies", unit="company"):
            company = future_to_company[future]
            try:
                data = future.result()
                validation_results.append(data)
            except Exception as exc:
                # Handle exceptions that occurred in the worker thread
                ticker = company.get("ticker", "N/A")
                company_name = company.get("name", "N/A")
                print(f"--- Error processing {company_name} ({ticker}): {exc}")
                validation_results.append({
                    "ticker": ticker,
                    "name": company_name,
                    "working_links": 0,
                    "status": f"Critical Error: {str(exc)}"
                })

    output_file_path = os.path.join(current_dir, "xbrl_link_validation_results.json")
    with open(output_file_path, 'w') as f:
        json.dump(validation_results, f, indent=4)

    print(f"\nValidation complete. Results saved to: {output_file_path}")

if __name__ == "__main__":
    run_xbrl_link_validation()