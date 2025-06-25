import sys
import os
import json
from tqdm import tqdm
import warnings



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
    # Use standard print for errors during import as tqdm might not be fully set up
    print(f"Error importing modules: {e}")
    print("Please ensure your PYTHONPATH is configured correctly or that files are in expected locations.")
    print(f"Current sys.path: {sys.path}")
    sys.exit(1)


def run_xbrl_link_validation():
    warnings.filterwarnings("ignore")

    # Initial messages before the main tqdm loop starts
    print("Starting XBRL link validation process...")
    print("Fetching all US company tickers...")

    all_companies = get_sec_tickers()

    if not all_companies:
        print("No companies found or an error occurred while fetching tickers. Exiting.")
        return

    print(f"Successfully fetched {len(all_companies)} companies.")
    print("--------------------------------------------------") # Separator for clarity

    validation_results = []

    # Use tqdm for the main loop over companies
    with tqdm(all_companies, desc="Processed companies", unit="company") as pbar:
        for company in pbar:
            ticker = company.get("ticker")
            company_name = company.get("name")

            if not ticker:
                # Use pbar.write for messages that should not interfere with the bar
                pbar.write(f"Skipping company with no ticker: {company_name}")
                continue

            # Print detailed messages for each company below the main progress bar
            # These messages will appear on new lines below the progress bar
            pbar.write(f"\n--- Processing {company_name} ({ticker}) ---")
            #pbar.write("Generating EDGAR link candidates...")

            num_working_links = 0
            try:
                edgar_api = sec_edgar_endpoint()
                trailing_data, cik = edgar_api.main_execution(ticker)

                if trailing_data is None or trailing_data.empty:
                    #pbar.write(f"No trailing data found for {ticker}. Skipping XBRL processing.")
                    validation_results.append({
                        "ticker": ticker,
                        "name": company_name,
                        "working_links": 0,
                        "status": "No trailing data"
                    })
                    continue

                num_working_links = xbrl_data_processor(trailing_data, ticker, cik)
                #pbar.write(f"Total working links found for {ticker}: {num_working_links}")


                validation_results.append({
                    "ticker": ticker,
                    "name": company_name,
                    "working_links": num_working_links,
                    "status": "Success"
                })

            except ValueError as ve:
                pbar.write(f"Error for {ticker}: {ve}")
                validation_results.append({
                    "ticker": ticker,
                    "name": company_name,
                    "working_links": 0,
                    "status": f"Error: {str(ve)}"
                })
            except Exception as e:
                pbar.write(f"An unexpected error occurred for {ticker}: {e}")
                validation_results.append({
                    "ticker": ticker,
                    "name": company_name,
                    "working_links": 0,
                    "status": f"Unexpected Error: {str(e)}"
                })

    output_file_path = os.path.join(current_dir, "xbrl_link_validation_results.json")
    with open(output_file_path, 'w') as f:
        json.dump(validation_results, f, indent=4)

    print(f"\nValidation complete. Results saved to: {output_file_path}")


if __name__ == "__main__":
    run_xbrl_link_validation()