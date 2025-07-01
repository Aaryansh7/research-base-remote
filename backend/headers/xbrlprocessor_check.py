import requests
import pandas as pd
import os
import sys
from tqdm import tqdm
import logging
from xbrl.cache import HttpCache
from xbrl.instance import XbrlParser, XbrlInstance
import json
import re
from datetime import datetime, timedelta
from urllib.parse import urlencode
import time

from .s3_utils import write_json_to_s3, read_json_from_s3

# Suppress InsecureRequestWarning
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

USER_AGENT = "YourCustomResearchApp/1.0 (your.email@example.com)" 

logging.basicConfig(level=logging.INFO)
logging.getLogger('xbrl').setLevel(logging.DEBUG)

# --- PERMANENT CACHE DIRECTORY SETUP ---
# Create the xbrl_caches folder in the same directory as this script
script_dir = os.path.dirname(os.path.abspath(__file__))
xbrl_cache_dir = os.path.join(script_dir, "xbrl_caches")
os.makedirs(xbrl_cache_dir, exist_ok=True)
logging.info(f"XBRL cache directory set to: {xbrl_cache_dir}")

cache: HttpCache = HttpCache(xbrl_cache_dir, verify_https=False) # Keep verify=False here for SEC connections
cache.set_headers({'User-Agent': USER_AGENT})

'''
# --- ADD THIS SECTION FOR TAXONOMY CATALOG MAPPING ---
cyd_namespace = "http://xbrl.sec.gov/cyd/2024"
# Assuming you extracted the cyd-2024.zip into xbrl_caches/cyd/2024/
cyd_local_path = os.path.join(xbrl_cache_dir, "cyd", "2024", "cyd-2024.xsd") 

# Check if the local file exists before adding to catalog (optional, but good for debugging)
if os.path.exists(cyd_local_path):
    logging.info(f"Adding XBRL taxonomy catalog entry: {cyd_namespace} -> {cyd_local_path}")
    cache.add_catalog_entry(cyd_namespace, cyd_local_path)
else:
    logging.warning(f"Local CYD 2024 taxonomy not found at {cyd_local_path}. "
                    "XBRL parser will try to fetch it from the internet, which might cause errors.")
    # As a fallback, if you want it to always try the online version first, 
    # you could explicitly add it here, though the cache handles this by default.
    # cache.add_catalog_entry(cyd_namespace, "https://xbrl.sec.gov/cyd/2024/cyd-2024.xsd")
# --- END ADDITION ---
'''
parser = XbrlParser(cache)


# ------------------ UTILITY FUNCTIONS ------------------------------- #

def create_initialized_financial_dataframe_by_date(all_extracted_facts_dict):
    # ... (no change, already in your code) ...
    financial_accounting_variables = [
        'Revenue', 'CostofSales', 'GrossProfit', 'OperatingExpense', 'ResearchExpense', 'Depreciation', 'Amortization', 'OperatingIncome', 'Interest', 'Tax', 'NetIncome',
        'TotalAsset', 'CurrentAssets', 'Inventory', 'PPEnet', 'Equity(BV)', 'ShortTermDebt(BV)', 'LongTermDebt(BV)', 'Debt(BV)', 'CurrentLiabilities', 'TotalLiability', 
        'LongTermDebtWithoutLease(BV)', 'LongTermLease(BV)', 'LeaseDueThisYear', 'LeaseDueYearOne', 'LeaseDueYearTwo', 'LeaseDueYearThree', 'LeaseDueYearFour', 
        'LeaseDueYearFive', 'LeaseDueAfterYearFive', 'Cash'
    ]

    sorted_report_dates = sorted(all_extracted_facts_dict.keys())
    date_columns = [date.strftime('%Y-%m-%d') for date in sorted_report_dates]

    columns = ["Accounting Variable"] + date_columns

    new_df = pd.DataFrame(columns=columns)

    for var in financial_accounting_variables:
        row_data = {"Accounting Variable": var}
        for col_date_str in date_columns:
            row_data[col_date_str] = 0
        new_df = pd.concat([new_df, pd.DataFrame([row_data])], ignore_index=True)

    return new_df

def find_latest_tuple_by_string(data_list, search_string_list):
    # ... (no change, already in your code) ...
    search_string = None

    for search_element in search_string_list:
        for main_tuple in data_list:
            if search_element == main_tuple[0]:
                search_string = search_element
                break
        if search_string:
                break

    latest_tuple = None
    
    for current_tuple in data_list:
        str_value, num_value, dt_object = current_tuple

        if str_value == search_string:
            if latest_tuple is None:
                latest_tuple = current_tuple
            else:
                _, _, latest_dt_object = latest_tuple 
                if dt_object > latest_dt_object:
                    latest_tuple = current_tuple
                    
    return latest_tuple

def get_company_cik(ticker):
    """
    Fetches the CIK for a given stock ticker from SEC's public mapping.
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    headers = {'User-Agent': USER_AGENT}
    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status() # Raise an exception for HTTP errors
        tickers_data = response.json()
        
        for company_info in tickers_data.values():
            if company_info['ticker'] == ticker.upper():
                # CIKs are typically 10 digits and might be padded with leading zeros
                return str(company_info['cik_str']).zfill(10)
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching CIK for {ticker}: {e}")
        return None
    

def fetch_historical_10k_filings_api_get(cik, company_name):
    """
    Fetches the FIRST page (max 10 rows) of historical 10-K filings for a given company CIK
    using the SEC's LATEST/search-index API with a GET request,
    5-year date range, and fetches only the top 10 results.
    """
    base_search_api_url = "https://efts.sec.gov/LATEST/search-index" # Base URL
    headers = {'User-Agent': USER_AGENT} 
    
    all_filings_data = []
    
    # Define the date range for the last 5 years
    end_date_obj = datetime.now()
    start_date_obj = end_date_obj - timedelta(days=5 * 365) # Approximately 5 years ago
    
    start_date_str = start_date_obj.strftime("%Y-%m-%d")
    end_date_str = end_date_obj.strftime("%Y-%m-%d")

    # Modified: Only fetch the first page with max 10 filings
    start_offset = 0  # Always start from the beginning
    rows_per_page = 10 # Set to 10 to get max 10 filings on the first page

    print(f"  Starting 10-K search for CIK {cik} ({company_name}) from {start_date_str} to {end_date_str} (fetching first {rows_per_page} results)...")

    # The while True loop is removed as we only need one request for the first page
    params = {
        'ciks': cik,
        'entityName': f"{company_name.upper()} (CIK {cik})", 
        'filter_forms': '10-K', # Filter specifically for 10-K form types
        'startdt': start_date_str,
        'enddt': end_date_str,
        'start': start_offset, # Always 0 for the most recent filings
        'rows': rows_per_page,  # Now set to 10
    }

    # Manually construct the full URL with all parameters
    search_url_with_filters = f"{base_search_api_url}?{urlencode(params)}"
    print(f"  Requesting URL: {search_url_with_filters}") # For debugging/visibility

    try:
        # Send a GET request with the full parameterized URL
        response = requests.get(search_url_with_filters, headers=headers, verify=False)
        response.raise_for_status() # Raise an exception for HTTP errors
        search_results = response.json()

        # The filings data is located under 'hits' -> 'hits'
        filings_in_batch = search_results.get('hits', {}).get('hits', [])
        
        if not filings_in_batch:
            print(f"  No filings found for {company_name} in this date range.")
        
        for hit in filings_in_batch:
            source = hit.get('_source', {})
            _id = hit.get('_id', 'N/A') # Get the _id field which contains filename
            
            # Extract relevant fields. Field names based on previous successful JSON response.
            form_type = source.get('form', 'N/A')
            filing_date = source.get('file_date', 'N/A') 
            accession_number_with_dashes = source.get('adsh', 'N/A') 
            
            # Get 'period_ending' string and attempt to convert to datetime.date object
            reporting_date_str = source.get('period_ending', 'N/A')
            parsed_reporting_date = None
            if reporting_date_str != 'N/A':
                try:
                    # Parse the string into a datetime.date object
                    parsed_reporting_date = datetime.strptime(reporting_date_str, "%Y-%m-%d").date()
                except ValueError:
                    print(f"  Warning: Could not parse reporting date '{reporting_date_str}' for accession {accession_number_with_dashes}.")
                    parsed_reporting_date = None # Keep as None if parsing fails

            # Construct the link to the annual report
            report_link = 'N/A'
            if accession_number_with_dashes != 'N/A' and _id != 'N/A':
                # Extract CIK without leading zeros for the URL path
                cik_for_url = str(int(cik)) # Convert to int then back to str to remove leading zeros
                
                # Accession number without dashes for the URL path
                accession_number_without_dashes = accession_number_with_dashes.replace('-', '')

                # Filename is the part of _id after the colon
                filename = _id.split(':', 1)[-1] if ':' in _id else ''

                if cik_for_url and accession_number_without_dashes and filename:
                    report_link = (
                        f"https://www.sec.gov/Archives/edgar/data/"
                        f"{cik_for_url}/"
                        f"{accession_number_without_dashes}/"
                        f"{filename}"
                    )

            # Double-check form type if needed, though 'filter_forms' should be effective
            if form_type in ['10-K', '10-K/A']:
                all_filings_data.append({
                    'form_type': form_type,
                    'filing_date': filing_date,
                    'reporting_date': parsed_reporting_date, # Now stores a datetime.date object or None
                    'accession_number': accession_number_with_dashes, # Keep with dashes for raw data
                    'report_link': report_link, 
                    'cik': cik 
                })
        
        print(f"  Fetched {len(filings_in_batch)} filings in this batch. Total collected: {len(all_filings_data)}")
        
        # A small sleep after the single request to be polite
        time.sleep(0.5) 

    except requests.exceptions.HTTPError as e:
        print(f"  HTTP Error fetching data for CIK {cik} ({company_name}): {e}")
        if e.response.status_code == 403:
            print("  (403 Forbidden: Ensure your User-Agent is unique and valid, and increase sleep times).")
            print(f"  Response content: {e.response.text}") # Print response for more info
        elif e.response.status_code == 400:
            print(f"  (400 Bad Request: Check GET parameters. Response: {e.response.text})")
    except json.JSONDecodeError:
        print(f"  Error decoding JSON response for CIK {cik} ({company_name}).")
    except requests.exceptions.RequestException as e:
        print(f"  Network error for CIK {cik} ({company_name}): {e}")
    except Exception as e:
        print(f"  An unexpected error occurred for {company_name} (CIK: {cik}): {e}")

    return pd.DataFrame(all_filings_data)

# ------------ MAIN DATA PROCESSING FUNCTION ------------------------#
def xbrl_data_processor(trailing_data, ticker, cik_original, s3_bucket_name=None):
    # Call the GET API function which now fetches only the first page with max 10 rows
    df_filings = fetch_historical_10k_filings_api_get(cik_original,ticker) 

    if not df_filings.empty:
        print(f"  SUCCESS: Fetched {len(df_filings)} historical 10-K filings for {ticker} in the last 5 years.")
        print("\n  Sample of fetched data:")
        print(df_filings.head())

    else:
        print(f"  No historical 10-K filings found for {ticker} in the last 5 years.")




    df = df_filings

    df['s3_json_key'] = None

    loop_break_flag = False
    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        time.sleep(1)
        schema_url = row['report_link'] # this is now guaranteed to be a working link (or skipped)

        if schema_url is None: # Double check, though the filter above should handle it
            logging.warning(f"Skipping row {index} as no working EDGAR link was found.")
            df.at[index, 's3_json_key'] = "ERROR: No working EDGAR link"
            continue

        try:
            logging.info(f"Processing XBRL instance from: {schema_url}")
            inst: XbrlInstance = parser.parse_instance(schema_url)

            match = re.search(r'/([^/]+)\.htm$', schema_url)
            if match:
                base_filename = match.group(1)
            else:
                base_filename = f"unknown_file_{index}"
                logging.warning(f"Could not extract base filename from URL: {schema_url}. Using '{base_filename}'.")

            s3_key = f"xbrl_json_data/{base_filename}.json" 

            xbrl_json_data = inst.json()
            data_dict = json.loads(xbrl_json_data)

            write_json_to_s3(
                data=data_dict,
                file_key=s3_key,
                bucket_name=s3_bucket_name
            )
            logging.info(f"Successfully uploaded XBRL JSON to s3://{s3_bucket_name}/{s3_key}")

            df.at[index, 's3_json_key'] = s3_key

        except Exception as e:
            logging.error(f"Error processing and uploading {schema_url}: {e}")
            df.at[index, 's3_json_key'] = f"ERROR: {e}"
            if str(e) == "The taxonomy with namespace http://xbrl.sec.gov/cyd/2024 could not be found. Please check if it is imported in the schema file": 
                print("Ending loop now")
                loop_break_flag = True
                break

    if loop_break_flag:
        print("Exiting")
        sys.exit()

    print(f"\nAll XBRL instances processed. JSON files are uploaded to S3.")
    print("\nUpdated DataFrame with S3 JSON keys:")
    print(df)

    all_extracted_facts = {}

    logging.info("--- Extracting facts from S3-backed JSON files ---")
    for index, row in df.iterrows():
        company_main_list = []
        current_s3_json_key = row['s3_json_key'] 
        report_date = row['reporting_date']

        if not current_s3_json_key or "ERROR" in current_s3_json_key:
            logging.warning(f"Skipping row {index} due to invalid JSON filepath: {current_s3_json_key}")
            continue

        try:
            data = read_json_from_s3(file_key=current_s3_json_key, bucket_name=s3_bucket_name)

            if "facts" not in data:
                logging.warning(f"No 'facts' key found in JSON file: {current_s3_json_key}")
                continue

            for fact_key, fact_data in data["facts"].items():
                if ("dimensions" in fact_data and "concept" in fact_data["dimensions"] and
                        "period" in fact_data["dimensions"] and "value" in fact_data):
                    
                    if len(fact_data["dimensions"]) != 5:
                        continue

                    concept_value = fact_data["dimensions"]["concept"]
                    period_value = fact_data["dimensions"]["period"]
                    actual_value = fact_data["value"]

                    date_str = ""
                    if '/' in period_value:
                        parts = period_value.split('/')
                        if len(parts) != 2:
                            raise ValueError(f"Invalid range format for period '{period_value}' in '{current_s3_json_key}'. Expected 'start/end'.")
                        date_str = parts[1]
                    else:
                        date_str = period_value
                
                    try:
                        period_datetime = datetime.fromisoformat(date_str)
                    except ValueError as ve:
                        logging.warning(f"Could not parse date '{date_str}' from '{current_s3_json_key}' for concept '{concept_value}': {ve}. Skipping this fact.")
                        continue

                    company_main_list.append((concept_value, actual_value, datetime.fromisoformat(date_str)))
            all_extracted_facts[report_date] = company_main_list

        except FileNotFoundError:
            logging.error(f"JSON file not found: {current_s3_json_key}. Skipping.")
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON from file: {current_s3_json_key}: {e}. Skipping.")
        except Exception as e:
            logging.error(f"An unexpected error occurred while processing {current_s3_json_key}: {e}. Skipping.")

    print(f"\n--- Fact Extraction Complete ---")
    print(f"Total facts extracted from all JSON files: {len(all_extracted_facts)}")

    keys_view = all_extracted_facts.keys()
    #print(f"Using .keys(): {keys_view}")


# ----------- Creating Financial Company Database --------------# 

    initialized_financial_df = create_initialized_financial_dataframe_by_date(all_extracted_facts)

    #print("\nNew Initialized Financial DataFrame:")
    #print(initialized_financial_df)

    print("\n--- Populating DataFrame with extracted facts ---")

    for report_date_dt, company_main_list in all_extracted_facts.items():
        report_date_str = report_date_dt.strftime('%Y-%m-%d')

        if report_date_str in initialized_financial_df.columns:
            # Revenue Filling 
            revenue = find_latest_tuple_by_string(company_main_list, ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"])
            if revenue is not None:
               revenue = find_latest_tuple_by_string(company_main_list, ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"])[1]
            else:
                revenue = 0.0 
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Revenue'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = revenue
            
            # Operating Income Filling 
            operating_income = find_latest_tuple_by_string(company_main_list, ["OperatingIncomeLoss"])
            if operating_income is not None:
                operating_income = find_latest_tuple_by_string(company_main_list, ["OperatingIncomeLoss"])[1]
            else:
                operating_income = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'OperatingIncome'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = operating_income

            # Equity(Book Value) Filling 
            book_value_equity = find_latest_tuple_by_string(company_main_list, ["StockholdersEquity"])
            if book_value_equity is not None:
                book_value_equity = find_latest_tuple_by_string(company_main_list, ["StockholdersEquity"])[1]
            else:
                book_value_equity = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Equity(BV)'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = book_value_equity

            # ShortTermDebt(Book Value) Filling
            book_value_shortterm_debt = find_latest_tuple_by_string(company_main_list, ["DebtCurrent"])
            if book_value_shortterm_debt is not None:
                book_value_shortterm_debt = find_latest_tuple_by_string(company_main_list, ["DebtCurrent"])[1]
            else:
                book_value_shortterm_debt = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'ShortTermDebt(BV)'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = book_value_shortterm_debt

            # LongTermDebt without lease (Book Value) Filling
            book_value_longtermdebt_withoutlease = find_latest_tuple_by_string(company_main_list, ["LongTermDebtNoncurrent"])
            if book_value_longtermdebt_withoutlease is not None:
                book_value_longtermdebt_withoutlease = find_latest_tuple_by_string(company_main_list, ["LongTermDebtNoncurrent"])[1]
            else:
                book_value_longtermdebt_withoutlease = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LongTermDebtWithoutLease(BV)'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = book_value_longtermdebt_withoutlease

            # LongTermLease(Book Value) Filling
            book_value_longterm_lease = find_latest_tuple_by_string(company_main_list, ["LongTermLeaseLiabilityNoncurrentNet"])
            if book_value_longterm_lease is not None:
                book_value_longterm_lease = find_latest_tuple_by_string(company_main_list, ["LongTermLeaseLiabilityNoncurrentNet"])[1]
            else:
                book_value_longterm_lease = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LongTermLease(BV)'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = book_value_longterm_lease

            # LongTerm Debt(BV) Filling
            book_value_longtermdebt = book_value_longtermdebt_withoutlease + book_value_longterm_lease
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LongTermDebt(BV)'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = book_value_longtermdebt

            # Debt(BV) Filling
            book_value_debt = book_value_longtermdebt + book_value_shortterm_debt
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Debt(BV)'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = book_value_debt

            # Cash Filling
            cash = find_latest_tuple_by_string(company_main_list, ["CashAndCashEquivalentsAtCarryingValue"])
            if cash is not None:
                cash = find_latest_tuple_by_string(company_main_list, ["CashAndCashEquivalentsAtCarryingValue"])[1]
            else:
                cash = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Cash'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = cash

            # Tax Filling
            tax = find_latest_tuple_by_string(company_main_list, ["IncomeTaxExpenseBenefit"])
            if tax is not None:
                tax = find_latest_tuple_by_string(company_main_list, ["IncomeTaxExpenseBenefit"])[1]
            else:
                tax = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Tax'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = tax

            # Lease Values
            # This Year
            lease_thisyear = find_latest_tuple_by_string(company_main_list, ["CurrentLeaseLiabilityNet"])
            if lease_thisyear is not None:
                lease_thisyear = find_latest_tuple_by_string(company_main_list, ["CurrentLeaseLiabilityNet"])[1]
            else:
                lease_thisyear = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueThisYear'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = lease_thisyear

            # Year One (Lease)
            lease_yearone = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueNextTwelveMonths"])
            if lease_yearone is not None:
                lease_yearone = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueNextTwelveMonths"])[1]
            else:
                lease_yearone = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueYearOne'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = lease_yearone

            # Year Two (Lease)
            lease_yeartwo = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearTwo"])
            if lease_yeartwo is not None:
                lease_yeartwo = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearTwo"])[1]
            else:
                lease_yeartwo = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueYearTwo'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = lease_yeartwo

            # Year Three (Lease)
            lease_yearthree = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearThree"])
            if lease_yearthree is not None:
                lease_yearthree = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearThree"])[1]
            else:
                lease_yearthree = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueYearThree'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = lease_yearthree

            # Year Four (Lease)
            lease_yearfour = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearFour"])
            if lease_yearfour is not None:
                lease_yearfour = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearFour"])[1]
            else:
                lease_yearfour = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueYearFour'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = lease_yearfour

            # Year Five (Lease)
            lease_yearfive = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearFive"])
            if lease_yearfive is not None:
                lease_yearfive = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearFive"])[1]
            else:
                lease_yearfive = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueYearFive'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = lease_yearfive

            # Year After Five (Lease)
            lease_afteryearfive = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueAfterYearFive"])
            if lease_afteryearfive is not None:
                lease_afteryearfive = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueAfterYearFive"])[1]
            else:
                lease_afteryearfive = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueAfterYearFive'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = lease_afteryearfive


            # Net Income Filling
            netincome = find_latest_tuple_by_string(company_main_list, ["NetIncomeLoss"])
            if netincome is not None:
                netincome = find_latest_tuple_by_string(company_main_list, ["NetIncomeLoss"])[1]
            else:
                netincome = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'NetIncome'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = netincome

            # Current Assets Filling
            currentasset = find_latest_tuple_by_string(company_main_list, ["AssetsCurrent"])
            if currentasset is not None:
                currentasset = find_latest_tuple_by_string(company_main_list, ["AssetsCurrent"])[1]
            else:
                currentasset = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'CurrentAssets'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = currentasset

            # Current Liabilities Filling
            currentliability = find_latest_tuple_by_string(company_main_list, ["LiabilitiesCurrent"])
            if currentliability is not None:
                currentliability = find_latest_tuple_by_string(company_main_list, ["LiabilitiesCurrent"])[1]
            else:
                currentliability = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'CurrentLiabilities'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = currentliability

            # Total Liability Filling
            totalliabilityplus_equitybv = find_latest_tuple_by_string(company_main_list, ["LiabilitiesAndStockholdersEquity"])
            if totalliabilityplus_equitybv is not None:
                totalliabilityplus_equitybv = find_latest_tuple_by_string(company_main_list, ["LiabilitiesAndStockholdersEquity"])[1]
            else:
                totalliabilityplus_equitybv = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'TotalLiability'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = totalliabilityplus_equitybv - book_value_equity

            # Total Assets Filling
            totalassets = find_latest_tuple_by_string(company_main_list, ["Assets"])
            if totalassets is not None:
                totalassets = find_latest_tuple_by_string(company_main_list, ["Assets"])[1]
            else:
                totalassets = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'TotalAsset'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = totalassets

            # Inventory Filling
            inventory = find_latest_tuple_by_string(company_main_list, ["InventoryNet"])
            if inventory is not None:
                inventory = find_latest_tuple_by_string(company_main_list, ["InventoryNet"])[1]
            else:
                inventory = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Inventory'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = inventory

            # CostOfSales Filling
            cogs = find_latest_tuple_by_string(company_main_list, ["CostOfRevenue", "CostOfGoodsAndServicesSold"])
            if cogs is not None:
                cogs = find_latest_tuple_by_string(company_main_list, ["CostOfRevenue", "CostOfGoodsAndServicesSold"])[1]
            else:
                cogs = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'CostofSales'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = cogs

            # GrossProfit Filling
            grossprofit = find_latest_tuple_by_string(company_main_list, ["GrossProfit"])
            if grossprofit is not None:
                grossprofit = find_latest_tuple_by_string(company_main_list, ["GrossProfit"])[1]
            else:
                grossprofit = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'GrossProfit'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = grossprofit

            # OperatingExpense Filling
            operatingexpense = find_latest_tuple_by_string(company_main_list, ["CostsAndExpenses"])
            if operatingexpense is not None:
                operatingexpense = find_latest_tuple_by_string(company_main_list, ["CostsAndExpenses"])[1]
            else:
                operatingexpense = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'OperatingExpense'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = operatingexpense

            # ResearchExpense Filling
            researchexpense = find_latest_tuple_by_string(company_main_list, ["ResearchAndDevelopmentExpense"])
            if researchexpense is not None:
                researchexpense = find_latest_tuple_by_string(company_main_list, ["ResearchAndDevelopmentExpense"])[1]
            else:
                researchexpense = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'ResearchExpense'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = researchexpense

            # InterestExpense Filling
            interestexpense = find_latest_tuple_by_string(company_main_list, ["InterestExpense"])
            if interestexpense is not None:
                interestexpense = find_latest_tuple_by_string(company_main_list, ["InterestExpense"])[1]
            else:
                interestexpense = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Interest'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = interestexpense

            # PPE-net Filling
            ppenet = find_latest_tuple_by_string(company_main_list, ["PropertyPlantAndEquipmentNet"])
            if ppenet is not None:
                ppenet = find_latest_tuple_by_string(company_main_list, ["PropertyPlantAndEquipmentNet"])[1]
            else:
                ppenet = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'PPEnet'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = ppenet

            # Depreciation Filling
            depreciation = find_latest_tuple_by_string(company_main_list, ["Depreciation", "DepreciationDepletionAndAmortization", "DepreciationAmortizationAndOther", "DepreciationAmortizationAndAccretionNet"])
            if depreciation is not None:
                depreciation = find_latest_tuple_by_string(company_main_list, ["Depreciation", "DepreciationDepletionAndAmortization", "DepreciationAmortizationAndOther", "DepreciationAmortizationAndAccretionNet"])[1]
            else:
                ppenet = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Depreciation'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = depreciation
            
            # Amortization Filling
            amortization = find_latest_tuple_by_string(company_main_list, ["AmortizationOfIntangibleAssets"])
            if amortization is not None:
                amortization = find_latest_tuple_by_string(company_main_list, ["AmortizationOfIntangibleAssets"])[1]
            else:
                amortization = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Amortization'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = amortization

    print(initialized_financial_df)
    return initialized_financial_df
    #initialized_financial_df.to_csv('company_data.csv', index=False)
    #df_cleaned.to_csv('company_meta_data.csv', index=False)