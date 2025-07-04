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

# Assuming s3_utils.py is in the same directory or accessible in PYTHONPATH
# from .s3_utils import write_json_to_s3, read_json_from_s3
# For standalone execution without S3, we'll mock these or provide simple placeholders
try:
    from .s3_utils import write_json_to_s3, read_json_from_s3
except ImportError:
    logging.warning("s3_utils not found. Mocking S3 functions for local testing.")
    def write_json_to_s3(data, file_key, bucket_name):
        print(f"Mock S3: Writing to {bucket_name}/{file_key}")
        # Example: write to a local file for testing
        os.makedirs(os.path.dirname(file_key), exist_ok=True)
        with open(file_key, 'w') as f:
            json.dump(data, f, indent=4)

    def read_json_from_s3(file_key, bucket_name):
        print(f"Mock S3: Reading from {bucket_name}/{file_key}")
        # Example: read from a local file for testing
        with open(file_key, 'r') as f:
            return json.load(f)


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

parser = XbrlParser(cache)


# ------------------ UTILITY FUNCTIONS ------------------------------- #

def create_initialized_financial_dataframe_by_date(all_extracted_facts_dict):
    financial_accounting_variables = [
        'Revenue', 'CostofSales', 'GrossProfit', 'OperatingExpense', 'ResearchExpense', 'Depreciation', 'Amortization', 'OperatingIncome', 'OperatingIncomeAfterInterest', 'InteresIncome', 'Interest', 'Tax', 'NetIncome',
        'TotalAsset', 'CurrentAssets', 'Inventory', 'PPEnet', 'MinorityInterest', 'EquityIncludingMinorityInterest', 'Equity(BV)', 'ShortTermDebt(BV)', 'LongTermDebtWithLease(BV)', 'Debt(BV)', 'CurrentLiabilities', 'TotalLiability',
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
    search_string = None

    for search_element in search_string_list:
        for main_tuple in data_list:
            if main_tuple[0] == search_element: # Directly compare the concept string
                search_string = search_element
                break
        if search_string:
                break

    latest_tuple = None

    if search_string is None: # If no search string found, return None
        return None

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

    params = {
        'ciks': cik,
        'entityName': f"{company_name.upper()} (CIK {cik})",
        'filter_forms': '10-K', # Filter specifically for 10-K form types
        'startdt': start_date_str,
        'enddt': end_date_str,
        'start': start_offset, # Always 0 for the most recent filings
        'rows': rows_per_page,  # Now set to 10
    }

    search_url_with_filters = f"{base_search_api_url}?{urlencode(params)}"
    print(f"  Requesting URL: {search_url_with_filters}")

    try:
        response = requests.get(search_url_with_filters, headers=headers, verify=False)
        response.raise_for_status()
        search_results = response.json()

        filings_in_batch = search_results.get('hits', {}).get('hits', [])

        if not filings_in_batch:
            print(f"  No filings found for {company_name} in this date range.")

        for hit in filings_in_batch:
            source = hit.get('_source', {})
            _id = hit.get('_id', 'N/A')

            form_type = source.get('form', 'N/A')
            filing_date = source.get('file_date', 'N/A')
            accession_number_with_dashes = source.get('adsh', 'N/A')

            reporting_date_str = source.get('period_ending', 'N/A')
            parsed_reporting_date = None
            if reporting_date_str != 'N/A':
                try:
                    parsed_reporting_date = datetime.strptime(reporting_date_str, "%Y-%m-%d").date()
                except ValueError:
                    print(f"  Warning: Could not parse reporting date '{reporting_date_str}' for accession {accession_number_with_dashes}.")
                    parsed_reporting_date = None

            report_link = 'N/A'
            if accession_number_with_dashes != 'N/A' and _id != 'N/A':
                cik_for_url = str(int(cik))
                accession_number_without_dashes = accession_number_with_dashes.replace('-', '')
                filename = _id.split(':', 1)[-1] if ':' in _id else ''

                if cik_for_url and accession_number_without_dashes and filename:
                    report_link = (
                        f"https://www.sec.gov/Archives/edgar/data/"
                        f"{cik_for_url}/"
                        f"{accession_number_without_dashes}/"
                        f"{filename}"
                    )

            if form_type in ['10-K', '10-K/A']:
                all_filings_data.append({
                    'form_type': form_type,
                    'filing_date': filing_date,
                    'reporting_date': parsed_reporting_date,
                    'accession_number': accession_number_with_dashes,
                    'report_link': report_link,
                    'cik': cik
                })

        print(f"  Fetched {len(filings_in_batch)} filings in this batch. Total collected: {len(all_filings_data)}")
        time.sleep(0.5)

    except requests.exceptions.HTTPError as e:
        print(f"  HTTP Error fetching data for CIK {cik} ({company_name}): {e}")
        if e.response.status_code == 403:
            print("  (403 Forbidden: Ensure your User-Agent is unique and valid, and increase sleep times).")
            print(f"  Response content: {e.response.text}")
        elif e.response.status_code == 400:
            print(f"  (400 Bad Request: Check GET parameters. Response: {e.response.text})")
    except json.JSONDecodeError:
        print(f"  Error decoding JSON response for CIK {cik} ({company_name}).")
    except requests.exceptions.RequestException as e:
        print(f"  Network error for CIK {cik} ({company_name}): {e}")
    except Exception as e:
        print(f"  An unexpected error occurred for {company_name} (CIK: {cik}): {e}")

    return pd.DataFrame(all_filings_data)

def fetch_company_facts_from_sec_api(cik):
    """
    Fetches all company facts for a given CIK from the SEC's companyfacts API.
    Transforms the data into a list of (concept, value, date) tuples.
    """
    cik_padded = str(cik).zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json"
    headers = {'User-Agent': USER_AGENT}
    all_facts = []
    print(f"  Attempting to fetch company facts from SEC API for CIK: {cik_padded}")

    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        company_facts_data = response.json()

        # Calculate the 5-year lookback date from today
        today = datetime.now().date()
        five_years_ago = today - timedelta(days=5 * 365) # Approximate 5 years

        for taxonomy_type in ['us-gaap', 'dei']:
            if taxonomy_type in company_facts_data.get('facts', {}):
                for concept, concept_data in company_facts_data['facts'][taxonomy_type].items():
                    for unit, unit_data_list in concept_data.get('units', {}).items():
                        for fact_entry in unit_data_list:
                            try:
                                date_str = fact_entry.get('end')
                                if not date_str:
                                    continue
                                period_datetime = datetime.fromisoformat(date_str)

                                # Filter facts to only include those within the last 5 years
                                if period_datetime.date() >= five_years_ago: # Compare date parts only
                                    value = fact_entry.get('val')
                                    if value is not None:
                                        all_facts.append((concept, value, period_datetime))
                                else:
                                    pass # Fact is older than 5 years, skip it
                            except ValueError as ve:
                                logging.warning(f"  Could not parse date or value for concept '{concept}' from SEC API: {ve}. Skipping fact.")
                                continue
                            except Exception as ex:
                                logging.warning(f"  Error processing fact for concept '{concept}' from SEC API: {ex}. Skipping fact.")
                                continue
        print(f"  Successfully fetched and processed {len(all_facts)} facts (filtered to last 5 years) from SEC Company Facts API.")
        return all_facts
    except requests.exceptions.HTTPError as e:
        print(f"  HTTP Error fetching company facts for CIK {cik_padded}: {e}")
        if e.response.status_code == 404:
            print(f"  (404 Not Found: Company facts might not be available for CIK {cik_padded} or API path is incorrect).")
        print(f"  Response content: {e.response.text}")
    except json.JSONDecodeError:
        print(f"  Error decoding JSON response from SEC Company Facts API for CIK {cik_padded}.")
    except requests.exceptions.RequestException as e:
        print(f"  Network error fetching company facts for CIK {cik_padded}: {e}")
    except Exception as e:
        print(f"  An unexpected error occurred while fetching company facts for CIK {cik_padded}: {e}")
    return []


# ------------ MAIN DATA PROCESSING FUNCTION ------------------------#
def xbrl_data_processor(trailing_data, ticker, cik_original, s3_bucket_name=None):
    df_filings = fetch_historical_10k_filings_api_get(cik_original,ticker)

    all_extracted_facts_from_xbrl = {} # Facts extracted directly from XBRL instance files
    MIN_FACTS_THRESHOLD = 1000 # Threshold for individual XBRL instance raw facts

    should_use_api_fallback = False # Flag to decide if we need global API fallback

    # Collect all valid reporting dates from df_filings
    # These are the specific dates for which we expect to have data, either from XBRL or API.
    valid_reporting_dates = set()
    if not df_filings.empty:
        valid_reporting_dates = set(df_filings['reporting_date'].dropna())
        print(f"  Valid reporting dates from filings: {sorted([d.strftime('%Y-%m-%d') for d in valid_reporting_dates])}")
    else:
        print(f"  No historical 10-K filings found for {ticker} in the last 5 years using original method.")
        should_use_api_fallback = True # If no filings, definitely need fallback

    if not df_filings.empty:
        print(f"  SUCCESS: Fetched {len(df_filings)} historical 10-K filings for {ticker} in the last 5 years.")
        print("\n  Sample of fetched data:")
        print(df_filings.head())

        df = df_filings.copy()
        df['s3_json_key'] = None
        loop_break_flag = False

        logging.info("--- Processing XBRL instances from EDGAR links ---")
        for index, row in df.iterrows():
            time.sleep(1)
            schema_url = row['report_link']
            report_date = row['reporting_date']

            # Only process if report_date is valid (not None) and in our expected list
            if report_date is None or report_date not in valid_reporting_dates:
                logging.warning(f"Skipping row {index} with invalid or unexpected report date: {report_date}.")
                df.at[index, 's3_json_key'] = "ERROR: Invalid or unexpected report date"
                continue

            if schema_url == 'N/A':
                logging.warning(f"Skipping row {index} as no valid EDGAR link was found.")
                df.at[index, 's3_json_key'] = "ERROR: No working EDGAR link"
                # If a filing has no link, it contributes to needing fallback for its date
                should_use_api_fallback = True # Global fallback triggered if any specific filing fails this way
                all_extracted_facts_from_xbrl[report_date] = [] # Mark as empty for this date
                continue

            try:
                logging.info(f"Processing XBRL instance from: {schema_url}")
                inst: XbrlInstance = parser.parse_instance(schema_url)
                xbrl_json_data = inst.json()
                data_dict = json.loads(xbrl_json_data)

                # Count facts directly from data_dict['facts'] immediately as requested
                current_instance_raw_facts_count = len(data_dict.get("facts", {}))
                print(f"  Raw facts found in {os.path.basename(schema_url)} (from data_dict): {current_instance_raw_facts_count}")

                # **Decision Point: Check if we should proceed with original method or use SEC API**
                if current_instance_raw_facts_count < MIN_FACTS_THRESHOLD:
                    logging.warning(f"  Raw facts ({current_instance_raw_facts_count}) for {os.path.basename(schema_url)} are below threshold ({MIN_FACTS_THRESHOLD}). This will trigger global API fallback.")
                    should_use_api_fallback = True
                    # Do NOT extract or filter facts for this report here.
                    all_extracted_facts_from_xbrl[report_date] = [] # Mark as empty or discard these insufficient facts
                    df.at[index, 's3_json_key'] = f"facts below threshold ({current_instance_raw_facts_count}), using companyfacts API json" # Mark in DF
                    
                    # Continue to the next filing, but don't populate detailed facts from this one
                    s3_key = f"xbrl_json_data/{os.path.basename(schema_url).replace('.htm', '.json')}"
                    write_json_to_s3(
                        data=data_dict,
                        file_key=s3_key,
                        bucket_name=s3_bucket_name
                    )
                    logging.info(f"Successfully uploaded raw XBRL JSON to s3://{s3_bucket_name}/{s3_key}")
                    continue # Skip to the next iteration of the loop
                
                # IF we reach here, it means current_instance_raw_facts_count >= MIN_FACTS_THRESHOLD
                # So, proceed with extracting and filtering facts for this report
                company_main_list_for_report = []
                if "facts" in data_dict:
                    for fact_key, fact_data in data_dict["facts"].items():
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
                                date_str = parts[1] if len(parts) > 1 else period_value
                            else:
                                date_str = period_value

                            try:
                                period_datetime = datetime.fromisoformat(date_str)
                                # Ensure the extracted fact's date is relevant to the current report_date
                                # For XBRL instances, the fact's period_datetime should ideally match the report_date
                                # or be the end date of the reporting period. We will still add it.
                                company_main_list_for_report.append((concept_value, actual_value, period_datetime))
                            except ValueError as ve:
                                logging.warning(f"  Could not parse date '{date_str}' for concept '{concept_value}' in {schema_url}: {ve}. Skipping this fact for report {report_date}.")
                                continue
                
                all_extracted_facts_from_xbrl[report_date] = company_main_list_for_report

                # Always attempt to upload the raw JSON to S3 for debugging/archiving
                s3_key = f"xbrl_json_data/{os.path.basename(schema_url).replace('.htm', '.json')}"
                write_json_to_s3(
                    data=data_dict,
                    file_key=s3_key,
                    bucket_name=s3_bucket_name
                )
                logging.info(f"Successfully uploaded XBRL JSON to s3://{s3_bucket_name}/{s3_key}")
                df.at[index, 's3_json_key'] = s3_key # Record success of S3 upload

            except Exception as e:
                logging.error(f"Error processing {schema_url}: {e}")
                df.at[index, 's3_json_key'] = f"ERROR {e}"
                all_extracted_facts_from_xbrl[report_date] = [] # Mark as empty due to error
                should_use_api_fallback = True # An error processing an XBRL also triggers fallback
                if "The taxonomy with namespace http://xbrl.sec.gov/cyd/2024 could not be found" in str(e):
                    print("Ending loop due to missing CYD taxonomy (critical error).")
                    loop_break_flag = True
                    break

        print(df)
        if loop_break_flag:
            print("Exiting XBRL parsing loop early.")
            sys.exit(1)
    # else: condition for should_use_api_fallback already handled at the top

    final_facts_for_processing = {}
    if should_use_api_fallback:
        print(f"\n--- Initiating global fallback to SEC Company Facts API for CIK {cik_original} (filtering for last 5 years and filing dates) ---")
        sec_api_facts = fetch_company_facts_from_sec_api(cik_original) # This function already filters for last 5 years
        
        if sec_api_facts:
            # Filter API facts to only include those relevant to the dates found in df_filings
            api_facts_for_filing_dates = []
            for concept, value, date_obj in sec_api_facts:
                if date_obj.date() in valid_reporting_dates: # Check if the fact's date is in our expected filing dates
                    api_facts_for_filing_dates.append((concept, value, date_obj))
                # else:
                #    logging.debug(f"Skipping API fact {concept} for date {date_obj.date()} as it's not a filing date.")
            
            if api_facts_for_filing_dates:
                # Group filtered API facts by their end date for consistent processing
                for concept, value, date_obj in api_facts_for_filing_dates:
                    date_key = date_obj.date() # Use only date part for keying
                    if date_key not in final_facts_for_processing:
                        final_facts_for_processing[date_key] = []
                    final_facts_for_processing[date_key].append((concept, value, date_obj))
                print(f"Successfully collected {len(api_facts_for_filing_dates)} facts from SEC Company Facts API (filtered by filing dates) for processing.")
            else:
                print("  Filtered SEC Company Facts API data did not yield any facts matching filing dates.")
                # If API fallback filtered too much, still use whatever limited XBRL facts we might have
                final_facts_for_processing = {
                    k.date() if isinstance(k, datetime) else k: v
                    for k, v in all_extracted_facts_from_xbrl.items()
                    if v # Only include entries that actually have facts
                }

        else: # sec_api_facts was empty from the start
            print("  SEC Company Facts API fallback did not yield any data.")
            # If API fallback also fails, use whatever limited facts were extracted from XBRL (even if insufficient per-filing)
            final_facts_for_processing = {
                k.date() if isinstance(k, datetime) else k: v
                for k, v in all_extracted_facts_from_xbrl.items()
                if v # Only include entries that actually have facts
            }
    else:
        print(f"\nSufficient facts extracted from XBRL instances. Skipping SEC Company Facts API fallback.")
        final_facts_for_processing = {
            k.date() if isinstance(k, datetime) else k: v
            for k, v in all_extracted_facts_from_xbrl.items()
            if v # Only include entries that actually have facts
        }


    if not final_facts_for_processing:
        print("No facts available to create financial dataframe. Returning empty DataFrame.")
        return pd.DataFrame()


    initialized_financial_df = create_initialized_financial_dataframe_by_date(final_facts_for_processing)

    print("\n--- Populating DataFrame with extracted facts ---")

    for report_date_dt, company_main_list in final_facts_for_processing.items():
        report_date_str = report_date_dt.strftime('%Y-%m-%d')

        if report_date_str in initialized_financial_df.columns:
            # Revenue Filling
            revenue = find_latest_tuple_by_string(company_main_list, ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"])
            if revenue is not None:
               revenue = revenue[1]
            else:
                revenue = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Revenue'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = revenue

            # Operating Income Filling
            operating_income = find_latest_tuple_by_string(company_main_list, ["OperatingIncomeLoss"])
            if operating_income is not None:
                operating_income = operating_income[1]
            else:
                operating_income = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'OperatingIncome'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = operating_income

            # Equity(Book Value) Filling
            book_value_equity = find_latest_tuple_by_string(company_main_list, ["StockholdersEquity"])
            if book_value_equity is not None:
                book_value_equity = book_value_equity[1]
            else:
                book_value_equity = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Equity(BV)'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = book_value_equity

            # ShortTermDebt(Book Value) Filling
            book_value_shortterm_debt = find_latest_tuple_by_string(company_main_list, ["DebtCurrent"])
            if book_value_shortterm_debt is not None:
                book_value_shortterm_debt = book_value_shortterm_debt[1]
            else:
                book_value_shortterm_debt = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'ShortTermDebt(BV)'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = book_value_shortterm_debt

            # LongTermDebt without lease (Book Value) Filling
            book_value_longtermdebt_withoutlease = find_latest_tuple_by_string(company_main_list, ["LongTermDebtNoncurrent", "LongTermDebt"])
            if book_value_longtermdebt_withoutlease is not None:
                book_value_longtermdebt_withoutlease = book_value_longtermdebt_withoutlease[1]
            else:
                book_value_longtermdebt_withoutlease = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LongTermDebtWithoutLease(BV)'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = book_value_longtermdebt_withoutlease

            # LongTermLease(Book Value) Filling
            book_value_longterm_lease = find_latest_tuple_by_string(company_main_list, ["LongTermLeaseLiabilityNoncurrentNet"])
            if book_value_longterm_lease is not None:
                book_value_longterm_lease = book_value_longterm_lease[1]
            else:
                book_value_longterm_lease = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LongTermLease(BV)'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = book_value_longterm_lease

            # LongTerm Debt(BV) Filling
            book_value_longtermdebt = find_latest_tuple_by_string(company_main_list, ["LongTermDebtAndCapitalLeaseObligations", "LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities", "DebtAndCapitalLeaseObligations"])
            if book_value_longtermdebt is not None:
                book_value_longtermdebt = book_value_longtermdebt[1]
            else:
                book_value_longtermdebt = book_value_longtermdebt_withoutlease + book_value_longterm_lease
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LongTermDebtWithLease(BV)'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = book_value_longtermdebt

            # Debt(BV) Filling
            book_value_debt = book_value_longtermdebt + book_value_shortterm_debt
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Debt(BV)'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = book_value_debt

            # Cash Filling
            cash = find_latest_tuple_by_string(company_main_list, ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"])
            if cash is not None:
                cash = cash[1]
            else:
                cash = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Cash'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = cash

            # Tax Filling
            tax = find_latest_tuple_by_string(company_main_list, ["IncomeTaxExpenseBenefit"])
            if tax is not None:
                tax = tax[1]
            else:
                tax = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Tax'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = tax

            # Lease Values
            # This Year
            lease_thisyear = find_latest_tuple_by_string(company_main_list, ["CurrentLeaseLiabilityNet"])
            if lease_thisyear is not None:
                lease_thisyear = lease_thisyear[1]
            else:
                lease_thisyear = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueThisYear'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = lease_thisyear

            # Year One (Lease)
            lease_yearone = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueNextTwelveMonths"])
            if lease_yearone is not None:
                lease_yearone = lease_yearone[1]
            else:
                lease_yearone = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueYearOne'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = lease_yearone

            # Year Two (Lease)
            lease_yeartwo = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearTwo"])
            if lease_yeartwo is not None:
                lease_yeartwo = lease_yeartwo[1]
            else:
                lease_yeartwo = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueYearTwo'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = lease_yeartwo

            # Year Three (Lease)
            lease_yearthree = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearThree"])
            if lease_yearthree is not None:
                lease_yearthree = lease_yearthree[1]
            else:
                lease_yearthree = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueYearThree'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = lease_yearthree

            # Year Four (Lease)
            lease_yearfour = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearFour"])
            if lease_yearfour is not None:
                lease_yearfour = lease_yearfour[1]
            else:
                lease_yearfour = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueYearFour'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = lease_yearfour

            # Year Five (Lease)
            lease_yearfive = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearFive"])
            if lease_yearfive is not None:
                lease_yearfive = lease_yearfive[1]
            else:
                lease_yearfive = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueYearFive'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = lease_yearfive

            # Year After Five (Lease)
            lease_afteryearfive = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueAfterYearFive"])
            if lease_afteryearfive is not None:
                lease_afteryearfive = lease_afteryearfive[1]
            else:
                lease_afteryearfive = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueAfterYearFive'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = lease_afteryearfive


            # Net Income Filling
            netincome = find_latest_tuple_by_string(company_main_list, ["NetIncomeLoss", "ProfitLoss", "NetIncomeLossAvailableToCommonStockholdersBasic"])
            if netincome is not None:
                netincome = netincome[1]
            else:
                netincome = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'NetIncome'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = netincome

            # Current Assets Filling
            currentasset = find_latest_tuple_by_string(company_main_list, ["AssetsCurrent"])
            if currentasset is not None:
                currentasset = currentasset[1]
            else:
                currentasset = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'CurrentAssets'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = currentasset

            # Current Liabilities Filling
            currentliability = find_latest_tuple_by_string(company_main_list, ["LiabilitiesCurrent"])
            if currentliability is not None:
                currentliability = currentliability[1]
            else:
                currentliability = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'CurrentLiabilities'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = currentliability

            # Total Liability Filling
            totalliabilityplus_equitybv = find_latest_tuple_by_string(company_main_list, ["LiabilitiesAndStockholdersEquity"])
            if totalliabilityplus_equitybv is not None:
                totalliabilityplus_equitybv = totalliabilityplus_equitybv[1]
            else:
                totalliabilityplus_equitybv = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'TotalLiability'].index

            if not row_index.empty:
                # Ensure book_value_equity is available for this calculation, if not, use 0.0
                equity_for_calc = book_value_equity if book_value_equity is not None else 0.0
                initialized_financial_df.at[row_index[0], report_date_str] = totalliabilityplus_equitybv - equity_for_calc

            # Total Assets Filling
            totalassets = find_latest_tuple_by_string(company_main_list, ["Assets"])
            if totalassets is not None:
                totalassets = totalassets[1]
            else:
                totalassets = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'TotalAsset'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = totalassets

            # Inventory Filling
            inventory = find_latest_tuple_by_string(company_main_list, ["InventoryNet"])
            if inventory is not None:
                inventory = inventory[1]
            else:
                inventory = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Inventory'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = inventory

            # CostOfSales Filling
            cogs = find_latest_tuple_by_string(company_main_list, ["CostOfRevenue", "CostOfGoodsAndServicesSold"])
            if cogs is not None:
                cogs = cogs[1]
            else:
                cogs = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'CostofSales'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = cogs

            # GrossProfit Filling
            grossprofit = find_latest_tuple_by_string(company_main_list, ["GrossProfit"])
            if grossprofit is not None:
                grossprofit = grossprofit[1]
            else:
                grossprofit = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'GrossProfit'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = grossprofit

            # OperatingExpense Filling
            operatingexpense = find_latest_tuple_by_string(company_main_list, ["CostsAndExpenses"])
            if operatingexpense is not None:
                operatingexpense = operatingexpense[1]
            else:
                operatingexpense = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'OperatingExpense'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = operatingexpense

            # ResearchExpense Filling
            researchexpense = find_latest_tuple_by_string(company_main_list, ["ResearchAndDevelopmentExpense"])
            if researchexpense is not None:
                researchexpense = researchexpense[1]
            else:
                researchexpense = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'ResearchExpense'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = researchexpense

            # InterestExpense Filling
            interestexpense = find_latest_tuple_by_string(company_main_list, ["InterestExpense", "InterestExpenseNonoperating", "InterestAndDebtExpense", "InterestIncomeExpenseNet"])
            if interestexpense is not None:
                interestexpense = interestexpense[1]
            else:
                interestexpense = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Interest'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = interestexpense

            # PPE-net Filling
            ppenet = find_latest_tuple_by_string(company_main_list, ["PropertyPlantAndEquipmentNet"])
            if ppenet is not None:
                ppenet = ppenet[1]
            else:
                ppenet = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'PPEnet'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = ppenet

            # Depreciation Filling
            depreciation = find_latest_tuple_by_string(company_main_list, ["Depreciation", "DepreciationDepletionAndAmortization", "DepreciationAmortizationAndOther", "DepreciationAmortizationAndAccretionNet"])
            if depreciation is not None:
                depreciation = depreciation[1]
            else:
                depreciation = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Depreciation'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = depreciation

            # Amortization Filling
            amortization = find_latest_tuple_by_string(company_main_list, ["AmortizationOfIntangibleAssets"])
            if amortization is not None:
                amortization = amortization[1]
            else:
                amortization = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Amortization'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = amortization

            # OperatingIncomeAfterINterestExpense Filling
            incomeafterinterest = find_latest_tuple_by_string(company_main_list, ["IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments", "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest"])
            if incomeafterinterest is not None:
                incomeafterinterest = incomeafterinterest[1]
            else:
                incomeafterinterest = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'OperatingIncomeAfterInterest'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = incomeafterinterest

            # MinorityInterest Filling
            miniorityinterest = find_latest_tuple_by_string(company_main_list, ["MinorityInterest"])
            if miniorityinterest is not None:
                miniorityinterest = miniorityinterest[1]
            else:
                miniorityinterest = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'MinorityInterest'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = miniorityinterest

            # EquityIncludingMinorityInterest Filling
            equitywithminiorityinterest = find_latest_tuple_by_string(company_main_list, ["StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"])
            if equitywithminiorityinterest is not None:
                equitywithminiorityinterest = equitywithminiorityinterest[1]
            else:
                equitywithminiorityinterest = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'EquityIncludingMinorityInterest'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = equitywithminiorityinterest

            # InterestIncome Filling
            interestincome = find_latest_tuple_by_string(company_main_list, ["InvestmentIncomeInterest"])
            if interestincome is not None:
                interestincome = interestincome[1]
            else:
                interestincome = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'InterestIncome'].index

            if not row_index.empty:
                initialized_financial_df.at[row_index[0], report_date_str] = interestincome

    print(initialized_financial_df)
    return initialized_financial_df