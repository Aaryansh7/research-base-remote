import requests
import pandas as pd
import os
from tqdm import tqdm
import logging
from xbrl.cache import HttpCache
from xbrl.instance import XbrlParser, XbrlInstance
import json
import re
from datetime import datetime

from .s3_utils import write_json_to_s3, read_json_from_s3

# Suppress InsecureRequestWarning
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ------ UTILITY FUNCTIONS --------------------#

def generate_edgar_link_candidates(row, cik_original):
    """
    Generates a list of potential EDGAR XBRL instance HTML links
    based on different CIK and filename formatting conventions.
    """
    accession_no_dashes = row['accessionNumber'].replace('-', '')
    formatted_date = row['reportDate'].strftime('%Y%m%d')
    ticker = row['ticker']

    cik_variations = [
        cik_original,
        cik_original.lstrip('0') # CIK with leading zeros removed
    ]

    htm_filename_variations = [
        f"{ticker}-{formatted_date}.htm",
    ]
    # Add other variations if you discover them, e.g., CIK-based filename:
    # f"{cik_original}-{formatted_date}.htm"
    # f"{cik_original.lstrip('0')}-{formatted_date}.htm"

    candidate_urls = []
    for current_cik in cik_variations:
        for htm_filename in htm_filename_variations:
            url = f"https://www.sec.gov/Archives/edgar/data/{current_cik}/{accession_no_dashes}/{htm_filename}"
            candidate_urls.append(url)
            
    # Add the "old-style" link from the accession number directly, often ends in .txt or .xml but can be .htm
    # This variation assumes the accession number itself is part of the path, not just the file name suffix.
    # This is often for the index.htm or main document itself.
    # It might already be covered by the edgarAPI or if accessionNumber.htm format works.
    # For a full XBRL instance, it's typically a specific .htm file.
    # Example: https://www.sec.gov/Archives/edgar/data/320193/000032019323000077/aapl-20230930.htm
    # The existing logic should generally cover the common formats.
    # The primary issue is typically the CIK formatting and filename variations.

    print(candidate_urls)
    return candidate_urls

def check_multiple_links(urls):
    """
    Checks a list of URLs and returns a list of URLs that return a 200 status code.
    """
    working_links = []
    headers = {'User-Agent': 'YourCompanyName YourEmail@example.com'} # Replace with your info
    
    for url in urls:
        try:
            response = requests.head(url, allow_redirects=True, timeout=10, headers=headers, verify=False)
            if response.status_code == 200:
                working_links.append(url)
        except requests.exceptions.RequestException:
            # print(f"Link failed: {url} - {e}") # Uncomment for deeper debugging
            pass # Just move to the next URL if there's an error
    return working_links

def create_initialized_financial_dataframe_by_date(all_extracted_facts_dict):
    # ... (no change, already in your code) ...
    financial_accounting_variables = [
        'Revenue', 'OperatingIncome', 'Equity(BV)', 'ShortTermDebt(BV)',
        'LongTermDebtWithoutLease(BV)', 'LongTermLease(BV)', 'LongTermDebt(BV)', 'Debt(BV)', 'Cash', 'Tax', 
        'LeaseDueThisYear', 'LeaseDueYearOne', 'LeaseDueYearTwo', 'LeaseDueYearThree', 'LeaseDueYearFour', 'LeaseDueYearFive',
        'LeaseDueAfterYearFive', 'NetIncome', 'CurrentAssets', 'CurrentLiabilities', 'TotalLiability', 'TotalAsset', 'Inventory'
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


# ------------ MAIN DATA PROCESSING FUNCTION ------------------------#
def xbrl_data_processor(trailing_data, ticker, cik_original, s3_bucket_name=None):
    company_ticker = ticker
    trailing_data['reportDate'] = pd.to_datetime(trailing_data['reportDate'])
    trailing_data['ticker'] = ticker.lower()
    
    # --- MODIFICATION START ---
    # Generate ALL possible link candidates for each row
    print("Generating EDGAR link candidates...")
    trailing_data['edgar_link_candidates'] = trailing_data.apply(
        lambda row: generate_edgar_link_candidates(row, cik_original), axis=1
    )
    
    # Use tqdm for progress when checking links
    tqdm.pandas(desc="Checking EDGAR links")
    # Check each set of candidates and get a list of working links
    trailing_data['working_edgar_links'] = trailing_data['edgar_link_candidates'].progress_apply(check_multiple_links)

    # From the list of working links, take the first one found (or None if list is empty)
    # This will be the definitive link used for parsing
    trailing_data['edgar_link'] = trailing_data['working_edgar_links'].apply(lambda x: x[0] if x else None)

    # Filter out rows where no working link was found
    df_cleaned = trailing_data[trailing_data['edgar_link'].notna()].copy()

    # Drop temporary columns if you don't need them
    df_cleaned = df_cleaned.drop(columns=['edgar_link_candidates', 'working_edgar_links'])
    # --- MODIFICATION END ---

    print("Original DataFrame size:", len(trailing_data))
    print("Cleaned DataFrame size (after removing broken links):", len(df_cleaned))

    logging.basicConfig(level=logging.INFO)

    xbrl_cache_dir = os.path.join("/tmp", "xbrl_cache")
    os.makedirs(xbrl_cache_dir, exist_ok=True)
    cache: HttpCache = HttpCache(xbrl_cache_dir, verify_https=False) # Keep verify=False here for SEC connections
    
    cache.set_headers({'From': 'YOUR@EMAIL.com', 'User-Agent': 'Company Name AdminContact@<company-domain>.com'})
    parser = XbrlParser(cache)

    df = df_cleaned

    df['s3_json_key'] = None

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        schema_url = row['edgar_link'] # This is now guaranteed to be a working link (or skipped)

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

    print(f"\nAll XBRL instances processed. JSON files are uploaded to S3.")
    print("\nUpdated DataFrame with S3 JSON keys:")
    print(df)

    all_extracted_facts = {}

    logging.info("--- Extracting facts from S3-backed JSON files ---")
    for index, row in df.iterrows():
        company_main_list = []
        current_s3_json_key = row['s3_json_key'] 
        ticker = row['ticker']
        report_date = row['reportDate']

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
    print(f"Using .keys(): {keys_view}")


# ----------- Creating Financial Company Database --------------# 

    initialized_financial_df = create_initialized_financial_dataframe_by_date(all_extracted_facts)

    print("\nNew Initialized Financial DataFrame:")
    print(initialized_financial_df)

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

    print(initialized_financial_df)
    return initialized_financial_df
    #initialized_financial_df.to_csv('company_data.csv', index=False)
    #df_cleaned.to_csv('company_meta_data.csv', index=False)