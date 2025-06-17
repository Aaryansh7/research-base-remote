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

# ------ UTILITY FUNCTION --------------------#
def create_edgar_link(row):
    cik = row['accessionNumber'].split('-')[0]
    accession_no_dashes = row['accessionNumber'].replace('-', '')
    formatted_date = row['reportDate'].strftime('%Y%m%d')
    ticker = row['ticker'] # Use the ticker from the DataFrame

    # For the htm file name, typically it's ticker-YYYYMMDD.htm or just CIK-YYYYMMDD.htm
    # The example given uses ticker-YYYYMMDD.htm, which is common for 10-K/Q.
    htm_filename = f"{ticker}-{formatted_date}.htm"

    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_dashes}/{htm_filename}"


def check_link(url):
    """
    Checks if a given URL is accessible.
    Returns True if the link is working (status code 200), False otherwise.
    Includes a User-Agent header to avoid being blocked by SEC.
    """
    try:
        headers = {'User-Agent': 'YourCompanyName YourEmail@example.com'} # Replace with your info
        response = requests.head(url, allow_redirects=True, timeout=5, headers=headers, verify=False)
        return response.status_code == 200
    except requests.exceptions.RequestException as e:
        # print(f"Error checking {url}: {e}") # Uncomment for debugging
        return False
    

def create_initialized_financial_dataframe_by_date(all_extracted_facts_dict):
    """
    Creates a new Pandas DataFrame with accounting variables as the first column
    and report dates (keys from all_extracted_facts_dict) as subsequent columns,
    initialized with zeros.

    Args:
        all_extracted_facts_dict (dict): A dictionary where keys are report dates
                                         and values are lists of (concept, value, period) tuples.

    Returns:
        pandas.DataFrame: The newly created DataFrame.
    """

    financial_accounting_variables = [
        'Revenue', 'OperatingIncome', 'Equity(BV)', 'ShortTermDebt(BV)',
        'LongTermDebtWithoutLease(BV)', 'LongTermLease(BV)', 'LongTermDebt(BV)', 'Debt(BV)', 'Cash', 'Tax', \
        'LeaseDueThisYear', 'LeaseDueYearOne', 'LeaseDueYearTwo', 'LeaseDueYearThree', 'LeaseDueYearFour', 'LeaseDueYearFive',\
        'LeaseDueAfterYearFive', 'NetIncome'
    ]

    # Get sorted report dates to use as column headers
    # Ensure dates are in a display-friendly string format for columns
    sorted_report_dates = sorted(all_extracted_facts_dict.keys())
    date_columns = [date.strftime('%Y-%m-%d') for date in sorted_report_dates]

    # Define the columns for the new DataFrame
    columns = ["Accounting Variable"] + date_columns

    # Create an empty DataFrame
    new_df = pd.DataFrame(columns=columns)

    # Populate the "Accounting Variable" column and initialize other cells to zero
    for var in financial_accounting_variables:
        row_data = {"Accounting Variable": var}
        for col_date_str in date_columns:
            row_data[col_date_str] = 0 # Initialize with 0
        new_df = pd.concat([new_df, pd.DataFrame([row_data])], ignore_index=True)

    return new_df


def find_latest_tuple_by_string(data_list, search_string_list):
    """
    Iterates over a list of (string, numeric, datetime) tuples
    and finds the tuple with the greatest datetime for a given string.

    Args:
        data_list (list): A list of tuples, where each tuple is
                          (str_value, num_value, datetime_object).
        mode : The Accounting item to be looked for
        ticker: Company code

    Returns:
        tuple or None: The tuple with the matching accounting item and the latest datetime,
                       or None if no matching tuple is found.
    """
    search_string = None

    for search_element in search_string_list:
        for main_tuple in data_list:
            if search_element == main_tuple[0]:
                search_string = search_element
                break  # Found a match, so break out of the inner loop
        if search_string:  # If search_string is no longer None, a match was found
                break      # Break out of the outer loop as well

    latest_tuple = None
    
    for current_tuple in data_list:
        str_value, num_value, dt_object = current_tuple

        if str_value == search_string:
            if latest_tuple is None:
                # First matching tuple found
                latest_tuple = current_tuple
            else:
                # Compare datetimes if a matching tuple already exists
                # We need to compare the datetime part of the current_tuple
                # with the datetime part of the latest_tuple found so far.
                _, _, latest_dt_object = latest_tuple 
                if dt_object > latest_dt_object:
                    latest_tuple = current_tuple
                    
    return latest_tuple


# ------------ MAIN DATA PROCESSING FUNCTION ------------------------#
def xbrl_data_processor(trailing_data, ticker):
    company_ticker = ticker
    trailing_data['reportDate'] = pd.to_datetime(trailing_data['reportDate'])
    trailing_data['ticker'] = ticker.lower()
    trailing_data['edgar_link'] = trailing_data.apply(create_edgar_link, axis=1)

    # Apply the check_link function to the 'edgar_link' column
    # Using tqdm for a progress bar, which is helpful for many links
    tqdm.pandas() # Initialize tqdm for pandas apply
    trailing_data['link_working'] = trailing_data['edgar_link'].progress_apply(check_link)

    # Filter out rows where 'link_working' is False
    df_cleaned = trailing_data[trailing_data['link_working']].copy()

    # Drop the temporary 'link_working' column if you don't need it
    df_cleaned = df_cleaned.drop(columns=['link_working'])

    print("Original DataFrame size:", len(trailing_data))
    print("Cleaned DataFrame size (after removing broken links):", len(df_cleaned))

    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Setup XBRL cache and parser
    cache: HttpCache = HttpCache('./xbrl_cache', verify_https=False)
    cache.set_headers({'From': 'YOUR@EMAIL.com', 'User-Agent': 'Company Name AdminContact@<company-domain>.com'})
    parser = XbrlParser(cache)

    df = df_cleaned

    # Add a new column to store JSON file paths, initialized with None or empty string
    df['json_filepath'] = None

    # Directory to save JSON files
    output_dir = './xbrl_json_data'
    os.makedirs(output_dir, exist_ok=True)

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        schema_url = row['edgar_link']

        try:
            logging.info(f"Processing XBRL instance from: {schema_url}")
            inst: XbrlInstance = parser.parse_instance(schema_url)

            # Extract the last keyword from the URL for the filename
            match = re.search(r'/([^/]+)\.htm$', schema_url)
            if match:
                base_filename = match.group(1)
            else:
                base_filename = f"unknown_file_{index}"
                logging.warning(f"Could not extract base filename from URL: {schema_url}. Using '{base_filename}'.")

            filename = f"{base_filename}.json"
            filepath = os.path.join(output_dir, filename)

            # Save to file
            inst.json(filepath)
            logging.info(f"Successfully saved XBRL JSON to: {filepath}")

            # Update the DataFrame with the JSON file path
            df.at[index, 'json_filepath'] = filepath

        except Exception as e:
            logging.error(f"Error processing {schema_url}: {e}")
            # Optionally, mark the path as an error or None if processing failed
            df.at[index, 'json_filepath'] = f"ERROR: {e}"

    print(f"\nAll XBRL instances processed. JSON files are saved in the '{output_dir}' directory.")
    print("\nUpdated DataFrame with JSON file paths:")
    print(df)

# --- END: Dummy DataFrame Creation and Initial JSON Generation ---


# --- START: Iterating over JSON files and extracting facts ---

# This list will store all extracted fact dictionaries from all JSON files
    all_extracted_facts = {}

    logging.info("--- Extracting facts from generated JSON files ---")
    for index, row in df.iterrows():
        company_main_list = []
        json_filepath = row['json_filepath']
        ticker = row['ticker'] # Optionally carry ticker information
        report_date = row['reportDate']

        # Skip if the file path is an error or None
        if not json_filepath or "ERROR" in json_filepath:
            logging.warning(f"Skipping row {index} due to invalid JSON filepath: {json_filepath}")
            continue

        try:
            logging.info(f"Loading and processing facts from JSON file: {json_filepath}")
            with open(json_filepath, 'r') as f:
                data = json.load(f)

            if "facts" not in data:
                logging.warning(f"No 'facts' key found in JSON file: {json_filepath}")
                continue

            for fact_key, fact_data in data["facts"].items():
                # Ensure necessary keys exist before accessing
                if ("dimensions" in fact_data and "concept" in fact_data["dimensions"] and
                        "period" in fact_data["dimensions"] and "value" in fact_data):
                    
                    # Check the current fact's dimensions key nums
                    if len(fact_data["dimensions"]) is not 5:
                        continue

                    concept_value = fact_data["dimensions"]["concept"]
                    period_value = fact_data["dimensions"]["period"]
                    actual_value = fact_data["value"]

                    date_str = ""
                    if '/' in period_value:
                        # It's a range, take the second part (end date)
                        parts = period_value.split('/')
                        if len(parts) != 2:
                            raise ValueError(f"Invalid range format for period '{period_value}' in '{json_filepath}'. Expected 'start/end'.")
                        date_str = parts[1]
                    else:
                        # It's a single datetime
                        date_str = period_value
                
                    # Convert date string to datetime object
                    try:
                        period_datetime = datetime.fromisoformat(date_str)
                    except ValueError as ve:
                        logging.warning(f"Could not parse date '{date_str}' from '{json_filepath}' for concept '{concept_value}': {ve}. Skipping this fact.")
                        continue

                    company_main_list.append((concept_value, actual_value, datetime.fromisoformat(date_str)))
            all_extracted_facts[report_date] = company_main_list

        except FileNotFoundError:
            logging.error(f"JSON file not found: {json_filepath}. Skipping.")
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON from file: {json_filepath}: {e}. Skipping.")
        except Exception as e:
            logging.error(f"An unexpected error occurred while processing {json_filepath}: {e}. Skipping.")

    print(f"\n--- Fact Extraction Complete ---")
    print(f"Total facts extracted from all JSON files: {len(all_extracted_facts)}")

    # Get a view of the keys
    keys_view = all_extracted_facts.keys()
    print(f"Using .keys(): {keys_view}")


# ----------- Creating Financial Compnay Database --------------# 

    # Call the function to create the DataFrame
    initialized_financial_df = create_initialized_financial_dataframe_by_date(all_extracted_facts)

    # Print the new DataFrame
    print("\nNew Initialized Financial DataFrame:")
    print(initialized_financial_df)

    # 2. Populate the DataFrame
    print("\n--- Populating DataFrame with extracted facts ---")

    for report_date_dt, company_main_list in all_extracted_facts.items():
        # Convert the datetime object key to the string format used in DataFrame columns
        report_date_str = report_date_dt.strftime('%Y-%m-%d')
        #print(report_date_str)

        # Check if this date column exists in the DataFrame
        if report_date_str in initialized_financial_df.columns:
            # Revenue Filling 
            revenue = find_latest_tuple_by_string(company_main_list, ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"])
            if revenue is not None:
               revenue = find_latest_tuple_by_string(company_main_list, ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"])[1]
            else:
                revenue = 0.0 
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Revenue'].index

            if not row_index.empty:
                # If a matching accounting variable row is found, update the cell
                initialized_financial_df.at[row_index[0], report_date_str] = revenue
            
            # Operating Income Filling 
            operating_income = find_latest_tuple_by_string(company_main_list, ["OperatingIncomeLoss"])
            if operating_income is not None:
                operating_income = find_latest_tuple_by_string(company_main_list, ["OperatingIncomeLoss"])[1]
            else:
                operating_income = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'OperatingIncome'].index

            if not row_index.empty:
                # If a matching accounting variable row is found, update the cell
                initialized_financial_df.at[row_index[0], report_date_str] = operating_income

            # Equity(Book Value) Filling 
            book_value_equity = find_latest_tuple_by_string(company_main_list, ["StockholdersEquity"])
            if book_value_equity is not None:
                book_value_equity = find_latest_tuple_by_string(company_main_list, ["StockholdersEquity"])[1]
            else:
                book_value_equity = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Equity(BV)'].index

            if not row_index.empty:
                # If a matching accounting variable row is found, update the cell
                initialized_financial_df.at[row_index[0], report_date_str] = book_value_equity

            # ShortTermDebt(Book Value) Filling
            book_value_shortterm_debt = find_latest_tuple_by_string(company_main_list, ["DebtCurrent"])
            if book_value_shortterm_debt is not None:
                book_value_shortterm_debt = find_latest_tuple_by_string(company_main_list, ["DebtCurrent"])[1]
            else:
                book_value_shortterm_debt = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'ShortTermDebt(BV)'].index

            if not row_index.empty:
                # If a matching accounting variable row is found, update the cell
                initialized_financial_df.at[row_index[0], report_date_str] = book_value_shortterm_debt

            # LongTermDebt without lease (Book Value) Filling
            book_value_longtermdebt_withoutlease = find_latest_tuple_by_string(company_main_list, ["LongTermDebtNoncurrent"])
            if book_value_longtermdebt_withoutlease is not None:
                book_value_longtermdebt_withoutlease = find_latest_tuple_by_string(company_main_list, ["LongTermDebtNoncurrent"])[1]
            else:
                book_value_longtermdebt_withoutlease = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LongTermDebtWithoutLease(BV)'].index

            if not row_index.empty:
                # If a matching accounting variable row is found, update the cell
                initialized_financial_df.at[row_index[0], report_date_str] = book_value_longtermdebt_withoutlease

            # LongTermLease(Book Value) Filling
            book_value_longterm_lease = find_latest_tuple_by_string(company_main_list, ["LongTermLeaseLiabilityNoncurrentNet"])
            if book_value_longterm_lease is not None:
                book_value_longterm_lease = find_latest_tuple_by_string(company_main_list, ["LongTermLeaseLiabilityNoncurrentNet"])[1]
            else:
                book_value_longterm_lease = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LongTermLease(BV)'].index

            if not row_index.empty:
                # If a matching accounting variable row is found, update the cell
                initialized_financial_df.at[row_index[0], report_date_str] = book_value_longterm_lease

            # LongTerm Debt(BV) Filling
            book_value_longtermdebt = book_value_longtermdebt_withoutlease + book_value_longterm_lease
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LongTermDebt(BV)'].index

            if not row_index.empty:
                # If a matching accounting variable row is found, update the cell
                initialized_financial_df.at[row_index[0], report_date_str] = book_value_longtermdebt

            # Debt(BV) Filling
            book_value_debt = book_value_longtermdebt + book_value_shortterm_debt
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Debt(BV)'].index

            if not row_index.empty:
                # If a matching accounting variable row is found, update the cell
                initialized_financial_df.at[row_index[0], report_date_str] = book_value_debt

            # Cash Filling
            cash = find_latest_tuple_by_string(company_main_list, ["CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"])
            if cash is not None:
                cash = find_latest_tuple_by_string(company_main_list, ["CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"])[1]
            else:
                cash = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Cash'].index

            if not row_index.empty:
                # If a matching accounting variable row is found, update the cell
                initialized_financial_df.at[row_index[0], report_date_str] = cash

            # Tax Filling
            tax = find_latest_tuple_by_string(company_main_list, ["IncomeTaxExpenseBenefit"])
            if tax is not None:
                tax = find_latest_tuple_by_string(company_main_list, ["IncomeTaxExpenseBenefit"])[1]
            else:
                tax = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'Tax'].index

            if not row_index.empty:
                # If a matching accounting variable row is found, update the cell
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
                # If a matching accounting variable row is found, update the cell
                initialized_financial_df.at[row_index[0], report_date_str] = lease_thisyear

            # Year One (Lease)
            lease_yearone = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueNextTwelveMonths"])
            if lease_yearone is not None:
                lease_yearone = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueNextTwelveMonths"])[1]
            else:
                lease_yearone = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueYearOne'].index

            if not row_index.empty:
                # If a matching accounting variable row is found, update the cell
                initialized_financial_df.at[row_index[0], report_date_str] = lease_yearone

            # Year Two (Lease)
            lease_yeartwo = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearTwo"])
            if lease_yeartwo is not None:
                lease_yeartwo = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearTwo"])[1]
            else:
                lease_yeartwo = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueYearTwo'].index

            if not row_index.empty:
                # If a matching accounting variable row is found, update the cell
                initialized_financial_df.at[row_index[0], report_date_str] = lease_yeartwo

            # Year Three (Lease)
            lease_yearthree = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearThree"])
            if lease_yearthree is not None:
                lease_yearthree = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearThree"])[1]
            else:
                lease_yearthree = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueYearThree'].index

            if not row_index.empty:
                # If a matching accounting variable row is found, update the cell
                initialized_financial_df.at[row_index[0], report_date_str] = lease_yearthree

            # Year Four (Lease)
            lease_yearfour = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearFour"])
            if lease_yearfour is not None:
                lease_yearfour = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearFour"])[1]
            else:
                lease_yearfour = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueYearFour'].index

            if not row_index.empty:
                # If a matching accounting variable row is found, update the cell
                initialized_financial_df.at[row_index[0], report_date_str] = lease_yearfour

            # Year Five (Lease)
            lease_yearfive = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearFive"])
            if lease_yearfive is not None:
                lease_yearfive = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueYearFive"])[1]
            else:
                lease_yearfive = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueYearFive'].index

            if not row_index.empty:
                # If a matching accounting variable row is found, update the cell
                initialized_financial_df.at[row_index[0], report_date_str] = lease_yearfive

            # Year After Five (Lease)
            lease_afteryearfive = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueAfterYearFive"])
            if lease_afteryearfive is not None:
                lease_afteryearfive = find_latest_tuple_by_string(company_main_list, ["LesseeOperatingLeaseLiabilityPaymentsDueAfterYearFive"])[1]
            else:
                lease_afteryearfive = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'LeaseDueAfterYearFive'].index

            if not row_index.empty:
                # If a matching accounting variable row is found, update the cell
                initialized_financial_df.at[row_index[0], report_date_str] = lease_afteryearfive


            # Net Income Filling
            netincome = find_latest_tuple_by_string(company_main_list, ["NetIncomeLoss"])
            if netincome is not None:
                netincome = find_latest_tuple_by_string(company_main_list, ["NetIncomeLoss"])[1]
            else:
                netincome = 0.0
            row_index = initialized_financial_df[initialized_financial_df['Accounting Variable'] == 'NetIncome'].index

            if not row_index.empty:
                # If a matching accounting variable row is found, update the cell
                initialized_financial_df.at[row_index[0], report_date_str] = netincome

    print(initialized_financial_df)
    return initialized_financial_df
    #initialized_financial_df.to_csv('company_data.csv', index=False)
    #df_cleaned.to_csv('company_meta_data.csv', index=False)

