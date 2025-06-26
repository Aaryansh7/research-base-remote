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

# Assuming s3_utils.py is in the same 'headers' directory or accessible via relative path
# from .s3_utils import write_json_to_s3, read_json_from_s3 # This import is fine if run as part of package
# For standalone script, ensure s3_utils is in PYTHONPATH or copied to same dir.
# For this example, we assume s3_utils is available via relative import or path setup.

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
        # Add other variations if you discover them, e.g., CIK-based filename:
        # f"{cik_original}-{formatted_date}.htm",
        # f"{cik_original.lstrip('0')}-{formatted_date}.htm"
        f"{accession_no_dashes}.htm", # Common alternative, direct accession number HTML
    ]

    candidate_urls = []
    for current_cik in cik_variations:
        for htm_filename in htm_filename_variations:
            url = f"https://www.sec.gov/Archives/edgar/data/{current_cik}/{accession_no_dashes}/{htm_filename}"
            candidate_urls.append(url)
            
    # Add a general index.htm link for the accession number's directory
    url_index_htm = f"https://www.sec.gov/Archives/edgar/data/{cik_original}/{accession_no_dashes}/index.htm"
    candidate_urls.append(url_index_htm)

    # print(candidate_urls) # Uncomment for debugging link generation
    return candidate_urls

def check_multiple_links(urls):
    """
    Checks a list of URLs and returns a list of URLs that return a 200 status code.
    """
    working_links = []
    # IMPORTANT: Replace with your actual identifying information.
    headers = {'User-Agent': 'FinancialDataValidator/1.0 (contact@example.com)'} 
    
    for url in urls:
        try:
            response = requests.head(url, allow_redirects=True, timeout=10, headers=headers, verify=False)
            if response.status_code == 200:
                working_links.append(url)
        except requests.exceptions.RequestException as e:
            # print(f"Link failed: {url} - {e}") # Uncomment for deeper debugging
            pass # Just move to the next URL if there's an error
    return working_links



# ------------ MAIN DATA PROCESSING FUNCTION ------------------------#
def xbrl_data_processor(trailing_data, ticker, cik_original, s3_bucket_name=None):
    # Ensure 'reportDate' is datetime and 'ticker' is present for link generation
    trailing_data['reportDate'] = pd.to_datetime(trailing_data['reportDate'])
    trailing_data['ticker'] = ticker.lower() # Ensure ticker column is available for link generation
    
    # --- Link Validation and Selection ---
    #print("Generating EDGAR link candidates...")
    trailing_data['edgar_link_candidates'] = trailing_data.apply(
        lambda row: generate_edgar_link_candidates(row, cik_original), axis=1
    )
    
    # Use tqdm for progress when checking links
    #tqdm.pandas(desc=f"Checking EDGAR links for {ticker}")
    # Check each set of candidates and get a list of working links
    trailing_data['working_edgar_links'] = trailing_data['edgar_link_candidates'].apply(check_multiple_links)

    

    # From the list of working links, take the first one found (or None if list is empty)
    # This will be the definitive link used for parsing for actual data processing
    trailing_data['edgar_link'] = trailing_data['working_edgar_links'].apply(lambda x: x[0] if x else None)

    # Filter out rows where no working link was found for further processing
    df_cleaned = trailing_data[trailing_data['edgar_link'].notna()].copy()

    # Drop temporary columns as they are no longer needed for subsequent processing
    df_cleaned = df_cleaned.drop(columns=['edgar_link_candidates', 'working_edgar_links'])

    # Calculate total working links for this company for the validation record
    total_working_links_for_company = df_cleaned.shape[0]

    #print(f"Original DataFrame size for {ticker}: {len(trailing_data)}")
    #print(f"Cleaned DataFrame size (after removing broken links) for {ticker}: {len(df_cleaned)}")
    #print(f"Total working links found for {ticker}: {total_working_links_for_company}")

    return total_working_links_for_company

   