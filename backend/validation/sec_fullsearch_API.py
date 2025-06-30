import requests
import json
import time
import warnings
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import urlencode # Import urlencode to build the query string

warnings.filterwarnings("ignore") # Ignore unverified HTTPS requests warning for simplicity, but avoid in production.

# --- IMPORTANT: Configure your User-Agent ---
# Replace with your actual application name and email address.
# This is crucial for SEC compliance and avoiding 403 errors.
# Example: "MyInvestmentApp/1.0 (your.name@example.com)"
USER_AGENT = "YourCustomResearchApp/1.0 (your.email@example.com)" 

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
            reporting_date = source.get('period_ending', 'N/A')

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
                    'reporting_date': reporting_date, 
                    'accession_number': accession_number_with_dashes, # Keep with dashes for raw data
                    'report_link': report_link, # New: Link to the annual report
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


if __name__ == "__main__":
    # Example tickers to test. Make sure to replace USER_AGENT at the top of the file!
    company_tickers = ["JPM"] 

    all_companies_10k_data_dfs = {}

    for ticker in company_tickers:
        print(f"\n--- Processing {ticker} ---")
        cik = get_company_cik(ticker)
        time.sleep(0.5) # Small delay after CIK lookup
        
        if cik:
            print(f"  Found CIK for {ticker}: {cik}")
            # Call the GET API function which now fetches only the first page with max 10 rows
            df_filings = fetch_historical_10k_filings_api_get(cik, ticker) 
            
            if not df_filings.empty:
                print(f"  SUCCESS: Fetched {len(df_filings)} historical 10-K filings for {ticker} in the last 5 years.")
                all_companies_10k_data_dfs[ticker] = df_filings
                print("\n  Sample of fetched data:")
                print(df_filings.head())

                # --- NEW ADDITION: Print complete links ---
                print(f"\n  Complete Annual Report Links for {ticker}:")
                for index, row in df_filings.iterrows():
                    print(f"    - {row['filing_date']} ({row['form_type']}): {row['report_link']}")
                # --- END NEW ADDITION ---

            else:
                print(f"  No historical 10-K filings found for {ticker} in the last 5 years.")
        else:
            print(f"  Could not find CIK for {ticker}. Please check the ticker symbol or network connection.")
        
        # Longer sleep between companies to be very polite to SEC servers
        print(f"\nPausing for 3 seconds before processing next company...")
        time.sleep(3) 

    print("\n--- All Processing Complete ---")
    print("\nSummary of 10-K Filings Found (Last 5 Years, First Page Max 10 Rows):")
    for ticker, df in all_companies_10k_data_dfs.items():
        print(f"- {ticker}: {len(df)} filings")

    # Example: Accessing data for MSFT
    if 'MSFT' in all_companies_10k_data_dfs:
        print("\nFull DataFrame for MSFT (all fetched rows):")
        print(all_companies_10k_data_dfs['MSFT'])
