import requests
import json
import time
import warnings
warnings.filterwarnings("ignore")

def get_company_cik(ticker):
    """
    Fetches the CIK for a given stock ticker.
    This uses a publicly available mapping from SEC.
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    headers = {
        'User-Agent': 'YourAppName/1.0 (your.email@example.com)' # Replace with your app name and email
    }
    response = requests.get(url, headers=headers, verify=False)
    response.raise_for_status() # Raise an exception for HTTP errors

    tickers_data = response.json()
    #print(tickers_data)
    
    for company_info in tickers_data.values():
        if company_info['ticker'] == ticker.upper():
            # CIKs are typically 10 digits and might be padded with leading zeros
            return str(company_info['cik_str']).zfill(10)
    return None

def fetch_10k_submission_dates(cik, company_name):
    """
    Fetches 10-K filing submission dates for a given company CIK.
    """
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    headers = {
        'User-Agent': 'Aryan/1.0 (your.email@example.com)' # Replace with your app name and email
    }

    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status() # Raise an exception for HTTP errors
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for CIK {cik} ({company_name}): {e}")
        return []

    filings = data.get('filings', {})
    recent_filings = filings.get('recent', {})

    form_types = recent_filings.get('form', [])
    accession_numbers = recent_filings.get('accessionNumber', [])
    filing_dates = recent_filings.get('filingDate', [])

    ten_k_filings = []
    for i in range(len(form_types)):
        if form_types[i] == '10-K' or form_types[i] == '10-K/A': # Include amended 10-Ks
            ten_k_filings.append({
                'form_type': form_types[i],
                'accession_number': accession_numbers[i],
                'submission_date': filing_dates[i]
            })
    return ten_k_filings

if __name__ == "__main__":
    company_tickers = ["IBM"] # Example tickers

    for ticker in company_tickers:
        print(f"\nFetching 10-K submission dates for {ticker}...")
        cik = get_company_cik(ticker)
        time.sleep(0.2)
        if cik:
            print(f"  CIK for {ticker}: {cik}")
            ten_k_data = fetch_10k_submission_dates(cik, ticker)
            
            if ten_k_data:
                print(f"  Found {len(ten_k_data)} 10-K filings:")
                for filing in ten_k_data:
                    print(f"    Form Type: {filing['form_type']}, Submission Date: {filing['submission_date']}")
            else:
                print(f"  No 10-K filings found for {ticker}.")
        else:
            print(f"  Could not find CIK for {ticker}. Please check the ticker symbol.")
        
        time.sleep(0.2) # Be respectful to SEC's rate limits