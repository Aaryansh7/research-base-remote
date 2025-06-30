import requests
import pandas as pd
import warnings
import json # Import json for explicit parsing
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time # For rate limiting, if not already implemented

class sec_edgar_endpoint:
    # ... (existing __init__ with session, headers, and rate limiting logic) ...
    def __init__(self):
        self.headers = {"User-Agent": 'FinancialDataValidator/1.0 (contact@example.com)'}
        # Create a session for connection pooling
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def get_cik_matching_ticker(self, ticker):
        #self._apply_rate_limit() # Ensure rate limit is applied
        ticker = ticker.upper()
        self.ticker = ticker
        
        # DEBUGGING CHANGE START
        print(f"Fetching company_tickers.json for {self.ticker}...")
        response = self.session.get("https://www.sec.gov/files/company_tickers.json", verify=False, timeout=15) # Add timeout
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        
        if not response.text.strip(): # Check if response body is empty
            print(f"ERROR: company_tickers.json response is empty for {self.ticker}. Status: {response.status_code}")
            raise ValueError(f"Empty response for company_tickers.json while processing {self.ticker}")

        try:
            ticker_json = response.json()
        except json.JSONDecodeError as e:
            print(f"ERROR: Failed to parse JSON for company_tickers.json for {self.ticker}. Error: {e}")
            print(f"Response text (first 500 chars): {response.text[:500]}")
            raise ValueError(f"Invalid JSON for company_tickers.json while processing {self.ticker}") from e
        # DEBUGGING CHANGE END

        for company in ticker_json.values():
            if company["ticker"] == self.ticker:
                self.cik = str(company["cik_str"]).zfill(10)
                return self.cik
            
        raise ValueError(f"Ticker {self.ticker} not found in SEC database")
    
    def get_submission_data(self):
        url = f"https://data.sec.gov/submissions/CIK{self.cik}.json"
        
        # DEBUGGING CHANGE START
        print(f"Fetching submission data for CIK {self.cik} from URL: {url}...")
        response = self.session.get(url, verify=False, timeout=15) # Add timeout
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)

        if not response.text.strip(): # Check if response body is empty
            print(f"ERROR: Submission data response is empty for CIK {self.cik}. Status: {response.status_code}")
            raise ValueError(f"Empty response for submission data for CIK {self.cik}")

        try:
            self.company_submission_data = response.json()
        except json.JSONDecodeError as e:
            print(f"ERROR: Failed to parse JSON for submission data for CIK {self.cik}. Error: {e}")
            print(f"Response text (first 500 chars): {response.text[:500]}")
            raise ValueError(f"Invalid JSON for submission data for CIK {self.cik}") from e
        # DEBUGGING CHANGE END
        
        return self.company_submission_data
    
    def get_filtered_filings_data(self):
        # Returns only rows with form 10-K or 10-Q
        self.only_filings_df = pd.DataFrame(self.company_submission_data['filings']['recent'])
        #print(self.only_filings_df)
        self.filtered_filings_df = self.only_filings_df[(self.only_filings_df['form'] == '10-K') |( self.only_filings_df['form'] == '10-Q')]
        self.filtered_filings_df = self.filtered_filings_df[['accessionNumber', 'reportDate', 'form']]
        self.filtered_filings_df = self.filtered_filings_df.reset_index(drop=True)
        #print(self.filtered_filings_df)
        
        return self.filtered_filings_df
    
    def get_trailing_data(self):
        # Find the index of the first '10-K'
        first_10k_index = -1
        index_count = 0
        count = 0
        for index, row in self.filtered_filings_df.iterrows():
            if row['form'] == '10-K':
                count = count+1
                first_10k_index = index_count
                if count == 2:
                    first_10k_index = index_count
                    break
            index_count +=1

        if first_10k_index == -1:
            print("No 10-K found")

        else:
            #print(first_10k_index)
            latest_10qs_until_first_10k = self.filtered_filings_df.loc[:first_10k_index - 1][self.filtered_filings_df['form'].isin(['10-Q', '10-K']) ]
            all_10ks_after_first = self.filtered_filings_df.loc[first_10k_index:][self.filtered_filings_df['form'] == '10-K']
            self.trailing_df = pd.concat([latest_10qs_until_first_10k, all_10ks_after_first])
            self.trailing_df.drop_duplicates().sort_values(by='reportDate', ascending=False)
            self.trailing_df = self.trailing_df.reset_index(drop=True)

            return self.trailing_df
        
    def main_execution(self, ticker):
        cik = self.get_cik_matching_ticker(ticker)
        print("cik = ", cik)
        data = self.get_submission_data()
        filtered_filings = self.get_filtered_filings_data()
        trailing_data = self.get_trailing_data()
        
        return trailing_data, cik