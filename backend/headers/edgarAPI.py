import requests
import pandas as pd
from xbrl.cache import HttpCache
from xbrl.instance import XbrlParser, XbrlInstance


class sec_edgar_endpoint:
    def __init__(self):
        self.headers = {"User-Agent": 'picdude001@gmail.com'}

    def get_cik_matching_ticker(self, ticker):
        ticker = ticker.upper().replace(".", "-")
        self.ticker = ticker
        ticker_json = requests.get("https://www.sec.gov/files/company_tickers.json", headers=self.headers, verify=False).json()

        for company in ticker_json.values():
            if company["ticker"] == self.ticker:
                self.cik = str(company["cik_str"]).zfill(10)
                return self.cik
            
        raise ValueError(f"Ticker {self.ticker} not found in SEC database")
    
    def get_submission_data(self):
        url = f"https://data.sec.gov/submissions/CIK{self.cik}.json"
        self.company_submission_data = requests.get(url, headers=self.headers, verify=False).json()
        return self.company_submission_data
    
    def get_filtered_filings_data(self):
        # Returns only rows with form 10-K or 10-Q
        self.only_filings_df = pd.DataFrame(self.company_submission_data['filings']['recent'])
        print(self.only_filings_df)
        self.filtered_filings_df = self.only_filings_df[(self.only_filings_df['form'] == '10-K') |( self.only_filings_df['form'] == '10-Q')]
        self.filtered_filings_df = self.filtered_filings_df[['accessionNumber', 'reportDate', 'form']]
        self.filtered_filings_df = self.filtered_filings_df.reset_index(drop=True)
        print(self.filtered_filings_df)
        
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