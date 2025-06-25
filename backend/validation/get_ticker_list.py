import requests
import json

def get_sec_tickers():
    """
    Fetches US company tickers from the SEC EDGAR company_tickers_exchange.json file
    and returns them in a list of dictionaries with 'name' and 'ticker'.
    """
    url = "https://www.sec.gov/files/company_tickers_exchange.json"
    
    # IMPORTANT: Replace with your actual identifying information.
    # The SEC requires a User-Agent header to identify your requests.
    # Failing to do so may result in your requests being blocked.
    # Example: YourAppName/1.0 (YourEmail@example.com)
    headers = {
        "User-Agent": "FinancialDataValidator/1.0 (contact@example.com)" 
    }

    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        
        data = response.json()

        # The JSON structure is:
        # {
        #   "fields": ["cik", "name", "ticker", "exchange"],
        #   "data": [
        #     [cik1, name1, ticker1, exchange1],
        #     [cik2, name2, ticker2, exchange2],
        #     ...
        #   ]
        # }
        
        # We need to extract 'name' (index 1) and 'ticker' (index 2) from each sublist in 'data'.
        
        all_companies_list = []
        for company_info_list in data['data']:
            # Ensure the list has enough elements to avoid IndexError
            if len(company_info_list) >= 3:
                name = company_info_list[1]
                ticker = company_info_list[2]
                
                # Only add if both name and ticker are present and not empty
                if name and ticker:
                    all_companies_list.append({"name": name, "ticker": ticker})
        
        return all_companies_list

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from SEC EDGAR: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []

if __name__ == "__main__":
    us_tickers = get_sec_tickers()

    if us_tickers:
        print(f"Successfully fetched {len(us_tickers)} US company tickers.")
        print("\n--- First 10 Companies ---")
        for i in range(min(10, len(us_tickers))):
            print(us_tickers[i])
        
        print("\n--- Last 5 Companies ---")
        for i in range(max(0, len(us_tickers) - 5), len(us_tickers)):
            print(us_tickers[i])
            
        # You can now use 'us_tickers' just like your 'allCompanies' state variable
        # For example, to find a specific company:
        # apple = next((company for company in us_tickers if company["ticker"] == "AAPL"), None)
        # if apple:
        #     print(f"\nFound Apple: {apple}")
