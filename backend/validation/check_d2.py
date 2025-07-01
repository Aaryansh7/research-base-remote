import requests
import json
import logging
from xbrl.cache import HttpCache
from xbrl.instance import XbrlParser, XbrlInstance
import os
import urllib3

# Suppress InsecureRequestWarning as SEC links often use self-signed certs or are flagged
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Configuration ---
# Your User-Agent is crucial for SEC requests. Replace with your actual info.
USER_AGENT = "YourCustomResearchApp/1.0 (your.email@example.com)"
XBRL_CACHE_DIR = "xbrl_d2_cache" # Directory to store cached XBRL files

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('xbrl').setLevel(logging.DEBUG) # More verbose logging for xbrl library

def extract_facts_from_d2_html(url: str):
    """
    Extracts all facts from a given d2.htm iXBRL URL using py-xbrl.

    Args:
        url (str): The URL to the d2.htm iXBRL document.

    Returns:
        dict: A dictionary containing the extracted facts, or None if an error occurs.
    """
    logging.info(f"Starting fact extraction from: {url}")

    # Set up HTTP cache
    os.makedirs(XBRL_CACHE_DIR, exist_ok=True)
    cache = HttpCache(XBRL_CACHE_DIR, verify_https=False)
    cache.set_headers({'User-Agent': USER_AGENT})

    # Set up XBRL parser
    parser = XbrlParser(cache)

    try:
        # Parse the iXBRL instance
        inst: XbrlInstance = parser.parse_instance(url)
        logging.info(f"Successfully parsed XBRL instance from {url}")

        # Convert the instance to a JSON representation
        xbrl_json_data = inst.json()
        data_dict = json.loads(xbrl_json_data)

        # Log the number of facts found
        num_facts = len(data_dict.get('facts', {}))
        logging.info(f"Extracted {num_facts} facts from the document.")

        return data_dict

    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP Error fetching {url}: {e} (Status Code: {e.response.status_code})")
        logging.error(f"Response content: {e.response.text}")
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON response from XBRL instance for {url}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during XBRL parsing for {url}: {e}")

    return None

if __name__ == "__main__":
    d2_html_link = "https://www.sec.gov/Archives/edgar/data/51143/000005114325000015/ibm-20241231_d2.htm"
    extracted_data = extract_facts_from_d2_html(d2_html_link)

    if extracted_data:
        print("\n--- Extracted Facts (Sample) ---")
        # Print a sample of facts, as the full output can be very large
        facts_sample_count = 5
        printed_count = 0
        for fact_id, fact_content in extracted_data.get('facts', {}).items():
            print(f"Fact ID: {fact_id}")
            print(f"  Concept: {fact_content.get('dimensions', {}).get('concept')}")
            print(f"  Value: {fact_content.get('value')}")
            print(f"  Period: {fact_content.get('dimensions', {}).get('period')}")
            print("-" * 20)
            printed_count += 1
            if printed_count >= facts_sample_count:
                break
        
        print(f"\nTotal facts extracted: {len(extracted_data.get('facts', {}))}")

        # Optionally, save the full extracted JSON to a file
        output_filename = "ibm_20241231_d2_extracted_facts.json"
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(extracted_data, f, indent=4)
        print(f"Full extracted JSON saved to {output_filename}")
    else:
        print("Failed to extract facts.")