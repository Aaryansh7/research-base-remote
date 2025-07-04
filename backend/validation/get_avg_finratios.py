import pandas as pd
import os
from io import StringIO
import sys
# Suppress InsecureRequestWarning (if desired, for local testing)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Add the parent directory of 'validation' (RESEARCH-BASE) to the Python path
# Adjust this path as necessary for your project's root based on where this script is located
current_dir = os.path.dirname(os.path.abspath(__file__))
research_base_dir = os.path.join(current_dir, '..', '..') # Assumes this script is 2 levels deep from RESEARCH-BASE
sys.path.append(research_base_dir)

# Import s3_client and read_csv_from_s3 from your structured path
from backend.headers.s3_utils import s3_client, _get_s3_bucket_name, read_csv_from_s3

def calculate_average_net_profit_margin(bucket_name: str = None) -> pd.DataFrame:
    """
    Iterates over company CSVs located in the 'company-csv-data/' folder within an S3 bucket,
    calculates various financial ratios for each company over all available time periods,
    and returns a DataFrame with the results.
    Gross Profit, Operating Income, and Equity(BV) are recalculated under specific conditions if their values are 0 or NaN.

    Args:
        bucket_name (str, optional): The name of the S3 bucket (e.g., 'my-company-archive-bucket').
                                     If not provided, it will try to use the 'S3_BUCKET_NAME'
                                     environment variable (loaded from .env).

    Returns:
        pd.DataFrame: A DataFrame with 'Company Ticker' and various financial ratios
                      (e.g., 'Net Profit Margin', 'Gross Profit Ratio').
                      Returns an empty DataFrame if no CSVs are found or an error occurs.
    """
    try:
        actual_bucket_name = _get_s3_bucket_name(bucket_name)
    except ValueError as e:
        print(f"Error: {e}")
        return pd.DataFrame()

    all_companies_ratios = []
    
    S3_COMPANY_CSV_PREFIX = "company-csv-data/"
    
    print(f"Listing objects in S3 bucket: {actual_bucket_name} under prefix: {S3_COMPANY_CSV_PREFIX}")
    
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=actual_bucket_name, Prefix=S3_COMPANY_CSV_PREFIX)

    found_csvs = False
    for page in pages:
        if 'Contents' in page:
            found_csvs = True
            for obj in page['Contents']:
                full_s3_key_from_list = obj['Key']
                
                if full_s3_key_from_list.endswith('.csv'):
                    company_ticker = os.path.splitext(os.path.basename(full_s3_key_from_list))[0]
                    s3_file_key_to_read = f"{S3_COMPANY_CSV_PREFIX}{company_ticker.lower()}.csv"
                    
                    print(f"Processing S3 file key: s3://{actual_bucket_name}/{s3_file_key_to_read}")

                    try:
                        df = read_csv_from_s3(file_key=s3_file_key_to_read, bucket_name=actual_bucket_name)
                        
                        if 'Accounting Variable' in df.columns:
                            date_columns = [col for col in df.columns if col != 'Accounting Variable']
                            
                            if not date_columns:
                                print(f"Skipping {s3_file_key_to_read}: No date columns found for financial data.")
                                continue
                                
                            df_melted = df.melt(
                                id_vars=['Accounting Variable'],
                                value_vars=date_columns,
                                var_name='reporting_date',
                                value_name='value'
                            )
                            
                            # Include all required metrics
                            required_metrics = [
                                'NetIncome', 'Revenue', 'GrossProfit', 'TotalAsset', 'CostofSales',
                                'OperatingIncome', 'OperatingIncomeAfterInterest', 'Interest',
                                'Equity(BV)', 'Cash', 'CurrentLiabilities', 'Debt(BV)', 'TotalLiability',
                                'ResearchExpense', 'EquityIncludingMinorityInterest', 'MinorityInterest'
                            ]
                            df_filtered = df_melted[
                                df_melted['Accounting Variable'].isin(required_metrics)
                            ].copy()
                            
                            if df_filtered.empty:
                                print(f"Skipping {s3_file_key_to_read}: None of the required metrics ({required_metrics}) found in 'Accounting Variable'.")
                                continue

                            df_pivoted = df_filtered.pivot_table(
                                index='reporting_date',
                                columns='Accounting Variable',
                                values='value',
                                aggfunc='first' 
                            ).reset_index()
                            
                            avg_npm = pd.NA
                            avg_gpr = pd.NA
                            avg_om = pd.NA
                            avg_roa = pd.NA
                            avg_roe = pd.NA
                            avg_cash_ratio = pd.NA
                            avg_dte = pd.NA
                            avg_dta = pd.NA
                            avg_icr = pd.NA
                            avg_rd_sale = pd.NA
                            avg_sale_equity = pd.NA
                            
                            # Convert all relevant columns to numeric first
                            for col in required_metrics:
                                if col in df_pivoted.columns:
                                    df_pivoted[col] = pd.to_numeric(df_pivoted[col], errors='coerce')

                            # --- Conditional Equity(BV) Calculation ---
                            if 'Equity(BV)' in df_pivoted.columns and \
                               'EquityIncludingMinorityInterest' in df_pivoted.columns and \
                               'MinorityInterest' in df_pivoted.columns:
                                
                                condition_equity_bv_zero = (df_pivoted['Equity(BV)'].isna()) | (df_pivoted['Equity(BV)'] == 0)
                                condition_has_equity_mi_and_mi = \
                                    df_pivoted['EquityIncludingMinorityInterest'].notna() & df_pivoted['MinorityInterest'].notna()
                                
                                df_pivoted.loc[condition_equity_bv_zero & condition_has_equity_mi_and_mi, 'Equity(BV)'] = \
                                    df_pivoted['EquityIncludingMinorityInterest'] - df_pivoted['MinorityInterest']
                                print(f"Info: Recalculated Equity(BV) for {company_ticker} where it was 0 or NaN.")
                            else:
                                print(f"Warning: Cannot recalculate Equity(BV) for {company_ticker} due to missing 'Equity(BV)', 'EquityIncludingMinorityInterest', or 'MinorityInterest' columns.")

                            # --- Net Profit Margin Calculation ---
                            npm_cols = ['NetIncome', 'Revenue']
                            if all(col in df_pivoted.columns for col in npm_cols):
                                temp_npm_df = df_pivoted.dropna(subset=npm_cols).copy()
                                temp_npm_df = temp_npm_df[temp_npm_df['Revenue'] != 0]
                                
                                if not temp_npm_df.empty:
                                    temp_npm_df['NetProfitMargin'] = (temp_npm_df['NetIncome'] / temp_npm_df['Revenue'])
                                    avg_npm = temp_npm_df['NetProfitMargin'].mean()
                                else:
                                    print(f"Warning: Insufficient valid data for Net Profit Margin for {company_ticker}.")
                            else:
                                print(f"Warning: Missing 'NetIncome' or 'Revenue' columns for {company_ticker}. Cannot calculate Net Profit Margin.")


                            # --- Gross Profit Ratio Calculation ---
                            gpr_required_base_cols = ['GrossProfit', 'TotalAsset']
                            gpr_recalc_cols = ['Revenue', 'CostofSales']

                            if all(col in df_pivoted.columns for col in gpr_required_base_cols):
                                if all(col in df_pivoted.columns for col in gpr_recalc_cols):
                                    condition_gross_profit_to_recalculate = \
                                        (df_pivoted['GrossProfit'].isna()) | (df_pivoted['GrossProfit'] == 0)
                                    condition_has_revenue_and_costofsales = \
                                        df_pivoted['Revenue'].notna() & df_pivoted['CostofSales'].notna()
                                    
                                    df_pivoted.loc[condition_gross_profit_to_recalculate & condition_has_revenue_and_costofsales, 'GrossProfit'] = \
                                        df_pivoted['Revenue'] - df_pivoted['CostofSales']
                                    print(f"Info: Recalculated GrossProfit for {company_ticker} where it was 0 or NaN.")
                                else:
                                    print(f"Warning: 'Revenue' or 'CostofSales' missing for {company_ticker}. Cannot recalculate GrossProfit for GPR.")
                                
                                temp_gpr_df = df_pivoted.dropna(subset=gpr_required_base_cols).copy()
                                temp_gpr_df = temp_gpr_df[temp_gpr_df['TotalAsset'] != 0]

                                if not temp_gpr_df.empty:
                                    temp_gpr_df['GrossProfitRatio'] = (temp_gpr_df['GrossProfit'] / temp_gpr_df['TotalAsset'])
                                    avg_gpr = temp_gpr_df['GrossProfitRatio'].mean()
                                else:
                                    print(f"Warning: Insufficient valid data for Gross Profit Ratio for {company_ticker}.")
                            else:
                                print(f"Warning: Missing 'GrossProfit' or 'TotalAsset' columns for {company_ticker}. Cannot calculate Gross Profit Ratio.")
                            
                            # --- Operating Margin Calculation ---
                            om_required_base_cols = ['OperatingIncome', 'Revenue']
                            om_recalc_cols = ['OperatingIncomeAfterInterest', 'Interest']

                            if all(col in df_pivoted.columns for col in om_required_base_cols):
                                if all(col in df_pivoted.columns for col in om_recalc_cols):
                                    condition_op_income_to_recalculate = \
                                        (df_pivoted['OperatingIncome'].isna()) | (df_pivoted['OperatingIncome'] == 0)
                                    condition_has_op_income_after_interest_and_interest = \
                                        df_pivoted['OperatingIncomeAfterInterest'].notna() & df_pivoted['Interest'].notna()

                                    df_pivoted.loc[condition_op_income_to_recalculate & condition_has_op_income_after_interest_and_interest, 'OperatingIncome'] = \
                                        df_pivoted['OperatingIncomeAfterInterest'] + df_pivoted['Interest']
                                    print(f"Info: Recalculated OperatingIncome for {company_ticker} where it was 0 or NaN.")
                                else:
                                    print(f"Warning: 'OperatingIncomeAfterInterest' or 'Interest' missing for {company_ticker}. Cannot recalculate OperatingIncome for OM.")

                                temp_om_df = df_pivoted.dropna(subset=om_required_base_cols).copy()
                                temp_om_df = temp_om_df[temp_om_df['Revenue'] != 0]

                                if not temp_om_df.empty:
                                    temp_om_df['OperatingMargin'] = (temp_om_df['OperatingIncome'] / temp_om_df['Revenue'])
                                    avg_om = temp_om_df['OperatingMargin'].mean()
                                else:
                                    print(f"Warning: Insufficient valid data for Operating Margin for {company_ticker}.")
                            else:
                                print(f"Warning: Missing 'OperatingIncome' or 'Revenue' columns for {company_ticker}. Cannot calculate Operating Margin.")

                            # --- Return on Assets (ROA) Calculation ---
                            roa_cols = ['NetIncome', 'TotalAsset']
                            if all(col in df_pivoted.columns for col in roa_cols):
                                temp_roa_df = df_pivoted.dropna(subset=roa_cols).copy()
                                temp_roa_df = temp_roa_df[temp_roa_df['TotalAsset'] != 0]

                                if not temp_roa_df.empty:
                                    temp_roa_df['ROA'] = (temp_roa_df['NetIncome'] / temp_roa_df['TotalAsset'])
                                    avg_roa = temp_roa_df['ROA'].mean()
                                else:
                                    print(f"Warning: Insufficient valid data for ROA for {company_ticker}.")
                            else:
                                print(f"Warning: Missing 'NetIncome' or 'TotalAsset' columns for {company_ticker}. Cannot calculate ROA.")

                            # --- Return on Equity (ROE) Calculation ---
                            roe_cols = ['NetIncome', 'Equity(BV)']
                            if all(col in df_pivoted.columns for col in roe_cols):
                                temp_roe_df = df_pivoted.dropna(subset=roe_cols).copy()
                                temp_roe_df = temp_roe_df[temp_roe_df['Equity(BV)'] != 0]

                                if not temp_roe_df.empty:
                                    temp_roe_df['ROE'] = (temp_roe_df['NetIncome'] / temp_roe_df['Equity(BV)'])
                                    avg_roe = temp_roe_df['ROE'].mean()
                                else:
                                    print(f"Warning: Insufficient valid data for ROE for {company_ticker}.")
                            else:
                                print(f"Warning: Missing 'NetIncome' or 'Equity(BV)' columns for {company_ticker}. Cannot calculate ROE.")

                            # --- Cash Ratio Calculation ---
                            cash_ratio_cols = ['Cash', 'CurrentLiabilities']
                            if all(col in df_pivoted.columns for col in cash_ratio_cols):
                                temp_cash_ratio_df = df_pivoted.dropna(subset=cash_ratio_cols).copy()
                                temp_cash_ratio_df = temp_cash_ratio_df[temp_cash_ratio_df['CurrentLiabilities'] != 0]

                                if not temp_cash_ratio_df.empty:
                                    temp_cash_ratio_df['CashRatio'] = (temp_cash_ratio_df['Cash'] / temp_cash_ratio_df['CurrentLiabilities'])
                                    avg_cash_ratio = temp_cash_ratio_df['CashRatio'].mean()
                                else:
                                    print(f"Warning: Insufficient valid data for Cash Ratio for {company_ticker}.")
                            else:
                                print(f"Warning: Missing 'Cash' or 'CurrentLiabilities' columns for {company_ticker}. Cannot calculate Cash Ratio.")
                            
                            # --- Debt-to-Equity Ratio Calculation ---
                            dte_cols = ['Debt(BV)', 'Equity(BV)']
                            if all(col in df_pivoted.columns for col in dte_cols):
                                temp_dte_df = df_pivoted.dropna(subset=dte_cols).copy()
                                temp_dte_df = temp_dte_df[temp_dte_df['Equity(BV)'] != 0]

                                if not temp_dte_df.empty:
                                    temp_dte_df['DebtToEquityRatio'] = (temp_dte_df['Debt(BV)'] / temp_dte_df['Equity(BV)'])
                                    avg_dte = temp_dte_df['DebtToEquityRatio'].mean()
                                else:
                                    print(f"Warning: Insufficient valid data for Debt-to-Equity Ratio for {company_ticker}.")
                            else:
                                print(f"Warning: Missing 'Debt(BV)' or 'Equity(BV)' columns for {company_ticker}. Cannot calculate Debt-to-Equity Ratio.")

                            # --- Debt-to-Asset Ratio Calculation ---
                            dta_cols = ['TotalLiability', 'Equity(BV)', 'TotalAsset']
                            if all(col in df_pivoted.columns for col in dta_cols):
                                temp_dta_df = df_pivoted.dropna(subset=dta_cols).copy()
                                temp_dta_df = temp_dta_df[temp_dta_df['TotalAsset'] != 0]

                                if not temp_dta_df.empty:
                                    temp_dta_df['DebtToAssetRatio'] = (temp_dta_df['TotalLiability'] - temp_dta_df['Equity(BV)']) / temp_dta_df['TotalAsset']
                                    avg_dta = temp_dta_df['DebtToAssetRatio'].mean()
                                else:
                                    print(f"Warning: Insufficient valid data for Debt-to-Asset Ratio for {company_ticker}.")
                            else:
                                print(f"Warning: Missing 'TotalLiability', 'Equity(BV)', or 'TotalAsset' columns for {company_ticker}. Cannot calculate Debt-to-Asset Ratio.")

                            # --- Interest Coverage Ratio Calculation ---
                            icr_cols = ['OperatingIncome', 'Interest']
                            if all(col in df_pivoted.columns for col in icr_cols):
                                temp_icr_df = df_pivoted.dropna(subset=icr_cols).copy()
                                temp_icr_df = temp_icr_df[temp_icr_df['Interest'] != 0]

                                if not temp_icr_df.empty:
                                    temp_icr_df['InterestCoverageRatio'] = (temp_icr_df['OperatingIncome'] / temp_icr_df['Interest'])
                                    avg_icr = temp_icr_df['InterestCoverageRatio'].mean()
                                else:
                                    print(f"Warning: Insufficient valid data for Interest Coverage Ratio for {company_ticker}.")
                            else:
                                print(f"Warning: Missing 'OperatingIncome' or 'Interest' columns for {company_ticker}. Cannot calculate Interest Coverage Ratio.")
                            
                            # --- RD_SALE Calculation ---
                            rd_sale_cols = ['ResearchExpense', 'Revenue']
                            if all(col in df_pivoted.columns for col in rd_sale_cols):
                                temp_rd_sale_df = df_pivoted.dropna(subset=rd_sale_cols).copy()
                                temp_rd_sale_df = temp_rd_sale_df[temp_rd_sale_df['Revenue'] != 0]

                                if not temp_rd_sale_df.empty:
                                    temp_rd_sale_df['RD_SALE'] = (temp_rd_sale_df['ResearchExpense'] / temp_rd_sale_df['Revenue'])
                                    avg_rd_sale = temp_rd_sale_df['RD_SALE'].mean()
                                else:
                                    print(f"Warning: Insufficient valid data for RD_SALE for {company_ticker}.")
                            else:
                                print(f"Warning: Missing 'ResearchExpense' or 'Revenue' columns for {company_ticker}. Cannot calculate RD_SALE.")

                            # --- SALE_EQUITY Calculation ---
                            sale_equity_cols = ['Revenue', 'Equity(BV)']
                            if all(col in df_pivoted.columns for col in sale_equity_cols):
                                temp_sale_equity_df = df_pivoted.dropna(subset=sale_equity_cols).copy()
                                temp_sale_equity_df = temp_sale_equity_df[temp_sale_equity_df['Equity(BV)'] != 0]

                                if not temp_sale_equity_df.empty:
                                    temp_sale_equity_df['SALE_EQUITY'] = (temp_sale_equity_df['Revenue'] / temp_sale_equity_df['Equity(BV)'])
                                    avg_sale_equity = temp_sale_equity_df['SALE_EQUITY'].mean()
                                else:
                                    print(f"Warning: Insufficient valid data for SALE_EQUITY for {company_ticker}.")
                            else:
                                print(f"Warning: Missing 'Revenue' or 'Equity(BV)' columns for {company_ticker}. Cannot calculate SALE_EQUITY.")

                            # Add computed averages to the list
                            if pd.notna(avg_npm) or pd.notna(avg_gpr) or pd.notna(avg_om) or \
                               pd.notna(avg_roa) or pd.notna(avg_roe) or pd.notna(avg_cash_ratio) or \
                               pd.notna(avg_dte) or pd.notna(avg_dta) or pd.notna(avg_icr) or \
                               pd.notna(avg_rd_sale) or pd.notna(avg_sale_equity):
                                all_companies_ratios.append({
                                    'Company Ticker': company_ticker.upper(),
                                    'Net Profit Margin': avg_npm,
                                    'Gross Profit Ratio': avg_gpr,
                                    'Operating Margin': avg_om,
                                    'ROA': avg_roa,
                                    'ROE': avg_roe,
                                    'Cash Ratio': avg_cash_ratio,
                                    'Debt-to-Equity Ratio': avg_dte,
                                    'Debt-to-Asset Ratio': avg_dta,
                                    'Interest Coverage Ratio': avg_icr,
                                    'RD_SALE': avg_rd_sale,
                                    'SALE_EQUITY': avg_sale_equity
                                })
                            else:
                                print(f"No valid ratios calculated for {company_ticker}. Skipping entry.")
                            # --- MODIFICATION END ---
                        else:
                            print(f"Skipping {s3_file_key_to_read}: Missing 'Accounting Variable' column.")

                    except Exception as e:
                        print(f"Error processing {s3_file_key_to_read}: {e}")
    
    if not found_csvs:
        print(f"No CSV objects found under prefix '{S3_COMPANY_CSV_PREFIX}' in bucket: {actual_bucket_name}")
        # Update the columns for empty DataFrame return
        return pd.DataFrame(columns=[
            'Company Ticker', 
            'Net Profit Margin', 
            'Gross Profit Ratio', 
            'Operating Margin', 
            'ROA', 
            'ROE', 
            'Cash Ratio', 
            'Debt-to-Equity Ratio',
            'Debt-to-Asset Ratio',
            'Interest Coverage Ratio',
            'RD_SALE',
            'SALE_EQUITY'
        ])

    if all_companies_ratios:
        result_df = pd.DataFrame(all_companies_ratios)
        #result_df = result_df.sort_values(by='Net Profit Margin', ascending=False).reset_index(drop=True)
        return result_df
    else:
        print("No financial ratios calculated for any company.")
        # Update the columns for empty DataFrame return
        return pd.DataFrame(columns=[
            'Company Ticker', 
            'Net Profit Margin', 
            'Gross Profit Ratio', 
            'Operating Margin', 
            'ROA', 
            'ROE', 
            'Cash Ratio', 
            'Debt-to-Equity Ratio',
            'Debt-to-Asset Ratio',
            'Interest Coverage Ratio',
            'RD_SALE',
            'SALE_EQUITY'
        ])

if __name__ == "__main__":
    # --- IMPORTANT ---
    # Ensure you have a .env file in your project's root directory with:
    # S3_BUCKET_NAME=your-actual-s3-bucket-name
    # AWS_REGION=your-aws-region

    # --- Display all rows (User requested option) ---
    pd.set_option('display.max_rows', None)

    average_ratios_df = calculate_average_net_profit_margin()

    if not average_ratios_df.empty:
        print("\nFinancial Ratios for Companies:")
        print(average_ratios_df)
        average_ratios_df.to_csv('average_ratios.csv')
        print("Saved as CSV..")
    else:
        print("\nCould not calculate financial ratios for any company.")