import pandas as pd
import os
import sys # Import sys for path manipulation
from datetime import datetime
from flask import Flask, jsonify, request


# Debt to Equity Ratio
def get_debtequityratio(company_data_filepath):
    """
    Reads company_data.csv, extracts Total Liability and Total Equity(BV) data, calculates basic statistics (Avg, Std Dev),
    and prepares data for a graph.
    """
    print(f"Backend received request for Debt to Equity Ratio.")

    try:
        # Ensure company_data.csv exists before attempting to read
        if not os.path.exists(company_data_filepath):
            return jsonify({"status": "error", "message": "company_data.csv not found. Please select a company first to generate the data."}), 404

        if not os.path.exists(company_data_filepath):
            return jsonify({"status": "error", "message": "company_data.csv not found. Please select a company first to generate the data."}), 404

        df = pd.read_csv(company_data_filepath)
        print("df = ", df)

        # Find the rows for 'TotalLiability' and 'Equity(BV)'
        totalliability_row = df[df['Accounting Variable'] == 'TotalLiability']
        totalasset_row = df[df['Accounting Variable'] == 'Equity(BV)']

        if totalliability_row.empty or totalasset_row.empty:
            return jsonify({"status": "error", "message": "Total Liability or Equity(BV) data not found in company_data.csv."}), 404

        # Extract numerical values and dates
        # Exclude 'Accounting Variable' column to get only date columns
        totalliability_data_raw = totalliability_row.iloc[0].drop('Accounting Variable')
        totalasset_data_raw = totalasset_row.iloc[0].drop('Accounting Variable')

        debtassetratio_data = []
        numeric_debtassetratio = []

        # Iterate through common dates to calculate Debt to Equity Ratio
        # Ensure that both TotalLiability and Equity(BV) exist for a given date
        all_dates = sorted(list(set(totalliability_data_raw.index) & set(totalasset_data_raw.index)))

        for date_col in all_dates:
            debt = pd.to_numeric(totalliability_data_raw.get(date_col, 0.0), errors='coerce')
            asset = pd.to_numeric(totalasset_data_raw.get(date_col, 0.0), errors='coerce')

            if pd.isna(debt) or pd.isna(asset):
                # Skip if values are not numeric
                continue

            # Calculate Debt to Equity Ratio as (Total Current Liability / Shareholder's Equity) 
            # Handle division by zero
            if asset != 0:
                currentratio = (debt / asset) 
                debtassetratio_data.append({'date': date_col, 'value': currentratio})
                numeric_debtassetratio.append(currentratio)
            else:
                # Decide how to handle zero asset (e.g., set margin to 0 or NaN, or skip)
                # For now, we'll append 0 if asset is 0, indicating no debt to asset ratio
                debtassetratio_data.append({'date': date_col, 'value': 0.0})
                numeric_debtassetratio.append(0.0)

        # Sort data by date for proper graph display
        debtassetratio_data.sort(key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d'))

        # Calculate statistics for Debt Equity Ratio
        average_margin = pd.Series(numeric_debtassetratio).mean() if numeric_debtassetratio else 0
        std_dev_margin = pd.Series(numeric_debtassetratio).std() if len(numeric_debtassetratio) > 1 else 0

        # Return the data
        return jsonify({
            "status": "success",
            "data": {
                "metric_name": "Debt Equity Ratio",
                "graph_data": debtassetratio_data,
                "statistics": {
                    "average_margin": round(average_margin, 2),
                    "std_dev_margin": round(std_dev_margin, 2)
                }
            }
        })

    except pd.errors.EmptyDataError:
        return jsonify({"status": "error", "message": "company_data.csv is empty."}), 400
    except Exception as e:
        print(f"Error in get_debtequityratio: {e}")
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {str(e)}"}), 500
    


# Debt to Asset Ratio
def get_debtassetratio(company_data_filepath):
    """
    Reads company_data.csv, extracts Total Liability and Total Assets data, calculates basic statistics (Avg, Std Dev),
    and prepares data for a graph.
    """
    print(f"Backend received request for Debt to Asset Ratio.")

    try:
        # Ensure company_data.csv exists before attempting to read
        if not os.path.exists(company_data_filepath):
            return jsonify({"status": "error", "message": "company_data.csv not found. Please select a company first to generate the data."}), 404

        if not os.path.exists(company_data_filepath):
            return jsonify({"status": "error", "message": "company_data.csv not found. Please select a company first to generate the data."}), 404

        df = pd.read_csv(company_data_filepath)
        print("df = ", df)

        # Find the rows for 'TotalLiability' and 'Equity(BV)'
        totalliability_row = df[df['Accounting Variable'] == 'TotalLiability']
        totalasset_row = df[df['Accounting Variable'] == 'TotalAsset']

        if totalliability_row.empty or totalasset_row.empty:
            return jsonify({"status": "error", "message": "Total Liability or Equity(BV) data not found in company_data.csv."}), 404

        # Extract numerical values and dates
        # Exclude 'Accounting Variable' column to get only date columns
        totalliability_data_raw = totalliability_row.iloc[0].drop('Accounting Variable')
        totalasset_data_raw = totalasset_row.iloc[0].drop('Accounting Variable')

        debtassetratio_data = []
        numeric_debtassetratio = []

        # Iterate through common dates to calculate Debt to Assets Ratio
        # Ensure that both TotalLiability and Assets exist for a given date
        all_dates = sorted(list(set(totalliability_data_raw.index) & set(totalasset_data_raw.index)))

        for date_col in all_dates:
            debt = pd.to_numeric(totalliability_data_raw.get(date_col, 0.0), errors='coerce')
            asset = pd.to_numeric(totalasset_data_raw.get(date_col, 0.0), errors='coerce')

            if pd.isna(debt) or pd.isna(asset):
                # Skip if values are not numeric
                continue

            # Calculate Debt to Asset Ratio as (Total Current Liability / Total Assets) 
            # Handle division by zero
            if asset != 0:
                currentratio = (debt / asset) 
                debtassetratio_data.append({'date': date_col, 'value': currentratio})
                numeric_debtassetratio.append(currentratio)
            else:
                # Decide how to handle zero asset (e.g., set margin to 0 or NaN, or skip)
                # For now, we'll append 0 if asset is 0, indicating no debt to asset ratio
                debtassetratio_data.append({'date': date_col, 'value': 0.0})
                numeric_debtassetratio.append(0.0)

        # Sort data by date for proper graph display
        debtassetratio_data.sort(key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d'))

        # Calculate statistics for Debt Asset Ratio
        average_margin = pd.Series(numeric_debtassetratio).mean() if numeric_debtassetratio else 0
        std_dev_margin = pd.Series(numeric_debtassetratio).std() if len(numeric_debtassetratio) > 1 else 0

        # Return the data
        return jsonify({
            "status": "success",
            "data": {
                "metric_name": "Debt Asset Ratio",
                "graph_data": debtassetratio_data,
                "statistics": {
                    "average_margin": round(average_margin, 2),
                    "std_dev_margin": round(std_dev_margin, 2)
                }
            }
        })

    except pd.errors.EmptyDataError:
        return jsonify({"status": "error", "message": "company_data.csv is empty."}), 400
    except Exception as e:
        print(f"Error in get_debtassetratio: {e}")
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {str(e)}"}), 500