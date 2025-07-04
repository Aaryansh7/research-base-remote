import pandas as pd
import os
import sys # Import sys for path manipulation
from datetime import datetime
from flask import Flask, jsonify, request


# Current Ratio
def get_currentratio(df):
    """
    Reads company_data.csv, extracts CurrentAssets and CurrentLiabilities data, calculates basic statistics (Avg, Std Dev),
    and prepares data for a graph.
    """
    print(f"Backend received request for Current Ratio.")

    try:
        print("df = ", df)

        # Find the rows for 'CurrentAssets' and 'CurrentLiabilities'
        currentasset_row = df[df['Accounting Variable'] == 'CurrentAssets']
        currentliability_row = df[df['Accounting Variable'] == 'CurrentLiabilities']

        if currentasset_row.empty or currentliability_row.empty:
            return jsonify({"status": "error", "message": "NetIncome or Revenue data not found in company_data.csv."}), 404

        # Extract numerical values and dates
        # Exclude 'Accounting Variable' column to get only date columns
        currentasset_data_raw = currentasset_row.iloc[0].drop('Accounting Variable')
        currentliability_data_raw = currentliability_row.iloc[0].drop('Accounting Variable')

        currentratio_data = []
        numeric_currentratio = []

        # Iterate through common dates to calculate Current Ratio
        # Ensure that both CurrentAssets and CurrentLiabilities exist for a given date
        all_dates = sorted(list(set(currentasset_data_raw.index) & set(currentliability_data_raw.index)))

        for date_col in all_dates:
            currentasset = pd.to_numeric(currentasset_data_raw.get(date_col, 0.0), errors='coerce')
            currentliability = pd.to_numeric(currentliability_data_raw.get(date_col, 0.0), errors='coerce')

            if pd.isna(currentasset) or pd.isna(currentliability):
                # Skip if values are not numeric
                continue

            # Calculate Current Ratio as (Current Assets / Current Liability) * 100
            # Handle division by zero
            if currentliability != 0:
                currentratio = (currentasset / currentliability) 
                currentratio_data.append({'date': date_col, 'value': currentratio})
                numeric_currentratio.append(currentratio)
            else:
                # Decide how to handle zero current liability (e.g., set margin to 0 or NaN, or skip)
                # For now, we'll append 0 if current liability is 0, indicating no current ratio
                currentratio_data.append({'date': date_col, 'value': 0.0})
                numeric_currentratio.append(0.0)

        # Sort data by date for proper graph display
        currentratio_data.sort(key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d'))

        # Calculate statistics for Current Ratio
        average_margin = pd.Series(numeric_currentratio).mean() if numeric_currentratio else 0
        std_dev_margin = pd.Series(numeric_currentratio).std() if len(numeric_currentratio) > 1 else 0

        # Return the data
        return jsonify({
            "status": "success",
            "data": {
                "metric_name": "Current Ratio",
                "graph_data": currentratio_data,
                "statistics": {
                    "average_margin": round(average_margin, 2),
                    "std_dev_margin": round(std_dev_margin, 2)
                }
            }
        })

    except pd.errors.EmptyDataError:
        return jsonify({"status": "error", "message": "company_data.csv is empty."}), 400
    except Exception as e:
        print(f"Error in get_profitability_netmargin: {e}")
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {str(e)}"}), 500
    


# Cash Ratio
def get_cashratio(df):
    """
    Reads company_data.csv, extracts Cash and CurrentLiabilities data, calculates basic statistics (Avg, Std Dev),
    and prepares data for a graph.
    """
    print(f"Backend received request for Cash Ratio.")

    try:
        print("df = ", df)

        # Find the rows for 'Cash' and 'CurrentLiabilities'
        cash_row = df[df['Accounting Variable'] == 'Cash']
        currentliability_row = df[df['Accounting Variable'] == 'CurrentLiabilities']

        if cash_row.empty or currentliability_row.empty:
            return jsonify({"status": "error", "message": "Cash or Current Liability data not found in company_data.csv."}), 404

        # Extract numerical values and dates
        # Exclude 'Accounting Variable' column to get only date columns
        cash_data_raw = cash_row.iloc[0].drop('Accounting Variable')
        currentliability_data_raw = currentliability_row.iloc[0].drop('Accounting Variable')

        cashratio_data = []
        numeric_cashratio = []

        # Iterate through common dates to calculate Cash Ratio
        # Ensure that both Cash and CurrentLiabilities exist for a given date
        all_dates = sorted(list(set(cash_data_raw.index) & set(currentliability_data_raw.index)))

        for date_col in all_dates:
            cash = pd.to_numeric(cash_data_raw.get(date_col, 0.0), errors='coerce')
            currentliability = pd.to_numeric(currentliability_data_raw.get(date_col, 0.0), errors='coerce')

            if pd.isna(cash) or pd.isna(currentliability):
                # Skip if values are not numeric
                continue

            # Calculate Current Ratio as (Cash / Current Liability) * 100
            # Handle division by zero
            if currentliability != 0:
                cashratio = (cash / currentliability) 
                cashratio_data.append({'date': date_col, 'value': cashratio})
                numeric_cashratio.append(cashratio)
            else:
                # Decide how to handle zero current liability (e.g., set margin to 0 or NaN, or skip)
                # For now, we'll append 0 if current liability is 0, indicating no current ratio
                cashratio_data.append({'date': date_col, 'value': 0.0})
                numeric_cashratio.append(0.0)

        # Sort data by date for proper graph display
        cashratio_data.sort(key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d'))

        # Calculate statistics for Cash Ratio
        average_margin = pd.Series(numeric_cashratio).mean() if numeric_cashratio else 0
        std_dev_margin = pd.Series(numeric_cashratio).std() if len(numeric_cashratio) > 1 else 0

        # Return the data
        return jsonify({
            "status": "success",
            "data": {
                "metric_name": "Cash Ratio",
                "graph_data": cashratio_data,
                "statistics": {
                    "average_margin": round(average_margin, 2),
                    "std_dev_margin": round(std_dev_margin, 2)
                }
            }
        })

    except pd.errors.EmptyDataError:
        return jsonify({"status": "error", "message": "company_data.csv is empty."}), 400
    except Exception as e:
        print(f"Error in get_cashratio: {e}")
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {str(e)}"}), 500