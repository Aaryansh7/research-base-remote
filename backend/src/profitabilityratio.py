import pandas as pd
import os
import sys # Import sys for path manipulation
from datetime import datetime
from flask import Flask, jsonify, request


# NET MARGIN
def get_netmargin(df):
    """
    Reads company_data.csv, extracts Revenue and NetIncome data, calculates basic statistics (Avg, Std Dev),
    and prepares data for a graph.
    """
    print(f"Backend received request for Net Margin and Revenue data.")

    try:
        print("df = ", df)

        # Find the rows for 'NetIncome' and 'Revenue'
        net_income_row = df[df['Accounting Variable'] == 'NetIncome']
        revenue_row = df[df['Accounting Variable'] == 'Revenue']

        if net_income_row.empty or revenue_row.empty:
            return jsonify({"status": "error", "message": "NetIncome or Revenue data not found in company_data.csv."}), 404

        # Extract numerical values and dates
        # Exclude 'Accounting Variable' column to get only date columns
        net_income_data_raw = net_income_row.iloc[0].drop('Accounting Variable')
        revenue_data_raw = revenue_row.iloc[0].drop('Accounting Variable')

        net_profit_margin_data = []
        numeric_net_profit_margins = []

        # Iterate through common dates to calculate Net Profit Margin
        # Ensure that both NetIncome and Revenue exist for a given date
        all_dates = sorted(list(set(net_income_data_raw.index) & set(revenue_data_raw.index)))

        for date_col in all_dates:
            net_income_value = pd.to_numeric(net_income_data_raw.get(date_col, 0.0), errors='coerce')
            revenue_value = pd.to_numeric(revenue_data_raw.get(date_col, 0.0), errors='coerce')

            if pd.isna(net_income_value) or pd.isna(revenue_value):
                # Skip if values are not numeric
                continue

            # Calculate Net Profit Margin as (Net Income / Revenue) * 100
            # Handle division by zero
            if revenue_value != 0:
                margin = (net_income_value / revenue_value) * 100
                net_profit_margin_data.append({'date': date_col, 'value': margin})
                numeric_net_profit_margins.append(margin)
            else:
                # Decide how to handle zero revenue (e.g., set margin to 0 or NaN, or skip)
                # For now, we'll append 0 if revenue is 0, indicating no profit margin
                net_profit_margin_data.append({'date': date_col, 'value': 0.0})
                numeric_net_profit_margins.append(0.0)

        # Sort data by date for proper graph display
        net_profit_margin_data.sort(key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d'))

        # Calculate statistics for Net Profit Margin
        average_margin = pd.Series(numeric_net_profit_margins).mean() if numeric_net_profit_margins else 0
        std_dev_margin = pd.Series(numeric_net_profit_margins).std() if len(numeric_net_profit_margins) > 1 else 0

        # Return the data
        return jsonify({
            "status": "success",
            "data": {
                "metric_name": "Net Profit Margin",
                "graph_data": net_profit_margin_data,
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
    

# OPERATING MARGIN
def get_operatingmargin(df):
    """
    Reads company_data.csv, extracts Revenue and Operating income data, calculates basic statistics (Avg, Std Dev),
    and prepares data for a graph.
    """
    print(f"Backend received request for Operating Income and Revenue data.")

    try:
        print("df = ", df)

        # Find the rows for 'OperatingIncome' and 'Revenue'
        operating_income_row = df[df['Accounting Variable'] == 'OperatingIncome']
        revenue_row = df[df['Accounting Variable'] == 'Revenue']

        if operating_income_row.empty or revenue_row.empty:
            return jsonify({"status": "error", "message": "OperatingIncome or Revenue data not found in company_data.csv."}), 404

        # Extract numerical values and dates
        # Exclude 'Accounting Variable' column to get only date columns
        operating_income_data_raw = operating_income_row.iloc[0].drop('Accounting Variable')
        revenue_data_raw = revenue_row.iloc[0].drop('Accounting Variable')

        operating_margin_data = []
        numeric_opertaing_margins = []

        # Iterate through common dates to calculate Net Profit Margin
        # Ensure that both NetIncome and Revenue exist for a given date
        all_dates = sorted(list(set(operating_income_data_raw.index) & set(revenue_data_raw.index)))

        for date_col in all_dates:
            operating_income_value = pd.to_numeric(operating_income_data_raw.get(date_col, 0.0), errors='coerce')
            revenue_value = pd.to_numeric(revenue_data_raw.get(date_col, 0.0), errors='coerce')

            if pd.isna(operating_income_value) or pd.isna(revenue_value):
                # Skip if values are not numeric
                continue

            # Calculate Net Profit Margin as (Net Income / Revenue) * 100
            # Handle division by zero
            if revenue_value != 0:
                margin = (operating_income_value / revenue_value) * 100
                operating_margin_data.append({'date': date_col, 'value': margin})
                numeric_opertaing_margins.append(margin)
            else:
                # Decide how to handle zero revenue (e.g., set margin to 0 or NaN, or skip)
                # For now, we'll append 0 if revenue is 0, indicating no profit margin
                operating_margin_data.append({'date': date_col, 'value': 0.0})
                numeric_opertaing_margins.append(0.0)

        # Sort data by date for proper graph display
        operating_margin_data.sort(key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d'))

        # Calculate statistics for Net Profit Margin
        average_margin = pd.Series(numeric_opertaing_margins).mean() if numeric_opertaing_margins else 0
        std_dev_margin = pd.Series(numeric_opertaing_margins).std() if len(numeric_opertaing_margins) > 1 else 0

        # Return the data
        return jsonify({
            "status": "success",
            "data": {
                "metric_name": "Operating Margin",
                "graph_data": operating_margin_data,
                "statistics": {
                    "average_margin": round(average_margin, 2),
                    "std_dev_margin": round(std_dev_margin, 2)
                }
            }
        })

    except pd.errors.EmptyDataError:
        return jsonify({"status": "error", "message": "company_data.csv is empty."}), 400
    except Exception as e:
        print(f"Error in get_profitability_operatingmargin: {e}")
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {str(e)}"}), 500