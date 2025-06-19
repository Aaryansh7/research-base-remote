import pandas as pd
import os
import sys # Import sys for path manipulation
from datetime import datetime
from flask import Flask, jsonify, request

def get_inventoryturnoverratio(company_data_filepath):
    """
    Reads company_data.csv, extracts Inventory and Revenue data, calculates basic statistics (Avg, Std Dev),
    and prepares data for a graph.
    """
    print(f"Backend received request for Inventory Turnover Ratio.")

    try:
        # Ensure company_data.csv exists before attempting to read
        if not os.path.exists(company_data_filepath):
            return jsonify({"status": "error", "message": "company_data.csv not found. Please select a company first to generate the data."}), 404

        df = pd.read_csv(company_data_filepath)
        print("df = ", df)

        # Find the rows for 'Inventory' and 'Revenue'
        revenue_row = df[df['Accounting Variable'] == 'Revenue']
        inventory_row = df[df['Accounting Variable'] == 'Inventory']

        if revenue_row.empty or inventory_row.empty:
            return jsonify({"status": "error", "message": "Revenue or Inventory data not found in company_data.csv."}), 404

        # Extract numerical values and dates
        # Exclude 'Accounting Variable' column to get only date columns
        revenue_data_raw = revenue_row.iloc[0].drop('Accounting Variable')
        inventory_data_raw = inventory_row.iloc[0].drop('Accounting Variable')

        inventoryturnoverratio_data = []
        numeric_inventoryturnoverratio = []

        # Get all dates common to both Revenue and Inventory, and sort them
        all_dates = sorted(list(set(revenue_data_raw.index) & set(inventory_data_raw.index)))

        # We need at least two inventory data points to calculate an average inventory for the first period.
        # However, for simplicity and to align with the "minimal changes" request,
        # we'll calculate the ratio for each period using the current period's revenue
        # and the average of the current and *previous* period's inventory.
        # This means the first ratio calculation will only be possible from the second date onwards.

        previous_inventory = None
        for i, date_col in enumerate(all_dates):
            current_revenue = pd.to_numeric(revenue_data_raw.get(date_col, 0.0), errors='coerce')
            current_inventory = pd.to_numeric(inventory_data_raw.get(date_col, 0.0), errors='coerce')

            if pd.isna(current_revenue) or pd.isna(current_inventory):
                # Skip if current values are not numeric
                previous_inventory = current_inventory # Update previous even if current is NaN to allow future calculations
                continue

            if i > 0 and previous_inventory is not None:
                # Calculate Average Inventory: (Previous Inventory + Current Inventory) / 2
                average_inventory = (previous_inventory + current_inventory) / 2

                # Calculate Inventory Turnover Ratio as (Revenue / Average Inventory)
                # Handle division by zero for average_asset
                if average_inventory != 0:
                    turnover_ratio = (current_revenue / average_inventory)
                    inventoryturnoverratio_data.append({'date': date_col, 'value': turnover_ratio})
                    numeric_inventoryturnoverratio.append(turnover_ratio)
                else:
                    # Decide how to handle zero average_asset (e.g., set to 0 or NaN, or skip)
                    inventoryturnoverratio_data.append({'date': date_col, 'value': 0.0})
                    numeric_inventoryturnoverratio.append(0.0)
            else:
                # For the first valid data point, we can't calculate average inventory, so skip or handle as appropriate.
                # Here, we'll just store the current inventory as the previous for the next iteration.
                pass

            previous_inventory = current_inventory # Update previous_asset for the next iteration

        # Sort data by date for proper graph display
        inventoryturnoverratio_data.sort(key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d'))

        # Calculate statistics for Inventory Turnover Ratio
        average_ratio = pd.Series(numeric_inventoryturnoverratio).mean() if numeric_inventoryturnoverratio else 0
        std_dev_ratio = pd.Series(numeric_inventoryturnoverratio).std() if len(numeric_inventoryturnoverratio) > 1 else 0

        # Return the data
        return jsonify({
            "status": "success",
            "data": {
                "metric_name": "Inventory Turnover Ratio", # Corrected metric name
                "graph_data": inventoryturnoverratio_data,
                "statistics": {
                    "average_margin": round(average_ratio, 2), # Changed key name for clarity
                    "std_dev_margin": round(std_dev_ratio, 2)   # Changed key name for clarity
                }
            }
        })

    except pd.errors.EmptyDataError:
        return jsonify({"status": "error", "message": "company_data.csv is empty."}), 400
    except Exception as e:
        print(f"Error in get_inventoryturnoverratio: {e}") # Corrected function name in error message
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {str(e)}"}), 500
    


# Asset Turnover
def get_assetturnoverratio(company_data_filepath):
    """
    Reads company_data.csv, extracts Assets and Revenue data, calculates basic statistics (Avg, Std Dev),
    and prepares data for a graph.
    """
    print(f"Backend received request for Asset Turnover Ratio.")

    try:
        # Ensure company_data.csv exists before attempting to read
        if not os.path.exists(company_data_filepath):
            return jsonify({"status": "error", "message": "company_data.csv not found. Please select a company first to generate the data."}), 404

        df = pd.read_csv(company_data_filepath)
        print("df = ", df)

        # Find the rows for 'Asset' and 'Revenue'
        revenue_row = df[df['Accounting Variable'] == 'Revenue']
        asset_row = df[df['Accounting Variable'] == 'TotalAsset']

        if revenue_row.empty or asset_row.empty:
            return jsonify({"status": "error", "message": "Revenue or Asset data not found in company_data.csv."}), 404

        # Extract numerical values and dates
        # Exclude 'Accounting Variable' column to get only date columns
        revenue_data_raw = revenue_row.iloc[0].drop('Accounting Variable')
        asset_data_raw = asset_row.iloc[0].drop('Accounting Variable')

        assetturnoverratio_data = []
        numeric_assetturnoverratio = []

        # Get all dates common to both Revenue and Asset, and sort them
        all_dates = sorted(list(set(revenue_data_raw.index) & set(asset_data_raw.index)))

        # We need at least two inventory data points to calculate an average inventory for the first period.
        # However, for simplicity and to align with the "minimal changes" request,
        # we'll calculate the ratio for each period using the current period's revenue
        # and the average of the current and *previous* period's inventory.
        # This means the first ratio calculation will only be possible from the second date onwards.

        previous_asset = None
        for i, date_col in enumerate(all_dates):
            current_revenue = pd.to_numeric(revenue_data_raw.get(date_col, 0.0), errors='coerce')
            current_asset = pd.to_numeric(asset_data_raw.get(date_col, 0.0), errors='coerce')

            if pd.isna(current_revenue) or pd.isna(current_asset):
                # Skip if current values are not numeric
                previous_asset = current_asset # Update previous even if current is NaN to allow future calculations
                continue

            if i > 0 and previous_asset is not None:
                # Calculate Average Inventory: (Previous Inventory + Current Inventory) / 2
                average_asset = (previous_asset + current_asset) / 2

                # Calculate Inventory Turnover Ratio as (Revenue / Average Inventory)
                # Handle division by zero for average_asset
                if average_asset != 0:
                    turnover_ratio = (current_revenue / average_asset)
                    assetturnoverratio_data.append({'date': date_col, 'value': turnover_ratio})
                    numeric_assetturnoverratio.append(turnover_ratio)
                else:
                    # Decide how to handle zero average_asset (e.g., set to 0 or NaN, or skip)
                    assetturnoverratio_data.append({'date': date_col, 'value': 0.0})
                    numeric_assetturnoverratio.append(0.0)
            else:
                # For the first valid data point, we can't calculate average inventory, so skip or handle as appropriate.
                # Here, we'll just store the current inventory as the previous for the next iteration.
                pass

            previous_asset = current_asset # Update previous_asset for the next iteration

        # Sort data by date for proper graph display
        assetturnoverratio_data.sort(key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d'))

        # Calculate statistics for Inventory Turnover Ratio
        average_ratio = pd.Series(numeric_assetturnoverratio).mean() if numeric_assetturnoverratio else 0
        std_dev_ratio = pd.Series(numeric_assetturnoverratio).std() if len(numeric_assetturnoverratio) > 1 else 0

        # Return the data
        return jsonify({
            "status": "success",
            "data": {
                "metric_name": "Asset Turnover Ratio", # Corrected metric name
                "graph_data": assetturnoverratio_data,
                "statistics": {
                    "average_margin": round(average_ratio, 2), # Changed key name for clarity
                    "std_dev_margin": round(std_dev_ratio, 2)   # Changed key name for clarity
                }
            }
        })

    except pd.errors.EmptyDataError:
        return jsonify({"status": "error", "message": "company_data.csv is empty."}), 400
    except Exception as e:
        print(f"Error in get_assetturnoverratio: {e}") # Corrected function name in error message
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {str(e)}"}), 500