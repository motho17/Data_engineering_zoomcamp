"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
  # Add other columns from the Parquet file that you want to keep and their types
  - name: passenger_count
    type: bigint
  - name: trip_distance
    type: double
  - name: fare_amount
    type: double
  - name: extra
    type: double
  - name: mta_tax
    type: double
  - name: tip_amount
    type: double
  - name: tolls_amount
    type: double
  - name: improvement_surcharge
    type: double
  - name: total_amount
    type: double
  - name: payment_type
    type: bigint # Assuming payment_type_id is an integer
  - name: congestion_surcharge
    type: double
  - name: airport_fee
    type: double
  - name: store_and_fwd_flag
    type: varchar
  - name: ratecode_id
    type: bigint
  - name: pulocation_id # Renamed from PULocationID for consistency
    type: bigint
  - name: dolocation_id # Renamed from DOLocationID for consistency
    type: bigint
  - name: taxi_type # This is a custom column you're adding
    type: varchar
# columns:
#   - name: pickup_datetime
#     type: timestamp
#     description: "When the meter was engaged"
#   - name: dropoff_datetime
#     type: timestamp
#     description: "When the meter was disengaged"
@bruin"""

import os
import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

def materialize():
    start_date_str = os.environ["BRUIN_START_DATE"]
    end_date_str = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    # Convert start and end dates from string to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    all_dataframes = []

    # Generate list of months between start and end dates
    current_date = start_date
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        month_str = f"{month:02d}" # Format month as two digits (e.g., 01, 02)

        for taxi_type in taxi_types:
            # Construct the URL for the Parquet file
            # Example URL: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet
            file_url = (
                f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
                f"{taxi_type}_tripdata_{year}-{month_str}.parquet"
            )

            try:
                print(f"Attempting to read data from: {file_url}")
                # Read the Parquet file directly into a pandas DataFrame
                df = pd.read_parquet(file_url)
                
                # Rename columns to match the metadata
                df = df.rename(columns={
                    'tpep_pickup_datetime': 'pickup_datetime',
                    'tpep_dropoff_datetime': 'dropoff_datetime',
                    'PULocationID': 'pulocation_id',
                    'DOLocationID': 'dolocation_id'
                })
                
                # Add the taxi_type column
                df['taxi_type'] = taxi_type
                
                all_dataframes.append(df)
                print(f"Successfully read {len(df)} rows from {file_url}")
            except Exception as e:
                print(f"Error reading data from {file_url}: {e}")
                # Depending on your requirements, you might want to raise the error
                # or just skip this file. For now, we'll just print an error.

        # Move to the next month
        current_date += relativedelta(months=1)

    if not all_dataframes:
        print("No data was fetched. Returning an empty DataFrame.")
        # Return an empty DataFrame with the expected columns if no data was found
        return pd.DataFrame(columns=["pickup_datetime", "dropoff_datetime"])

    # Concatenate all collected DataFrames into one final DataFrame
    final_dataframe = pd.concat(all_dataframes, ignore_index=True)

    print(f"Final DataFrame has {len(final_dataframe)} rows.")
    return final_dataframe