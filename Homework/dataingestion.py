#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
import os # To get environment variables if needed, or just for path checks
import psycopg
import tqdm
import click

@click.command()
@click.option("--pg-user", default="postgres", help="Postgres user")
@click.option("--pg-pass", default="postgres", help="Postgres password")
@click.option("--pg-host", default="localhost", help="Postgres host")
@click.option("--pg-port", default=5433, type=int, help="Postgres port")
@click.option("--pg-db", default="ny_taxi", help="Postgres database name")
@click.option("--green-file", default='green_tripdata_2025-11.parquet', help="Green taxi parquet file path")
@click.option("--zones-file", default='taxi_zone_lookup.csv', help="Taxi zone CSV file path")
@click.option("--chunksize", default=20000, type=int, help="Chunk size for ingestion")
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, green_file, zones_file, chunksize):
    # Database connection variables (from CLI)
    DB_HOST = pg_host
    DB_PORT = str(pg_port)
    DB_NAME = pg_db
    DB_USER = pg_user
    DB_PASSWORD = pg_pass

    GREEN_TAXI_PARQUET_FILE = green_file
    TAXI_ZONE_CSV_FILE = zones_file

    GREEN_TAXI_TABLE = 'green_taxi_data'
    ZONE_TABLE = 'zones'

    CHUNKSIZE = chunksize

    # Construct the SQLAlchemy connection string
    DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

    print(f"Attempting to connect to: {DATABASE_URL}")

    # Create a SQLAlchemy engine
    # Using 'postgresql+psycopg' for better compatibility with psycopg3
    engine = create_engine(f'postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    try:
        with engine.connect() as connection:
            print("Connected to PostgreSQL successfully!")
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        print("Please ensure your docker-compose services are running and accessible.")

    print(f"Loading {GREEN_TAXI_PARQUET_FILE} into pandas DataFrame...")
    df_green = pd.read_parquet(GREEN_TAXI_PARQUET_FILE)
    print(f"Loaded {len(df_green)} rows.")

    # Convert datetime columns to datetime objects for proper handling
    df_green['lpep_pickup_datetime'] = pd.to_datetime(df_green['lpep_pickup_datetime'])
    df_green['lpep_dropoff_datetime'] = pd.to_datetime(df_green['lpep_dropoff_datetime'])

    print(f"Ingesting green taxi data into '{GREEN_TAXI_TABLE}' table...")

    # Ingest in chunks
    first_chunk = True
    for i in tqdm.tqdm(range(0, len(df_green), CHUNKSIZE)):
        df_chunk = df_green.iloc[i:i + CHUNKSIZE]
        if first_chunk:
            df_chunk.to_sql(name=GREEN_TAXI_TABLE, con=engine, if_exists='replace', index=False)
            first_chunk = False
        else:
            df_chunk.to_sql(name=GREEN_TAXI_TABLE, con=engine, if_exists='append', index=False)

    print("Green taxi data ingestion complete.")

    print(f"Loading {TAXI_ZONE_CSV_FILE} into pandas DataFrame...")
    df_zones = pd.read_csv(TAXI_ZONE_CSV_FILE)
    print(f"Loaded {len(df_zones)} rows.")

    print(f"Ingesting zone data into '{ZONE_TABLE}' table...")

    # Zone data is small, so we can ingest it in one go.
    df_zones.to_sql(name=ZONE_TABLE, con=engine, if_exists='replace', index=False)

    print("Zone data ingestion complete.")


if __name__ == '__main__':
    run()

