#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click
import psycopg

# We need to Specify Data types


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


@click.command()
@click.option("--pg-user", default="root", help="Postgres user")
@click.option("--pg-pass", default="root", help="Postgres password")
@click.option("--pg-host", default="localhost", help="Postgres host")
@click.option("--pg-port", default=5432, type=int, help="Postgres port")
@click.option("--pg-db", default="ny_taxi", help="Postgres database name")
@click.option("--year", default=2021, type=int, help="Year of the dataset")
@click.option("--month", default=1, type=int, help="Month of the dataset")
@click.option("--target-table", default="yellow_taxi_data", help="Target table name")
@click.option("--chunksize", default=100000, type=int, help="Number of rows per chunk")
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize):
    
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'
    # create database connection
    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator = True,
        chunksize = chunksize,
        )
    # install this library to help us monitor progress of ingestion tqdm
    # iterate over the chunks#
    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.to_sql(
                name=target_table, 
                con=engine, 
                if_exists='replace')
            first = False
        df_chunk.to_sql(
            name=target_table, 
            con=engine, 
            if_exists='append')
        
if __name__ == '__main__':
    run()

    





