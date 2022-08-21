#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import gzip
import shutil

from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url_taxi_rides = params.url_taxi_rides
    url_zone_lookup = params.url_zone_lookup
    taxi_rides_csv_name = 'output.csv'
    zone_lookup_csv_name = 'lookup.csv'

    os.system(f"wget {url_taxi_rides} -O {taxi_rides_csv_name}.gz")
    with gzip.open(f'{taxi_rides_csv_name}.gz', 'r') as f_in, open(f'{taxi_rides_csv_name}', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(taxi_rides_csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break
    
    # ingest location lookup table
    os.system(f"wget {url_zone_lookup} -O {zone_lookup_csv_name}")
    df_lookup = pd.read_csv(zone_lookup_csv_name)
    df_lookup.to_sql(name='zones', con=engine, if_exists='replace')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url_taxi_rides', required=True, help='url of the csv file containing taxi rides')
    parser.add_argument('--url_zone_lookup', required=True, help='url of the csv file containing the zone lookup table')

    args = parser.parse_args()

    main(args)