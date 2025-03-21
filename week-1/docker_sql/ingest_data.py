#!/usr/bin/env python
# coding: utf-8

import psycopg2
import os
import argparse
import pandas as pd
from time import time
from sqlalchemy import create_engine


def main(params):
    # Create a connection to the database
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url=params.url

    csv_name = 'output.csv'

    #download th csv
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        t_start = time()
        
        df = next(df_iter)

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk..., took %.3f second' % (t_end - t_start))

    if __name__ == '__main__':
        # Execute when the module is not initialized from an import statement.
        parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

        parser.add_argument('--user', help='user name for postgres')
        parser.add_argument('--password', help='password for postgres')
        parser.add_argument('--host', help='host for postgres')
        parser.add_argument('--port', help='port for postgres')
        parser.add_argument('--db', help='database name for postgres')
        parser.add_argument('--table_name', help='name of the table where we will wrtire de results to')
        parser.add_argument('--url', help='url for the csv file')

        args = parser.parse_args()

        main(args)
