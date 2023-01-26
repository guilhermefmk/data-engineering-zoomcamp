#!/usr/bin/env python
# coding: utf-8


import os
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import gzip


# user
# password
# host
# port
# database name
# table name
# url of the csv
def main(params):
    user = params.user
    passwd = params.passwd
    host = params.host
    port = params.port
    db = params.db
    table = params.table_name
    url = params.url
    input_name = 'input.gz' if url[-3:] == '.gz' else 'input.csv'

    os.system(f"wget {url} -O {input_name}")

    engine = create_engine(f'postgresql://{user}:{passwd}@{host}:{port}/{db}')

    input = gzip.open(input_name) if input_name.split('.')[1] == 'gz' else input_name
    
    df_iter = pd.read_csv(input, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    df.head(n=0).to_sql(name = table, con = engine, if_exists='replace')

    df.to_sql(name = table, con = engine, if_exists='append')


    while True:
        t_start = time()
        
        df = next(df_iter)

        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

        df.to_sql(name = table, con = engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f seconds' % (t_end - t_start))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--passwd', help='passwd name for postgres')
    parser.add_argument('--host', help='host name for postgres')
    parser.add_argument('--port', help='port name for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')


    args = parser.parse_args()

    main(args)
