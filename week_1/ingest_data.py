#!/usr/bin/env python
# coding: utf-8


import os
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import gzip
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector
# user
# password
# host
# port
# database name
# table name
# url of the csv
@task(log_prints=True, retries = 3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    input_name = 'input.gz' if url[-3:] == '.gz' else 'input.csv'
    print(input_name)
    os.system(f"wget {url} -O {input_name}")

    input = gzip.open(input_name) if input_name.split('.')[1] == 'gz' else input_name
    

    df = pd.read_csv(input)

    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    return df

@task(log_prints=True)
def transform_data(df):
    total = len(df.index)
    zero_passageiros = df['passenger_count'].isin([0]).sum()

    print(f"pre: o total de linhas é {total}")
    print(f"pre: o total de linhas com 0 passageiros é {zero_passageiros}")
    
    df = df[df['passenger_count'] != 0]

    total = len(df.index)
    print(f"pre: após a limpeza o dataframe ficou com {total} linhas")

    return df



@task(log_prints=True, retries=3)
def ingest_data(params, df):
    table = params.table_name

    df_iter = [df[i:i+100000] for i in range(0,df.shape[0],100000)]
    
    connection_block = SqlAlchemyConnector.load("postgres-connector")

    with connection_block.get_connection(begin=False) as engine:

        df.head(n=0).to_sql(name = table, con = engine, if_exists='replace')

        df.to_sql(name = table, con = engine, if_exists='append')

    while True:

        try:
            t_start = time()
            
            df = next(df_iter)

            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

            df.to_sql(name = table, con = engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f seconds' % (t_end - t_start))
        except:
            print("Ingest finished!")
            break


@flow(name="Ingest Flow")
def main():
    parser = argparse.ArgumentParser(description='Ingest CSV data to postgres')

    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    raw_data = extract_data(args.url)
    data = transform_data(raw_data)
    ingest_data(args, data)

if __name__ == '__main__':
    main()
