from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import argparse
import os

@task(retries=3)
def fetch(url: str) -> pd.DataFrame:
    '''Read taxi data from web into pandas DF'''

    df = pd.read_csv(url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    '''Fix dtype issues'''

    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    '''Write DataFrame out locally as parquet file'''
    dir = Path(f'data_flow/{color}/')
    path = Path(f'{dir}/{dataset_file}.parquet')

    dir.mkdir(parents=True, exist_ok=True)

    df.to_parquet(path, compression='gzip')
    return path

@task()
def write_gcs(path: Path) -> None:
    '''Upload local parquet file to Google Cloud Storage'''
    gcp_block = GcsBucket.load("gcs-prefect")
    gcp_block.upload_from_path(
        from_path=f'{path}',
        to_path=path
    )
    return


@flow()
def etl_web_to_gcs() -> None:
    ''' Main ETL funtion '''
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(url)

    df_clean = clean(df)

    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

if __name__ == '__main__':
    etl_web_to_gcs()