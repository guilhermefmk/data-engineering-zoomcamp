from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import argparse
from prefect.tasks import task_input_hash
from datetime import timedelta
import os

@task(retries=3,log_prints=True)
def fetch(url: str) -> pd.DataFrame:
    '''Read taxi data from web into pandas DF'''
    df = pd.read_csv(url)
    return df

@task(retries=3,log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    '''Read taxi data from web into pandas DF'''
    df["passenger_count"].fillna(0, inplace=True)
    df["passenger_count"] = df["passenger_count"].astype(int)
    
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> str:
    '''Write DataFrame out locally as parquet file'''
    folder_path = 'data_flow'
    dir_path = os.path.join(folder_path, color)
    path = os.path.join(dir_path, dataset_file + '.parquet')


    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    df.to_parquet(path, compression='gzip')
    return path

@task()
def write_gcs(path: Path) -> None:
    '''Upload local parquet file to Google Cloud Storage'''
    path = path.replace("\\","/")
    gcp_block = GcsBucket.load("gcs-prefect")
    gcp_block.upload_from_path(
        from_path=f'{path}',
        to_path=path
    )
    return


@flow()
def etl_web_to_gcs(year: int, month:int, color: str) -> pd.DataFrame:
    ''' Main ETL funtion '''
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(url)
    df_clean = transform(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)
    return df

@flow(log_prints=True)
def etl_parent_flow(months: list[int] = [1,2], year : int = 2021, color: str = "yellow"):
    count_rows = 0
    for month in months:
        df = etl_web_to_gcs(year,month,color)
        count_rows += len(df.index)
    print(count_rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data from web to gcs')

    parser.add_argument('--year', help='year of csv')
    parser.add_argument('--months', nargs="+",help='month of csv')
    parser.add_argument('--color', help='color of dataset')

    args = parser.parse_args()

    etl_parent_flow(args.months,args.year,args.color)