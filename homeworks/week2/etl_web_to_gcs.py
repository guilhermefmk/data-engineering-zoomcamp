from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import argparse
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(retries=3,log_prints=True)
def fetch(url: str) -> pd.DataFrame:
    '''Read taxi data from web into pandas DF'''
    df = pd.read_csv(url)
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
def etl_web_to_gcs(year: int, month:int, color: str) -> pd.DataFrame:
    ''' Main ETL funtion '''
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(url)

    path = write_local(df, color, dataset_file)
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