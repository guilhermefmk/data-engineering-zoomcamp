from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task()
def extract_from_gcs(color: str,year: int,month: int) -> Path:
    '''Download trip data from GCS'''
    gcs_path = f"data_flow/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    local_path = Path("./extract_from_gcs/")
    local_path.mkdir(parents=True, exist_ok=True)
    gcp_block = GcsBucket.load("gcs-prefect")
    gcp_block.get_directory(from_path=gcs_path, local_path=local_path)

    return Path(f"{local_path}/{gcs_path}")

@task(log_prints=True)
def fetch(data: Path) -> pd.DataFrame:
    '''Data cleaning example'''
    df = pd.read_parquet(data)
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    '''Write DataFrame to BigQuery'''
    gcp_block = GcpCredentials.load("gcp-zoom")
    df.to_gbq(
        destination_table= "dezoomcamp.zoomcamp",
        project_id= "beaming-figure-375814",
        credentials= gcp_block.get_credentials_from_service_account(),
        chunksize= 500_000,
        if_exists="append"
    )



@flow(log_prints=True)
def etl_gcs_to_bq(year,month,color) -> pd.DataFrame:
    '''Main ETL flow to load data into Big Query(datawarehouse)'''
    path = extract_from_gcs(color, year,month)
    df = fetch(path)
    write_bq(df)
    return df

@flow()
def etl_sub_flow(months: list[int] = [2,3], year : int = 2019, color: str = 'yellow'):
    count_rows = 0
    for month in months:
        etl_gcs_to_bq(year,month,color)
        count_rows += len(df.index)
    print(count_rows)

if __name__ == "__main__":
    color = "yellow"
    months = [1, 2, 3]
    year = 2021
    etl_sub_flow(months,year,color)