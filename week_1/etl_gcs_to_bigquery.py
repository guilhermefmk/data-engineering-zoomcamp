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

@task()
def transform(data: Path) -> pd.DataFrame:
    '''Data cleaning example'''
    df = pd.read_parquet(data)
    total = len(df.index)
    na_passageiros = df['passenger_count'].isna().sum()

    print(f"pre: o total de linhas com passageiros nulos é {na_passageiros}")
    
    df["passenger_count"].fillna(0, inplace=True)
    na_passageiros = df['passenger_count'].isna().sum()
    print(f"pre: após a limpeza o dataframe ficou com {na_passageiros} linhas com valores nulos na coluna passageiros")
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



@flow()
def etl_gcs_to_bq():
    '''Main ETL flow to load data into Big Query(datawarehouse)'''
    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(color, year,month)
    df = transform(path)
    write_bq(df)

if __name__ == "__main__":
    etl_gcs_to_bq()