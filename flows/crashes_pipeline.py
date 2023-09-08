from pathlib import Path
import pandas as pd
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials
import os
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from sodapy import Socrata
from datetime import datetime, timedelta


gcp_credentials_block = GcpCredentials.load("cred")
token = "SxIz2jObj607193nFO4hWzVjk"
PROJECT_ID = "perfect-altar-395516"
BQ_DATASET = "chicago_crashes"
BQ_TABLENAME = "crashes_data"

@task(cache_key_fn=task_input_hash, log_prints = True)
def extract_data() -> pd.DataFrame:
    offset = 0
    limit = 7000
    old_len = 0
    df = pd.read_json(f"https://data.cityofchicago.org/resource/85ca-t3if.json?$$app_token={token}&$limit={limit}&$offset={offset}")

    while len(df) != old_len:
        offset += limit
        old_len = len(df)
        df = pd.concat([df, pd.read_json(f"https://data.cityofchicago.org/resource/85ca-t3if.json?$$app_token={token}&$limit={limit}&$offset={offset}")])

    return df

@task(cache_key_fn=task_input_hash)
def extract_data_daily(date:datetime) -> pd.DataFrame:
    date = date - timedelta(days=1)
    day = date.strftime("%Y-%m-%d")
    client = Socrata("data.cityofchicago.org", token)
    results = client.get("85ca-t3if", where=f"crash_date > '{day}'")
    df = pd.DataFrame.from_records(results)
    client.close()
    return df

@task()
def tranform(df:pd.DataFrame)-> pd.DataFrame:
    df['crash_date'] = pd.to_datetime(df["crash_date"])
    df["date_police_notified"] = pd.to_datetime(df["date_police_notified"])

    return df

@task()
def write_local(df: pd.DataFrame, date:datetime) -> Path:
    """Write DataFrame out locally as parquet file"""
    year = date.strftime("%Y")
    month = date.strftime("%m")
    day = date.strftime("%d")

    path = Path(f"./crashes_raw_data/{year}/{month}")
    if not os.path.exists(path):
        os.makedirs(path)
        
    path = f'./crashes_raw_data/{year}/{month}/{day}.parquet'
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_bucket_block = GcsBucket.load("crashes")

    gcp_bucket_block.upload_from_path(from_path=path, to_path=path, timeout=100000)
    return

@task(log_prints=True)
def remove_file(path: Path):
    if os.path.isfile(path):
        os.remove(path)
    else:
        print(f"Error:{path}file not found")

@task(log_prints = True)
def write_gbq(df):
    """ write dataframe to BigQuery"""
    df.to_gbq(
        destination_table= f"{BQ_DATASET}.{BQ_TABLENAME}",
        project_id = PROJECT_ID,
        credentials =gcp_credentials_block.get_credentials_from_service_account(),
        chunksize =500000,
        if_exists = "append"
    )

  

@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    date = datetime.now()
    df = extract_data()
    df_clean = tranform(df)
    write_gbq(df_clean)
    path = write_local(df_clean, date)
    write_gcs(path)
    remove_file(path)


@flow()
def etl_web_to_gcs_daily() -> None:
    """The main ETL function for daily updating of data"""
    date = datetime.now()
    df = extract_data_daily(date)
    df_clean = tranform(df)
    write_gbq(df_clean)
    path = write_local(df_clean, date)
    write_gcs(path)
    remove_file(path)

if __name__ == "__main__":
    """""
    date = datetime.now()
    cron = date.strftime("%M %H %d %m *")
    first_flow = etl_web_to_gcs.to_deployment(name="first", cron=cron)
    daily_flow = etl_web_to_gcs_daily.to_deployment(name="daily", rrule="FREQ=DAILY;INTERVAL=1")
    serve(first_flow, daily_flow)
    import serve!!!!
    """
    etl_web_to_gcs()
