from pathlib import Path
import pandas as pd
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials
import os
from prefect import flow, task, serve
from prefect_gcp.cloud_storage import GcsBucket
from sodapy import Socrata
from datetime import datetime, timedelta


gcp_credentials_block = GcpCredentials.load("cred")

PROJECT_ID = "perfect-altar-395516"
BQ_DATASET = "chicago_crashes"
BQ_CRASHES_TABLE = "crashes_data"
BQ_PEOPLE_TABLE = "people_data"

# data key of crashes and people table to use in extraction from the data API
CRASHES_DATA_KEY = "85ca-t3if"
PEOPLE_DATA_KEY = "u6pd-qa9d"
# API token for data requests
TOKEN = "SxIz2jObj607193nFO4hWzVjk"


@task(log_prints = True, cache_key_fn=task_input_hash)
def extract_data(data_key:str) -> pd.DataFrame:
    offset = 0
    limit = 50000
    old_len = 0
    df = pd.read_json(f"https://data.cityofchicago.org/resource/{data_key}.json?$$app_token={TOKEN}&$limit={limit}&$offset={offset}")

    while len(df) != old_len:
        offset += limit
        old_len = len(df)
        df = pd.concat([df, pd.read_json(f"https://data.cityofchicago.org/resource/{data_key}.json?$$app_token={TOKEN}&$limit={limit}&$offset={offset}")])

    return df

@task()
def extract_data_daily(date:datetime, data_key) -> pd.DataFrame:
    # specify the crash dat to extract updated data everyday
    date = date - timedelta(days=1)
    day = date.strftime("%Y-%m-%d")
    client = Socrata("data.cityofchicago.org", TOKEN)
    results = client.get(data_key, where=f"crash_date > '{day}'")
    df = pd.DataFrame.from_records(results)
    client.close()
    df = df.astype(str)
    return df

@task()
def tranform(df:pd.DataFrame)-> pd.DataFrame:
    # trash column in the crashes jason respons
    df.drop(columns=[":@computed_region_rpca_8um6"], inplace=True, axis=1)
    return df

@task(log_prints = True, cache_key_fn=task_input_hash)
def write_gbq(df, table_name):
    """ write dataframe to BigQuery"""
    df.to_gbq(
        destination_table= f"{BQ_DATASET}.{table_name}",
        project_id = PROJECT_ID,
        credentials =gcp_credentials_block.get_credentials_from_service_account(),
        chunksize =500000,
        if_exists = "append"
    )


@task()
def write_local(df: pd.DataFrame, date:datetime, data:str) -> Path:
    """Write DataFrame out locally as parquet file"""
    year = date.strftime("%Y")
    month = date.strftime("%m")
    day = date.strftime("%d")

    path = Path(f"./{data}_raw_data/{year}/{month}")
    if not os.path.exists(path):
        os.makedirs(path)
        
    path = f'./{data}_raw_data/{year}/{month}/{day}.parquet'
    df.to_parquet(path, compression="gzip")
    return path



@task(cache_key_fn=task_input_hash)
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
        print(f"{path} file not found")


  

@flow()
def crashes_web_to_gcs() -> None:
    """The main pipeline function for crashes data"""
    date = datetime.now()
    df = extract_data(CRASHES_DATA_KEY)
    df_clean = tranform(df)
    write_gbq(df_clean, BQ_CRASHES_TABLE)
    path = write_local(df_clean, date, 'crashes')
    write_gcs(path)
    remove_file(path)


@flow()
def crashes_web_to_gcs_daily() -> None:
    """The main pipeline function for daily updating of crashes data"""
    date = datetime.now()
    df = extract_data_daily(date, CRASHES_DATA_KEY)
    df_clean = tranform(df)
    write_gbq(df_clean, BQ_PEOPLE_TABLE)
    path = write_local(df_clean, date, 'crashes')
    write_gcs(path)
    remove_file(path)


@flow()
def people_web_to_gcs() -> None:
    """The main pipeline function for people data"""
    date = datetime.now()
    df = extract_data(PEOPLE_DATA_KEY)
    write_gbq(df)
    path = write_local(df, date, 'crashes')
    write_gcs(path)
    remove_file(path)


@flow()
def people_web_to_gcs_daily() -> None:
    """The main ETL function for daily updating of people data"""
    date = datetime.now()
    df = extract_data_daily(date, PEOPLE_DATA_KEY)
    write_gbq(df)
    path = write_local(df, date, 'crashes')
    write_gcs(path)
    remove_file(path)




if __name__ == "__main__":
    # extract date for daily injection of crash data
    date = datetime.now()
    cron = date.strftime("%M %H %d %m *")

    # first injection of crashes data (happen once)
    crashes_first_flow = crashes_web_to_gcs.to_deployment(name="crashes first", cron=cron)
    # daily injection of  crashes data (happen once daily)
    crashes_daily_flow = crashes_web_to_gcs_daily.to_deployment(name="crashes daily", rrule="FREQ=DAILY;INTERVAL=1")

    # first injection of crashes data (happen once)
    people_first_flow = people_web_to_gcs.to_deployment(name="people first", cron=cron)
    # daily injection of people data (happen once daily)
    people_daily_flow = people_web_to_gcs_daily.to_deployment(name="people daily", rrule="FREQ=DAILY;INTERVAL=1")
  
    serve(crashes_first_flow, crashes_daily_flow, people_first_flow, people_daily_flow)
