import pandas as pd
import logging
from sqlalchemy import create_engine
from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

PG_CONN = os.getenv("DB_URL")

BQ_PROJECT = "noovoleum-project"
BQ_DATASET = "noovoleum_data"
BQ_TABLE = 'contoh_v6'

CHUNK_SIZE = 100

QUERY = """
SELECT * FROM noovoleum.fact_trx limit 500;
"""

engine = create_engine(PG_CONN)
client = bigquery.Client(project=BQ_PROJECT)

table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    autodetect=True
)

def extract(db_url, query, chunk_size):
    logging.info("EXTRACT START...")
    engine = create_engine(db_url) 

    for df_chunk in pd.read_sql(query, engine, chunksize=CHUNK_SIZE):
        logging.info(f"ingested ({len(df_chunk)}) rows")
        yield df_chunk

def transform(df):
    logging.info("transform data...")

    df.columns = (
        df.columns
        .str.lower()
        .str.replace(".", "_", regex=False)
    )

    for col in df.columns:
        df[col] = df[col].astype("string")
    
    return df

def load(df, write_disposition="WRITE_APPEND"):
    client = bigquery.Client(project=BQ_PROJECT)

    table_id=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True
    )

    load_job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config
    )

    load_job.result()

    logging.info(f"loaded ({len(df)}) rows")


def run_etl():
    logging.info("ETL START...")

    total_rows = 0
    chunk_no = 1

    for df_chunk in extract(PG_CONN, QUERY, CHUNK_SIZE):
        logging.info(f"Process chunk no: {chunk_no}")

        df_transformed = transform(df_chunk)
        load(df_transformed)

        total_rows += len(df_chunk)
        logging.info(f"total rows loaded so far: {total_rows} rows")

        chunk_no += 1

    logging.info(f"ETL JOB FINISH | Total Rows Loaded: {total_rows} rows")

if __name__ == "__main__":
    run_etl()






