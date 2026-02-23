import requests
import time
import logging
import os
import json
from google.cloud import bigquery
from dotenv import load_dotenv
from datetime import datetime, UTC, timezone

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

PROJECT_ID = "noovoleum-project"
DATASET_ID = "noovoleum_data_v2"
TABLE_ID = "raw_api"

TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

API_URL = "https://api.noovoleum.com/api/admin/engineer/getTransaction"
TOKEN = os.getenv("API_TOKEN_TRX")

ROWS_API = 50
BATCH = 1000

HEADERS = {
    "Authorization" : f"Bearer {TOKEN}",
    "Accept" : "application/json"
}

client = bigquery.Client(project=PROJECT_ID)

START_DATE = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
END_DATE   = datetime(2023, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

# def get_last_data():
#     query = """
#         SELECT MAX(ta_start_time) as last_date 
#         FROM `noovoleum-project.noovoleum_data.transaction`
#     """

#     try:
#         query_job = client.query(query)
#         results = query_job.result()
#         for row in results:
#             if row.last_date:
#                 return row.last_date.strftime('%Y-%m-%dT%H:%M:%S')
#     except Exception as e:
#         logging.warning(f"Gagal ambil Max Date: {e}")
    
#     return "2026-02-01T00:00:00"

def extract():
    skip = 0
    ingested = 0
    
    while True:
        params = {
            'rows' : ROWS_API,
            'skip' : skip,
            'searchText' : '',
            'startDate' : START_DATE,
            'endDate' : END_DATE
        }

        try:
            resp = requests.get(
                API_URL,
                headers=HEADERS,
                params=params,
                timeout=(1,60)
            )

            if resp.status_code != 200:
                logging.warning(f"STATUS GAGAL ({resp.status_code}), retry in 2s ...")
                time.sleep(2)
                continue

            data = resp.json()
            result = data.get('result',[])

            if not result:
                logging.info("TIDAK ADA DATA LAGI, EXTRACT SELESAI!!!")
                break

            for row in result:
                yield row

            skip += ROWS_API
            ingested += len(result)

            logging.info(f"INGESTED FROM API: {ingested} rows")
            time.sleep(0.2)

        except Exception as e:
            logging.warning(f"ERROR: {e} | skip: {skip} rows")
            time.sleep(3)
            continue

def run_elt():
    logging.info(f"ELT Start")
    buffer = []
    total = 0

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    for row in extract():
        row.pop('image', None)

        buffer.append({
            "ingested_at" : datetime.now(UTC).isoformat(),
            "payload" : row
        })

        total += 1

        if len(buffer) >= BATCH:
            job = client.load_table_from_json(
                buffer,
                TABLE_REF,
                job_config=job_config
            )

            job.result()

            logging.info(f"BATCH LOAD: {len(buffer)} rows")
            buffer.clear()

    if buffer:
        job = client.load_table_from_json(
                buffer,
                TABLE_REF,
                job_config=job_config
            )
        
        job.result()
        logging.info(f"FINAL LOAD: {len(buffer)} rows")

    logging.info((f"ELT DONE, Total Rows: {total} rows"))

if __name__ == "__main__":
    run_elt()
