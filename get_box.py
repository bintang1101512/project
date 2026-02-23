import requests
import json
import time
import os
from google.cloud import bigquery
import logging
from datetime import datetime, UTC
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)

PROJECT_ID = "noovoleum-project"
DATASET_ID = "noovoleum_data"
TABLE_ID = "raw_api_box"
TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

API_URL = 'https://api.noovoleum.com/api/admin/engineer/getBoxes'

MAX_RETRY = 5

client = bigquery.Client(project=PROJECT_ID)

def extract(token):
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json'
    }

    logging.info("REQUESTING API...")

    resp = requests.get(API_URL, headers=headers, timeout=(1,60))
    resp.raise_for_status()

    data = resp.json()

    logging.info(f"TOTAL DATA: {len(data)}")

    for row in data:
        yield row

def run(token):
    buffer = []
    total = 0

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    for row in extract(token):
          buffer.append({
               "ingested_at" : datetime.now(UTC).isoformat(),
                "payload": row
          })

    if buffer:
     client.load_table_from_json(
            buffer,
            TABLE_REF,
            job_config=job_config
        ).result()
    logging.info("ELT Finished!!!")
    
    return len(buffer)
     


def main():
    token = os.getenv("API_TOKEN_TRX")
    
    if not token:
        logging.error("Environment Variable API_TOKEN_TRX is missing!")
        return "API_TOKEN_TRX tidak ditemukan!", 500

    try:
        total_ingested = run(token)
        return f"ELT Success: {total_ingested} rows", 200
    except Exception as e:
        logging.error(f"FATAL ERROR: {e}")
        return f"Error: {str(e)}", 500
    
    

if __name__ == '__main__':
    main()    

#tambahan line 86 
             