import requests
import time
import logging
import os
from google.cloud import bigquery
from datetime import datetime, UTC, timedelta
from zoneinfo import ZoneInfo

# Inisialisasi Logger
logging.basicConfig(level=logging.INFO)

# Konfigurasi Global
PROJECT_ID = "noovoleum-project"
DATASET_ID = "noovoleum_data"
TABLE_ID = "raw_api"
TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
API_URL = "https://api.noovoleum.com/api/admin/engineer/getTransaction"

ROWS_API = 50
BATCH = 1000
MAX_RETRY = 5
JAKARTA = ZoneInfo("Asia/Jakarta")

# Client BigQuery (Inisialisasi di luar agar reuse koneksi/warm start)
client = bigquery.Client(project=PROJECT_ID)

def get_last_data():
    # Gunakan f-string atau string biasa, pastikan project ID benar
    query = f"""
        SELECT MAX(DATETIME(ta_start_time, "Asia/Jakarta")) as last_date 
        FROM `{PROJECT_ID}.noovoleum_data.transaction`
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            if row.last_date:
                return row.last_date.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception as e:
        logging.warning(f"Gagal ambil Max Date: {e}")

    return "2026-02-01T00:00:00"

def extract(token):
    skip = 0
    ingested = 0
    retry_count = 0
    jakarta_now = datetime.now(JAKARTA)
    start_date = get_last_data()
    end_date = jakarta_now.strftime("%Y-%m-%dT%H:%M:%S")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    logging.info(f"START EXTRACT FROM {start_date} UNTIL {end_date}")

    while True:
        params = {
            "rows": ROWS_API,
            "skip": skip,
            "startDate": start_date,
            "endDate": end_date
        }

        try:
            resp = requests.get(
                API_URL,
                headers=headers,
                params=params,
                timeout=(5, 60)
            )

            if resp.status_code != 200:
                logging.warning(f"STATUS GAGAL ({resp.status_code}), retry {retry_count+1}...")
                retry_count += 1
                time.sleep(3)
                if retry_count >= MAX_RETRY: break
                continue

            data = resp.json()
            result = data.get("result", [])

            if not result:
                logging.info("TIDAK ADA DATA LAGI")
                break

            for row in result:
                yield row

            skip += ROWS_API
            ingested += len(result)
            retry_count = 0 # Reset retry jika berhasil
            
            if ingested % 500 == 0: # Log setiap 500 baris agar tidak nyampah log
                logging.info(f"FETCHED FROM API: {ingested} rows")
            
            time.sleep(0.1)

        except Exception as e:
            logging.warning(f"ERROR: {e}. Retry {retry_count+1}...")
            retry_count += 1
            time.sleep(3)
            if retry_count >= MAX_RETRY: break

def run_elt(token):
    buffer = []
    total = 0
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    for row in extract(token):
        buffer.append({
            "ingested_at": datetime.now(UTC).isoformat(),
            "payload": row
        })
        total += 1

        if len(buffer) >= BATCH:
            client.load_table_from_json(buffer, TABLE_REF, job_config=job_config).result()
            logging.info(f"BATCH LOAD: {len(buffer)} rows")
            buffer.clear()

    if buffer:
        client.load_table_from_json(buffer, TABLE_REF, job_config=job_config).result()
        logging.info(f"FINAL LOAD: {len(buffer)} rows")

    logging.info(f"ELT DONE. Total rows: {total}")
    return total

def main(request):
    # Ambil token di dalam fungsi main agar selalu fresh
    token = os.environ.get("API_TOKEN_TRX")
    
    if not token:
        logging.error("Environment Variable API_TOKEN_TRX is missing!")
        return "API_TOKEN_TRX tidak ditemukan!", 500

    try:
        total_ingested = run_elt(token)
        return f"ELT Success: {total_ingested} rows", 200
    except Exception as e:
        logging.error(f"FATAL ERROR: {e}")
        return f"Error: {str(e)}", 500