import pandas as pd
import json
import requests
import time
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, UTC
from dotenv import load_dotenv
import os

load_dotenv()




API_URL = 'https://api.noovoleum.com/api/admin/engineer/getTransaction'
DB_URL = os.getenv("DB_URL")
TOKEN = os.getenv("API_TOKEN_TRX")

ROWS_API = 50
BATCH = 500
MAPPING_TGL_TRX = ['ta_start_time', 'ta_end_time', 'createdat', 'updatedat']

HEADERS = {
    'Authorization' : f'Bearer {TOKEN}',
    'Accept' : 'application/json'
}

def normalize_list(x):
    if x is None or x == []:
        return [{}]
    return x

def get_last_date_pg(db_url, table_schema='noovoleum', table_name='fact_trx'):
    engine = create_engine(db_url)
    query = f"""
        SELECT MAX(ta_start_time) AS last_date
        FROM {table_schema}.{table_name}
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()
            if result and result[0]:
                last_date = result[0] + timedelta(minutes=1)
                return last_date.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception as e:
        print(f"GAGAL ambil last_date: {e}")

    return "2026-02-01T00:00:00"  

def extract(url, headers, start_date, end_date):
    skip = 0
    ingested = 0
    while True:
        params = {
            'rows': ROWS_API,
            'skip': skip,
            'searchText': '',
            'startDate': start_date,
            'endDate': end_date
        }

        try:
            response = requests.get(url, headers=headers, params=params, timeout=(3,60))
            if response.status_code != 200:
                print(f"STATUS ({response.status_code}), retry 2s...")
                time.sleep(2)
                continue

            data = response.json()
            result = data.get('result', [])

            if not result:
                print('Tidak Ada Data Lagi, Ingest Selesai')
                break

            yield result
            skip += ROWS_API
            ingested += len(result)
            print(f'INGESTED : {ingested} ROWS')
            time.sleep(0.3)

        except Exception as e:
            print(f"ERROR: {e}, retry 2s...")
            time.sleep(2)
            continue

def transform(data_raw):
    # table trx
    df_trx = pd.json_normalize(data_raw)
    df_trx.columns = df_trx.columns.str.lower().str.replace(" ","_")
    df_trx = df_trx.drop(columns=['extradata_image'], errors='ignore')
    for col in MAPPING_TGL_TRX:
        if col in df_trx.columns:
            df_trx[col] = pd.to_datetime(df_trx[col], errors='coerce')
    df_trx = df_trx.drop(columns=['user','box'], errors='ignore')

    # user table
    user_df = pd.json_normalize(
        data_raw,
        record_path=['user'],
        meta=['_id', 'TA_ID'],
        sep="_",
        errors='ignore'
    )
    user_df.columns = user_df.columns.str.lower()
    for col in ['createdat','updatedat']:
        if col in user_df.columns:
            user_df[col] = pd.to_datetime(user_df[col], errors='coerce')
    if '_id' in user_df.columns and 'ta_id' in user_df.columns:
        user_df = user_df[['_id','ta_id'] + [c for c in user_df.columns if c not in ['_id','ta_id']]]

    # box table
    box_df = pd.json_normalize(
        data_raw,
        record_path=['box'],
        meta=['_id', 'TA_ID'],
        errors='ignore',
        sep="_"
    )
    for col in ['createdat','updatedat','lastheartbeat','lastused']:
        if col in box_df.columns:
            box_df[col] = pd.to_datetime(box_df[col], errors='coerce')
    if '_id' in box_df.columns and 'ta_id' in box_df.columns:
        box_df = box_df[['_id','ta_id'] + [c for c in box_df.columns if c not in ['_id','ta_id']]]

    for df in [df_trx, user_df, box_df]:
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x,(dict,list)) else x)

    return df_trx, user_df, box_df

def load(df, url_db, table_name, table_schema='noovoleum'):
    df.columns = df.columns.str.lower()
    engine = create_engine(url_db)

    def type_map(col):
        if pd.api.types.is_datetime64_any_dtype(col):
            return "TIMESTAMP"
        elif pd.api.types.is_float_dtype(col):
            return "DOUBLE PRECISION"
        elif pd.api.types.is_integer_dtype(col):
            return "BIGINT"
        else:
            return "TEXT"
    
        
    with engine.begin() as conn:
        columns_in_db = pd.read_sql(
            text(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='{table_schema}' AND table_name='{table_name}'
            """), conn)['column_name'].tolist()
        
        for col in df.columns:
            if col not in columns_in_db:
                sql_type = type_map(df[col])
                try:
                    conn.execute(text(f'ALTER TABLE {table_schema}.{table_name} ADD COLUMN "{col}" {sql_type}'))
                    print(f'ADD COLUMN: {col} ({sql_type})')
                except Exception as e:
                    print(f'SKIP COLUMN {col}: {e}')
                columns_in_db.append(col)

    df.to_sql(
    name=table_name,
    schema=table_schema,
    con=engine,
    if_exists='append',
    index=False,
    chunksize=50
    )
        
def main():

    start_date = get_last_date_pg(DB_URL)
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    print(f"ETL START FROM {start_date} until {end_date}")

    buffer = []
    buffer_count = 0
    total_load = {'trx':0,'user':0,'box':0}
    total_ingested = 0

    for page_data in extract(API_URL, HEADERS, start_date, end_date):
        buffer.extend(page_data)
        buffer_count += len(page_data)
        total_ingested += len(page_data)

        if buffer_count >= BATCH:
            df_trx, user_df, box_df = transform(buffer)

            load(df_trx, DB_URL, 'fact_trx')
            load(user_df, DB_URL, 'fact_user')
            load(box_df, DB_URL, 'fact_box')

            total_load['trx'] += len(df_trx)
            total_load['user'] += len(user_df)
            total_load['box'] += len(box_df)
            print(f"DATA KE DB: trx={len(df_trx)}, user={len(user_df)}, box={len(box_df)} | TOTAL: {total_load}")

            buffer.clear()
            buffer_count = 0

    if buffer:
        df_trx, user_df, box_df = transform(buffer)

        load(df_trx, DB_URL, 'fact_trx')
        load(user_df, DB_URL, 'fact_user')
        load(box_df, DB_URL, 'fact_box')

        total_load['trx'] += len(df_trx)
        total_load['user'] += len(user_df)
        total_load['box'] += len(box_df)
        print(f"SISA DATA KE DB: trx={len(df_trx)}, user={len(user_df)}, box={len(box_df)} | TOTAL: {total_load}")

    print("ETL SELESAI!!!")

if __name__ == '__main__':
    main()

    
