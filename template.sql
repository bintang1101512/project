ALTER TABLE noovoleum.fact_trx
DROP COLUMN IF EXISTS ta_start_date,
DROP COLUMN IF EXISTS ta_start_time_only,
DROP COLUMN IF EXISTS ta_start_hour,
DROP COLUMN IF EXISTS ta_start_hour_str,
DROP COLUMN IF EXISTS ta_end_date,
DROP COLUMN IF EXISTS ta_end_time_only,
DROP COLUMN IF EXISTS duration_minutes,
ADD COLUMN ta_start_date DATE
    GENERATED ALWAYS AS (ta_start_time::date) STORED,
ADD COLUMN ta_start_time_only TIME
    GENERATED ALWAYS AS (ta_start_time::time) STORED,
ADD COLUMN ta_start_hour INTEGER
    GENERATED ALWAYS AS (EXTRACT(HOUR FROM ta_start_time)::int) STORED,
ADD COLUMN ta_start_hour_str TEXT
    GENERATED ALWAYS AS (
        LPAD(EXTRACT(HOUR FROM ta_start_time)::text, 2, '0') || ':00'
    ) STORED,
ADD COLUMN ta_end_date DATE
    GENERATED ALWAYS AS (ta_end_time::date) STORED,
ADD COLUMN ta_end_time_only TIME
    GENERATED ALWAYS AS (ta_end_time::time) STORED,
ADD COLUMN duration_minutes NUMERIC(10,2)
    GENERATED ALWAYS AS (
        EXTRACT(EPOCH FROM (ta_end_time - ta_start_time)) / 60
    ) STORED;


CREATE MATERIALIZED VIEW noovoleum.mv_fact_trx AS
SELECT
    t._id,
    t.boxid,
    t.ta_user_id,
    t.ta_id,
    t.ta_uco_volume,
    t.ta_slops_volume,
    t.ta_uco_weight,
    t.ta_slops_weight,
    t.uco_approved,
    t.amount,
    t.bonus,
    t.totalamount,
    t.method,
    t.ta_start_date,
    t.ta_start_time_only,
    t.ta_start_hour,
    t.ta_start_hour_str,
    t.ta_end_date,
    t.ta_end_time_only,
    t.duration_minutes,
    b."name"        AS box_name,
    b.internal_id,
    b.city          AS box_city,
    b.region,
    b."group",
    u."name"        AS user_full_name,
    u.username,
    u.city          AS user_city,
    u.postcode,
    u.phone_number,
    u.referral_code
FROM noovoleum.fact_trx t
JOIN noovoleum.fact_box b
    ON t._id = b._id
JOIN noovoleum.fact_user u
    ON t._id = u._id;

REFRESH MATERIALIZED VIEW CONCURRENTLY noovoleum.mv_fact_trx;

create materialized view noovoleum.mv_stg_trx as
select
	payload ->> '_id' as _id,
	payload ->> 'boxId' as box_id,
	(payload ->> 'TA_Start_Time')::timestamptz as ta_start_time,
	(payload ->> 'TA_End_Time')::timestamptz as ta_end_time,
	payload ->> 'TA_USER_ID' as ta_user_id,
	payload ->> 'TA_ID' as ta_id,
	(payload ->> 'TA_UCO_Volume')::numeric as ta_uco_volume,
	(payload ->> 'TA_Slops_Volume')::numeric as ta_slops_volume,
	(payload ->> 'TA_UCO_Weight')::numeric as ta_uco_weight,
	(payload ->> 'TA_Slops_Weight')::numeric as ta_slops_weight,
	payload ->> 'UCO_Approved' as uco_approved,
	(payload ->> 'amount')::numeric as amount,
	payload -> 'address' ->> 'city' as city,
	payload -> 'address' ->> 'postcode',
	(payload ->> 'bonus')::numeric as bonus,
	(payload ->> 'totalamount')::numeric as total_amount,
	payload ->> 'mothod' as "mothod",
	(payload -> 'extraData' ->> 'alcohol_level')::numeric as alcohol_level,
	(payload -> 'extraData' ->> 'density')::numeric as density,
	payload -> 'extraData' ->> 'detail_transaction' as detail_transaction,
	(payload ->> 'fare')::numeric as fare,
	(payload ->> 'uco_price_per_liter')::numeric as uco_price_per_liter,
	payload -> 'slug' ->> 'box_brand_partner' as box_brand_partner,
	payload ->> 'channel' as channel
from noovoleum.raw_api_data
order by ta_start_time desc

CREATE TABLE trx_user AS
SELECT
    -- META
    payload ->> '_id'   AS trx_id,
    payload ->> 'TA_ID' AS ta_id,

    -- USER FIELDS
    user_elem ->> 'name'      AS name,
    user_elem ->> 'username'  AS username,
    user_elem ->> 'email'     AS email,
    user_elem ->> 'user_type' AS user_type,
    user_elem ->> 'address'   AS address,
    user_elem ->> 'city'      AS city,
    user_elem ->> 'postcode'  AS postcode,
    user_elem ->> 'country'   AS country,
    user_elem ->> 'currency'  AS currency,

    -- phone nested object
    user_elem -> 'phone' ->> 'number' AS phone_number,
    user_elem -> 'phone' ->> 'prefix' AS phone_prefix
FROM raw_transactions
CROSS JOIN LATERAL jsonb_array_elements(payload -> 'user') AS user_elem;

DELETE FROM noovoleum.fact_trx
WHERE ctid IN (
    SELECT ctid
    FROM (
        SELECT 
            ctid,
            ROW_NUMBER() OVER (
                PARTITION BY ta_id
                ORDER BY ta_start_time DESC
            ) AS rn
        FROM noovoleum.fact_trx
    ) t
    WHERE rn > 1
);


create table noovoleum.stg_table_box as
select
	payload ->> 'id' as box_id,
	payload ->> 'name' as box_name,
	payload ->> 'model' as box_model,
	payload ->> 'status' as box_status,
	payload ->> 'lUCOHold' as uco_hold,
	payload ->> 'lSlopsHold' as slops_hold,
	(payload ->> 'createdAt')::timestamptz as created_at,
	(payload ->> 'updatedAt')::timestamptz as updated_at,
	payload ->> 'address' as box_address,
	(payload ->> 'dashCollectionPoint')::bool as dash_colletion,
	(payload ->> 'isOpen')::bool as is_open,
	payload ->> 'group' as box_group,
	payload ->> 'group_name' as box_group_name,
	(payload ->> 'lastUsed')::timestamptz as last_used,
	(payload ->> 'hidden')::bool as hidden_status,
	(payload ->> 'leakage')::bool as leakage_status,
	payload ->> 'city' as box_city,
	payload ->> 'region' as box_region,
	payload ->> 'box_state' as box_state_status,
	payload -> 'box_error_message' -> 0 ->> 'message' as box_message
from noovoleum.get_box

MERGE INTO noovoleum.stg_table_box AS t
USING (
    SELECT
        payload ->> 'id' AS box_id,
        payload ->> 'name' AS box_name,
        payload ->> 'model' AS box_model,
        payload ->> 'status' AS box_status,
        (payload ->> 'lUCOHold')::double precision AS uco_hold,
        (payload ->> 'lSlopsHold')::double precision AS slops_hold,
        (payload ->> 'createdAt')::timestamptz AS created_at,
        (payload ->> 'updatedAt')::timestamptz AS updated_at,
        payload ->> 'address' AS box_address,
        (payload ->> 'dashCollectionPoint')::bool AS dash_colletion,
        (payload ->> 'isOpen')::bool AS is_open,
        payload ->> 'group' AS box_group,
        payload ->> 'group_name' AS box_group_name,
        (payload ->> 'lastUsed')::timestamptz AS last_used,
        (payload ->> 'hidden')::bool AS hidden_status,
        (payload ->> 'leakage')::bool AS leakage_status,
        payload ->> 'city' AS box_city,
        payload ->> 'region' AS box_region,
        payload ->> 'box_state' AS box_state_status,
        payload -> 'box_error_message' -> 0 ->> 'message' AS box_message
    FROM noovoleum.get_box
) AS s
ON t.box_id = s.box_id

WHEN MATCHED THEN
UPDATE SET
    box_name = s.box_name,
    box_model = s.box_model,
    box_status = s.box_status,
    uco_hold = s.uco_hold,
    slops_hold = s.slops_hold,
    created_at = s.created_at,
    updated_at = s.updated_at,
    box_address = s.box_address,
    dash_colletion = s.dash_colletion,
    is_open = s.is_open,
    box_group = s.box_group,
    box_group_name = s.box_group_name,
    last_used = s.last_used,
    hidden_status = s.hidden_status,
    leakage_status = s.leakage_status,
    box_city = s.box_city,
    box_region = s.box_region,
    box_state_status = s.box_state_status,
    box_message = s.box_message

WHEN NOT MATCHED THEN
INSERT (
    box_id,
    box_name,
    box_model,
    box_status,
    uco_hold,
    slops_hold,
    created_at,
    updated_at,
    box_address,
    dash_colletion,
    is_open,
    box_group,
    box_group_name,
    last_used,
    hidden_status,
    leakage_status,
    box_city,
    box_region,
    box_state_status,
    box_message
)
VALUES (
    s.box_id,
    s.box_name,
    s.box_model,
    s.box_status,
    s.uco_hold,
    s.slops_hold,
    s.created_at,
    s.updated_at,
    s.box_address,
    s.dash_colletion,
    s.is_open,
    s.box_group,
    s.box_group_name,
    s.last_used,
    s.hidden_status,
    s.leakage_status,
    s.box_city,
    s.box_region,
    s.box_state_status,
    s.box_message
);

WITH dedup AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY payload ->> 'internal_id'
                                  ORDER BY (payload ->> 'createdAt')::timestamptz DESC) AS rn
        FROM noovoleum.get_box
    ) sub
    WHERE rn = 1
)
INSERT INTO noovoleum.stg_table_box (
    box_name,
    internal_id,
    box_model,
    box_status,
    uco_hold,
    slops_hold,
    created_at,
    updated_at,
    box_address,
    dash_colletion,
    is_open,
    box_group,
    box_group_name,
    last_used,
    hidden_status,
    leakage_status,
    box_city,
    box_region,
    box_state_status,
    box_message
)
SELECT
    payload ->> 'id' AS box_id
    payload ->> 'name' AS box_name,
    payload ->> 'internal_id' AS internal_id,
    payload ->> 'model' AS box_model,
    payload ->> 'status' AS box_status,
    (payload ->> 'lUCOHold')::double precision AS uco_hold,
    (payload ->> 'lSlopsHold')::double precision AS slops_hold,
    (payload ->> 'createdAt')::timestamptz AS created_at,
    (payload ->> 'updatedAt')::timestamptz AS updated_at,
    payload ->> 'address' AS box_address,
    (payload ->> 'dashCollectionPoint')::bool AS dash_colletion,
    (payload ->> 'isOpen')::bool AS is_open,
    payload ->> 'group' AS box_group,
    payload ->> 'group_name' AS box_group_name,
    (payload ->> 'lastUsed')::timestamptz AS last_used,
    (payload ->> 'hidden')::bool AS hidden_status,
    (payload ->> 'leakage')::bool AS leakage_status,
    payload ->> 'city' AS box_city,
    payload ->> 'region' AS box_region,
    payload ->> 'box_state' AS box_state_status,
    payload -> 'box_error_message' -> 0 ->> 'message' AS box_message
FROM dedup
ON CONFLICT (internal_id) DO UPDATE
SET
    box_name = EXCLUDED.box_name,
    box_model = EXCLUDED.box_model,
    box_status = EXCLUDED.box_status,
    uco_hold = EXCLUDED.uco_hold,
    slops_hold = EXCLUDED.slops_hold,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at,
    box_address = EXCLUDED.box_address,
    dash_colletion = EXCLUDED.dash_colletion,
    is_open = EXCLUDED.is_open,
    box_group = EXCLUDED.box_group,
    box_group_name = EXCLUDED.box_group_name,
    last_used = EXCLUDED.last_used,
    hidden_status = EXCLUDED.hidden_status,
    leakage_status = EXCLUDED.leakage_status,
    box_city = EXCLUDED.box_city,
    box_region = EXCLUDED.box_region,
    box_state_status = EXCLUDED.box_state_status,
    box_message = EXCLUDED.box_message;