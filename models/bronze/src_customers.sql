{{
    config(
        materialized='incremental',
        unique_key='customer_key'
    )
}}

SELECT 
    customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    address,
    city,
    state,
    country,
    postal_code,
    customer_segment,
    customer_since_date,
    total_accounts,
    active_accounts,
    total_transaction_volume,
    last_transaction_date,
    is_active,
    created_at,
    updated_at,
    ingestion_timestamp
FROM {{ source('finflow_analytics_production', 'src_customers') }}
WHERE TRUE
    {% if is_incremental() %}
    AND ingestion_timestamp > (SELECT max(ingestion_timestamp) FROM {{ this }})
    {% endif %}