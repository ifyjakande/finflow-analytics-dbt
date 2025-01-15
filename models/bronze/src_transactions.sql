{{
    config(
        materialized='incremental',
        unique_key='transaction_id'
    )
}}

SELECT 
    transaction_id,
    account_key,
    customer_key,
    product_key,
    location_key,
    date_key,
    transaction_type,
    transaction_amount,
    fee_amount,
    transaction_status,
    created_at,
    updated_at,
    ingestion_timestamp
FROM {{ source('finflow_analytics_production', 'src_transactions') }}
WHERE TRUE
    {% if is_incremental() %}
    AND ingestion_timestamp > (SELECT max(ingestion_timestamp) FROM {{ this }})
    {% endif %}

