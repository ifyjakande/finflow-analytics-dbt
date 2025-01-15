{{
    config(
        materialized='incremental',
        unique_key='account_key'
    )
}}

SELECT 
    account_key,
    account_id,
    customer_key,
    product_key,
    account_type,
    account_status,
    initial_deposit,
    minimum_balance,
    balance,
    credit_limit,
    opened_date,
    closed_date,
    created_at,
    updated_at,
    ingestion_timestamp
FROM {{ source('finflow_analytics_production', 'src_accounts') }}
WHERE TRUE
    {% if is_incremental() %}
    AND ingestion_timestamp > (SELECT max(ingestion_timestamp) FROM {{ this }})
    {% endif %}