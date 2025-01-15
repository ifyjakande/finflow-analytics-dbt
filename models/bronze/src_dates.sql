{{
    config(
        materialized='incremental',
        unique_key='date_key'
    )
}}

SELECT 
    date_key,
    full_date,
    year,
    quarter,
    month,
    day,
    day_of_week,
    is_weekend,
    is_holiday,
    fiscal_year,
    created_at,
    updated_at,
    ingestion_timestamp
FROM {{ source('finflow_analytics_production', 'src_dates') }}
WHERE TRUE
    {% if is_incremental() %}
    AND ingestion_timestamp > (SELECT max(ingestion_timestamp) FROM {{ this }})
    {% endif %}