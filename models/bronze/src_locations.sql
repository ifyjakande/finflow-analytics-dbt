{{
    config(
        materialized='incremental',
        unique_key='location_key'
    )
}}

SELECT 
    location_key,
    location_id,
    location_name,
    address,
    city,
    state,
    region,
    country,
    country_name,
    country_code,
    currency_code,
    postal_code,
    timezone,
    latitude,
    longitude,
    created_at,
    updated_at,
    ingestion_timestamp
FROM {{ source('finflow_analytics_production', 'src_locations') }}
WHERE TRUE
    {% if is_incremental() %}
    AND ingestion_timestamp > (SELECT max(ingestion_timestamp) FROM {{ this }})
    {% endif %}
