{{
    config(
        materialized='incremental',
        unique_key='product_key'
    )
}}

SELECT 
    product_key,
    product_id,
    product_name,
    product_category,
    product_subcategory,
    interest_rate,
    monthly_fee,
    is_active,
    created_at,
    updated_at,
    ingestion_timestamp
FROM {{ source('finflow_analytics_production', 'src_products') }}
WHERE TRUE
    {% if is_incremental() %}
    AND ingestion_timestamp > (SELECT max(ingestion_timestamp) FROM {{ this }})
    {% endif %}

