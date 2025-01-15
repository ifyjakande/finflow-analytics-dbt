{{
    config(
        materialized='incremental',
        unique_key=['customer_key', 'valid_from'],
        tags=['gold', 'dimension']
    )
}}

WITH source_customers AS (
    {{ deduplicate_records(
        relation=ref('src_customers'),
        key_column='customer_key'
    ) }}
),

customer_product_metrics AS (
    SELECT * FROM {{ ref('int_customer_product_metrics') }}
),

-- Track changes for SCD Type 2
change_tracking AS (
    SELECT 
        c.customer_key,
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.phone_number,
        c.address,
        c.city,
        c.state,
        c.country,
        c.postal_code,
        c.customer_segment,
        c.customer_since_date,
        c.total_accounts,
        c.active_accounts,
        c.total_transaction_volume,
        c.last_transaction_date,
        c.is_active,
        -- New metrics from customer_product_metrics
        pm.product_categories_used,
        pm.product_subcategories_used,
        pm.first_product_date,
        pm.latest_product_date,
        pm.active_products_count,
        c.created_at,
        c.updated_at,
        c.ingestion_timestamp as valid_from,
        LEAD(c.ingestion_timestamp) OVER (
            PARTITION BY c.customer_key 
            ORDER BY c.ingestion_timestamp
        ) as valid_to,
        CASE 
            WHEN LEAD(c.ingestion_timestamp) OVER (
                PARTITION BY c.customer_key 
                ORDER BY c.ingestion_timestamp
            ) IS NULL THEN TRUE 
            ELSE FALSE 
        END as is_current
    FROM source_customers c
    LEFT JOIN customer_product_metrics pm 
        ON c.customer_key = pm.customer_key
)

SELECT 
    *,
    current_timestamp() as dw_updated_at
FROM change_tracking
{% if is_incremental() %}
WHERE customer_key IN (
    SELECT DISTINCT customer_key 
    FROM source_customers 
    WHERE ingestion_timestamp > COALESCE(
        (SELECT max(valid_from) FROM {{ this }}),
        '1900-01-01'
    )
)
{% endif %}