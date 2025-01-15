WITH product_base AS (
    SELECT *
    FROM {{ ref('src_products') }}
),

final AS (
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
        ingestion_timestamp,
        -- Add derived columns
        CASE 
            WHEN interest_rate > 10 THEN 'High'
            WHEN interest_rate > 5 THEN 'Medium'
            ELSE 'Low'
        END as interest_rate_tier,
        CASE 
            WHEN monthly_fee = 0 THEN 'Free'
            WHEN monthly_fee < 10 THEN 'Basic'
            WHEN monthly_fee < 25 THEN 'Standard'
            ELSE 'Premium'
        END as fee_tier
    FROM product_base
)

SELECT * FROM final
