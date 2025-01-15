{{
    config(
        materialized='incremental',
        unique_key=['transaction_id', 'date_key'],
        tags=['gold', 'fact']
    )
}}

WITH transaction_base AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['t.transaction_id', 't.date_key']) }} as transaction_sk,
        t.transaction_id,
        t.account_key,
        t.customer_key,
        t.product_key,
        t.location_key,
        t.date_key,
        t.transaction_type,
        t.transaction_amount,
        t.fee_amount,
        t.transaction_status,
        t.created_at,
        -- Get account info at time of transaction
        a.account_type,
        a.account_status,
        a.balance as account_balance,
        -- Get product info
        p.product_category,
        p.product_subcategory,
        p.interest_rate,
        p.monthly_fee,
        -- Get customer info
        c.customer_segment,
        -- Get location info
        l.country,
        l.region,
        -- Get date attributes
        d.year,
        d.quarter,
        d.month,
        d.is_weekend,
        d.is_holiday
    FROM {{ ref('src_transactions') }} t
    LEFT JOIN {{ ref('dim_account') }} a 
        ON t.account_key = a.account_key
    LEFT JOIN {{ ref('dim_product') }} p 
        ON t.product_key = p.product_key
    LEFT JOIN {{ ref('dim_customer') }} c 
        ON t.customer_key = c.customer_key
        AND t.created_at >= c.valid_from 
        AND (t.created_at < c.valid_to OR c.valid_to IS NULL)
        AND c.is_current = true
    LEFT JOIN {{ ref('dim_location') }} l 
        ON t.location_key = l.location_key
    LEFT JOIN {{ ref('dim_date') }} d 
        ON t.date_key = d.date_key
    {% if is_incremental() %}
    WHERE t.created_at > (
        SELECT max(created_at) 
        FROM {{ this }}
    )
    {% endif %}
),

enriched_transactions AS (
    SELECT 
        tb.*,
        -- Derived transaction metrics
        CASE
            WHEN transaction_type = 'Withdrawal' THEN -transaction_amount
            ELSE transaction_amount
        END as net_amount,
        
        -- Time-based flags
        CASE 
            WHEN EXTRACT(HOUR FROM created_at) BETWEEN 9 AND 17 THEN 'Business Hours'
            ELSE 'Non-Business Hours'
        END as transaction_time_category,
        
        -- Risk indicators
        CASE
            WHEN transaction_amount > 10000 THEN 'High'
            WHEN transaction_amount > 1000 THEN 'Medium'
            ELSE 'Low'
        END as transaction_size_category,
        
        -- Account status indicators
        CASE
            WHEN account_balance < 0 THEN 1
            ELSE 0
        END as is_negative_balance,
        
        -- Customer value indicators
        CASE
            WHEN customer_segment = 'High Value' AND transaction_amount > 5000 THEN 1
            ELSE 0
        END as is_high_value_transaction,
        
        -- Geographic indicators
        CASE
            WHEN country = 'US' THEN 'Domestic'
            ELSE 'International'
        END as transaction_location_type,
        
        -- Temporal metrics
        EXTRACT(HOUR FROM created_at) as transaction_hour,
        EXTRACT(DAYOFWEEK FROM created_at) as transaction_day_of_week,
        
        -- Financial metrics
        COALESCE(fee_amount, 0) / NULLIF(transaction_amount, 0) * 100 as fee_percentage,
        
        current_timestamp() as dw_updated_at
    FROM transaction_base tb
)

SELECT * FROM enriched_transactions
