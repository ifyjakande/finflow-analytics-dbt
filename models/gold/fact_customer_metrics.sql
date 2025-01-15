WITH customer_base AS (
    SELECT 
        cm.customer_key,
        cm.month as metric_date,
        cm.monthly_transactions,
        cm.monthly_volume,
        cm.avg_daily_volume,
        cm.month_over_month_growth,
        -- Product metrics
        cpm.product_categories_used,
        cpm.product_subcategories_used,
        cpm.active_products_count,
        -- Customer attributes
        c.customer_segment,
        c.total_accounts,
        c.active_accounts
    FROM {{ ref('int_customer_metrics') }} cm
    LEFT JOIN {{ ref('int_customer_product_metrics') }} cpm 
        ON cm.customer_key = cpm.customer_key
    LEFT JOIN {{ ref('src_customers') }} c 
        ON cm.customer_key = c.customer_key
),

transaction_aggregates AS (
    SELECT
        customer_key,
        DATE_TRUNC(transaction_month, MONTH) as metric_date,
        SUM(total_fees) as monthly_fees,
        COUNT(DISTINCT transaction_type) as unique_transaction_types,
        COUNT(DISTINCT account_key) as active_accounts_with_transactions
    FROM {{ ref('int_transaction_metrics') }}
    GROUP BY 1, 2
),

final AS (
    SELECT 
        cb.*,
        ta.monthly_fees,
        ta.unique_transaction_types,
        ta.active_accounts_with_transactions,
        -- Derived metrics
        CASE
            WHEN cb.monthly_volume > 100000 THEN 'High'
            WHEN cb.monthly_volume > 10000 THEN 'Medium'
            ELSE 'Low'
        END as volume_tier,
        CASE
            WHEN cb.month_over_month_growth > 0.2 THEN 'High Growth'
            WHEN cb.month_over_month_growth > 0 THEN 'Growing'
            WHEN cb.month_over_month_growth = 0 THEN 'Stable'
            ELSE 'Declining'
        END as growth_status,
        current_timestamp() as dw_updated_at
    FROM customer_base cb
    LEFT JOIN transaction_aggregates ta 
        ON cb.customer_key = ta.customer_key 
        AND cb.metric_date = ta.metric_date
)

SELECT * FROM final
