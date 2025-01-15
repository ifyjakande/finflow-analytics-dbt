WITH product_metrics AS (
    SELECT
        p.product_key,
        p.product_name,
        p.product_category,
        COUNT(DISTINCT a.account_key) as total_accounts,
        SUM(t.transaction_amount) as total_volume,
        SUM(t.fee_amount) as total_fees,
        AVG(a.balance) as avg_balance
    FROM {{ ref('dim_product') }} p
    LEFT JOIN {{ ref('dim_account') }} a ON p.product_key = a.product_key
    LEFT JOIN {{ ref('fact_transactions') }} t ON a.account_key = t.account_key
    GROUP BY 1, 2, 3
)

SELECT
    product_name,
    product_category,
    total_accounts,
    total_volume,
    total_fees,
    avg_balance,
    SAFE_DIVIDE(total_fees, total_accounts) as revenue_per_account,
    SAFE_DIVIDE(total_fees, total_volume) * 100 as fee_percentage
FROM product_metrics
ORDER BY revenue_per_account DESC