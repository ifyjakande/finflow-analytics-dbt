WITH risk_metrics AS (
    SELECT
        a.account_key,
        c.customer_segment,
        p.product_category,
        arm.credit_utilization_pct,
        arm.below_minimum_balance_flag,
        arm.rapid_transaction_count,
        a.balance,
        a.credit_limit
    FROM {{ ref('dim_account') }} a
    JOIN {{ ref('dim_customer') }} c ON a.customer_key = c.customer_key
    JOIN {{ ref('dim_product') }} p ON a.product_key = p.product_key
    JOIN {{ ref('int_account_risk_metrics') }} arm ON a.account_key = arm.account_key
)

SELECT
    customer_segment,
    product_category,
    COUNT(*) as total_accounts,
    AVG(credit_utilization_pct) as avg_credit_utilization,
    SUM(below_minimum_balance_flag) as accounts_below_minimum,
    AVG(rapid_transaction_count) as avg_rapid_transactions,
    SUM(CASE WHEN credit_utilization_pct > 80 THEN 1 ELSE 0 END) as high_risk_accounts
FROM risk_metrics
GROUP BY 1, 2
ORDER BY high_risk_accounts DESC