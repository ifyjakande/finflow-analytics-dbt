WITH customer_revenue AS (
    SELECT
        c.customer_key,
        c.customer_segment,
        DATE_DIFF(CURRENT_DATE, c.customer_since_date, MONTH) as tenure_months,
        SUM(t.transaction_amount) as total_revenue,
        SUM(t.fee_amount) as total_fees
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN {{ ref('fact_transactions') }} t 
        ON c.customer_key = t.customer_key
    WHERE t.transaction_type IN ('Deposit', 'Fee')
    GROUP BY 1, 2, 3
)

SELECT
    customer_segment,
    AVG(tenure_months) as avg_tenure_months,
    AVG(total_revenue) as avg_total_revenue,
    AVG(total_fees) as avg_total_fees,
    AVG((total_revenue + total_fees) / NULLIF(tenure_months, 0)) as avg_monthly_value
FROM customer_revenue
GROUP BY 1
ORDER BY avg_monthly_value DESC