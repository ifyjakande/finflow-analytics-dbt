WITH daily_metrics AS (
    SELECT 
        a.account_key,
        d.full_date as metric_date,
        COUNT(DISTINCT t.transaction_id) as daily_transactions,
        SUM(CASE 
            WHEN t.transaction_type = 'Deposit' THEN t.transaction_amount
            WHEN t.transaction_type = 'Withdrawal' THEN -t.transaction_amount
            WHEN t.transaction_type = 'Transfer' THEN t.transaction_amount  -- Simplified transfer handling
            ELSE 0 
        END) as net_daily_change,
        SUM(t.fee_amount) as daily_fees
    FROM {{ ref('src_accounts') }} a
    LEFT JOIN {{ ref('src_transactions') }} t 
        ON a.account_key = t.account_key
    LEFT JOIN {{ ref('src_dates') }} d
        ON t.date_key = d.date_key
    GROUP BY 1, 2
),

monthly_metrics AS (
    SELECT 
        account_key,
        DATE_TRUNC(metric_date, MONTH) as month,
        SUM(daily_transactions) as monthly_transactions,
        SUM(net_daily_change) as net_monthly_change,
        SUM(daily_fees) as monthly_fees,
        AVG(net_daily_change) as avg_daily_change
    FROM daily_metrics
    GROUP BY 1, 2
),

with_previous_month AS (
    SELECT 
        *,
        LAG(net_monthly_change) OVER (
            PARTITION BY account_key 
            ORDER BY month
        ) as prev_month_change
    FROM monthly_metrics
)

SELECT 
    *,
    {{ calculate_growth_rate('net_monthly_change', 'prev_month_change') }} 
        as month_over_month_growth
FROM with_previous_month