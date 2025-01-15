WITH daily_metrics AS (
    SELECT 
        c.customer_key,
        d.full_date as metric_date,
        COUNT(DISTINCT t.transaction_id) as daily_transactions,
        SUM(t.transaction_amount) as daily_volume
    FROM {{ ref('src_customers') }} c
    LEFT JOIN {{ ref('src_transactions') }} t 
        ON c.customer_key = t.customer_key
    LEFT JOIN {{ ref('src_dates') }} d
        ON t.date_key = d.date_key
    GROUP BY 1, 2
),

monthly_metrics AS (
    SELECT 
        customer_key,
        DATE_TRUNC(metric_date, MONTH) as month,
        SUM(daily_transactions) as monthly_transactions,
        SUM(daily_volume) as monthly_volume,
        AVG(daily_volume) as avg_daily_volume
    FROM daily_metrics
    GROUP BY 1, 2
),

with_previous_month AS (
    SELECT 
        *,
        LAG(monthly_volume) OVER (
            PARTITION BY customer_key 
            ORDER BY month
        ) as prev_month_volume
    FROM monthly_metrics
)

SELECT 
    *,
    {{ calculate_growth_rate('monthly_volume', 'prev_month_volume') }} 
        as month_over_month_growth
FROM with_previous_month