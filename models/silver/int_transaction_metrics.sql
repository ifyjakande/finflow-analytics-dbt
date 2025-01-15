WITH transaction_patterns AS (
    SELECT 
        t.customer_key,
        t.account_key,
        t.transaction_type,
        DATE_TRUNC(d.full_date, MONTH) as transaction_month,
        COUNT(*) as transaction_count,
        SUM(t.transaction_amount) as total_amount,
        AVG(t.transaction_amount) as avg_amount,
        SUM(t.fee_amount) as total_fees
    FROM {{ ref('src_transactions') }} t
    LEFT JOIN {{ ref('src_dates') }} d
        ON t.date_key = d.date_key
    GROUP BY 1, 2, 3, 4
),

customer_averages AS (
    SELECT 
        customer_key,
        transaction_type,
        AVG(transaction_count) as avg_monthly_transactions,
        AVG(total_amount) as avg_monthly_amount,
        AVG(total_fees) as avg_monthly_fees
    FROM transaction_patterns
    GROUP BY 1, 2
),

monthly_rankings AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_key, transaction_month
            ORDER BY total_amount DESC
        ) as transaction_rank
    FROM transaction_patterns
)

SELECT 
    tp.*,
    ca.avg_monthly_transactions,
    ca.avg_monthly_amount,
    ca.avg_monthly_fees,
    mr.transaction_rank,
    CASE 
        WHEN tp.total_amount > ca.avg_monthly_amount * 2 THEN 'High'
        WHEN tp.total_amount < ca.avg_monthly_amount * 0.5 THEN 'Low'
        ELSE 'Normal'
    END as volume_category
FROM transaction_patterns tp
JOIN customer_averages ca 
    ON tp.customer_key = ca.customer_key 
    AND tp.transaction_type = ca.transaction_type
JOIN monthly_rankings mr 
    ON tp.customer_key = mr.customer_key 
    AND tp.transaction_month = mr.transaction_month 
    AND tp.transaction_type = mr.transaction_type