WITH daily_balance AS (
    SELECT 
        a.account_key,
        d.date_key,
        d.full_date,
        -- Account attributes
        MAX(a.account_type) as account_type,
        MAX(a.account_status) as account_status,
        MAX(a.minimum_balance) as minimum_balance,
        MAX(a.credit_limit) as credit_limit,
        -- Risk metrics
        MAX(arm.below_minimum_balance_flag) as below_minimum_balance_flag,
        MAX(arm.credit_utilization_pct) as credit_utilization_pct,
        MAX(arm.rapid_transaction_count) as rapid_transaction_count,
        -- Daily transaction metrics
        COUNT(t.transaction_id) as daily_transaction_count,
        SUM(CASE 
            WHEN t.transaction_type IN ('Deposit', 'Transfer In') THEN t.transaction_amount
            WHEN t.transaction_type IN ('Withdrawal', 'Transfer Out') THEN -t.transaction_amount
            ELSE 0 
        END) as net_daily_change,
        SUM(t.fee_amount) as daily_fees,
        -- Balance tracking
        MIN(a.balance) as opening_balance,  -- First balance of the day
        MAX(a.balance) as closing_balance   -- Last balance of the day
    FROM {{ ref('src_accounts') }} a
    CROSS JOIN {{ ref('src_dates') }} d
    LEFT JOIN {{ ref('src_transactions') }} t 
        ON a.account_key = t.account_key 
        AND d.date_key = t.date_key
    LEFT JOIN {{ ref('int_account_risk_metrics') }} arm 
        ON a.account_key = arm.account_key
    WHERE d.full_date BETWEEN a.opened_date 
        AND COALESCE(a.closed_date, CURRENT_DATE())
    GROUP BY 1, 2, 3
),

-- Rest of the CTEs remain the same
monthly_aggregates AS (
    SELECT
        account_key,
        DATE_TRUNC(full_date, MONTH) as month,
        AVG(closing_balance) as avg_monthly_balance,
        MIN(closing_balance) as min_monthly_balance,
        MAX(closing_balance) as max_monthly_balance,
        SUM(daily_transaction_count) as monthly_transaction_count,
        SUM(daily_fees) as monthly_fees,
        SUM(CASE WHEN below_minimum_balance_flag = 1 THEN 1 ELSE 0 END) 
            as days_below_minimum
    FROM daily_balance
    GROUP BY 1, 2
),

final AS (
    SELECT 
        db.*,
        ma.avg_monthly_balance,
        ma.min_monthly_balance,
        ma.max_monthly_balance,
        ma.monthly_transaction_count,
        ma.monthly_fees,
        ma.days_below_minimum,
        -- Derived metrics
        CASE
            WHEN db.closing_balance < db.opening_balance THEN 'Decrease'
            WHEN db.closing_balance > db.opening_balance THEN 'Increase'
            ELSE 'No Change'
        END as daily_balance_trend,
        CASE
            WHEN db.credit_utilization_pct > 80 THEN 'High Risk'
            WHEN db.credit_utilization_pct > 50 THEN 'Medium Risk'
            WHEN db.credit_utilization_pct > 30 THEN 'Low Risk'
            ELSE 'Healthy'
        END as credit_risk_category,
        current_timestamp() as dw_updated_at
    FROM daily_balance db
    LEFT JOIN monthly_aggregates ma 
        ON db.account_key = ma.account_key 
        AND DATE_TRUNC(db.full_date, MONTH) = ma.month
)

SELECT * FROM final
