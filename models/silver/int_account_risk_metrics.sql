WITH transaction_timing AS (
    SELECT 
        account_key,
        created_at,
        -- Calculate time difference between transactions
        TIMESTAMP_DIFF(
            created_at,
            LAG(created_at) OVER(
                PARTITION BY account_key 
                ORDER BY created_at
            ),
            MINUTE
        ) as time_diff_minutes
    FROM {{ ref('src_transactions') }}
),

rapid_transactions AS (
    SELECT
        account_key,
        COUNT(CASE WHEN time_diff_minutes < 5 THEN 1 END) as rapid_transaction_count
    FROM transaction_timing
    GROUP BY account_key
),

risk_metrics AS (
    SELECT 
        a.account_key,
        -- Balance risk indicators
        CASE 
            WHEN a.balance < a.minimum_balance THEN 1 
            ELSE 0 
        END as below_minimum_balance_flag,
        
        -- Credit utilization with better handling
        CASE 
            WHEN a.account_type = 'Credit' AND a.credit_limit > 0
            THEN LEAST(SAFE_DIVIDE(a.balance, a.credit_limit) * 100, 100)  -- Cap at 100%
            ELSE NULL 
        END as credit_utilization_pct,
        
        -- Join with rapid transactions
        COALESCE(rt.rapid_transaction_count, 0) as rapid_transaction_count
        
    FROM {{ ref('src_accounts') }} a
    LEFT JOIN rapid_transactions rt 
        ON a.account_key = rt.account_key
)

SELECT * FROM risk_metrics
