{{ config(
    tags=['finance', 'balance_validation'],
    error_if='>500',
    warn_if='>100'
) }}

WITH transaction_totals AS (
    SELECT
        t.account_key,
        SUM(CASE 
            WHEN transaction_type IN ('Deposit', 'Transfer In', 'Credit', 'Payment') THEN transaction_amount
            WHEN transaction_type IN ('Withdrawal', 'Transfer Out', 'Debit') THEN -transaction_amount
            ELSE 0 
        END) + COALESCE(MIN(a.initial_deposit), 0) as calculated_balance
    FROM {{ ref('fact_transactions') }} t
    LEFT JOIN {{ ref('dim_account') }} a 
        ON t.account_key = a.account_key
    WHERE t.transaction_status = 'Completed'
    GROUP BY t.account_key
)

SELECT 
    a.account_key,
    a.balance as reported_balance,
    t.calculated_balance,
    ABS(a.balance - t.calculated_balance) as balance_difference
FROM {{ ref('dim_account') }} a
JOIN transaction_totals t ON a.account_key = t.account_key
WHERE ABS(a.balance - t.calculated_balance) > 1.00
    AND a.account_status = 'Active'