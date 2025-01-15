-- Test to ensure all transaction amounts are positive
SELECT
    transaction_id,
    transaction_amount
FROM {{ ref('fact_transactions') }}
WHERE transaction_amount < 0