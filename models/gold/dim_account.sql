WITH deduplicated_accounts AS (
    {{ deduplicate_records(
        relation=ref('src_accounts'),
        key_column='account_key',
        order_by_column='updated_at'
    ) }}
),

account_base AS (
    SELECT 
        account_key,
        account_id,
        customer_key,
        product_key,
        account_type,
        account_status,
        initial_deposit,
        minimum_balance,
        balance,
        credit_limit,
        opened_date,
        closed_date,
        created_at,
        updated_at
    FROM deduplicated_accounts
),

product_info AS (
    SELECT 
        product_key,
        product_name,
        product_category,
        product_subcategory,
        interest_rate,
        monthly_fee
    FROM {{ ref('src_products') }}
),

final AS (
    SELECT 
        ab.account_key,
        ab.account_id,
        ab.customer_key,
        ab.product_key,
        ab.account_type,
        ab.account_status,
        ab.initial_deposit,
        ab.minimum_balance,
        ab.balance,
        ab.credit_limit,
        ab.opened_date,
        ab.closed_date,
        p.product_name,
        p.product_category,
        p.product_subcategory,
        p.interest_rate,
        p.monthly_fee,
        CASE
            WHEN ab.balance >= 1000000 THEN 'High Balance'
            WHEN ab.balance >= 100000 THEN 'Medium Balance'
            WHEN ab.balance >= 0 THEN 'Low Balance'
            ELSE 'Negative Balance'
        END as balance_segment,
        CASE
            WHEN p.product_category = 'Credit' 
            THEN ab.credit_limit - ab.balance
            ELSE ab.balance - ab.minimum_balance
        END as available_funds,
        ab.created_at,
        ab.updated_at,
        current_timestamp() as dw_updated_at
    FROM account_base ab
    LEFT JOIN product_info p 
        ON ab.product_key = p.product_key
)

SELECT * FROM final