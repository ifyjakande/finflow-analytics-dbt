{% snapshot customer_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_key',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

SELECT 
    customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    customer_segment,
    is_active,
    total_accounts,
    active_accounts,
    total_transaction_volume,
    last_transaction_date,
    updated_at
FROM {{ source('finflow_analytics_production', 'src_customers') }}

{% endsnapshot %}