{{ config(
    tags=['finance', 'date_validation'],
    error_if='>500',
    warn_if='>100'
) }}

SELECT
    date_key,
    full_date
FROM {{ ref('dim_date') }}
WHERE full_date > '2025-12-31'
   OR full_date < '2020-01-01' 