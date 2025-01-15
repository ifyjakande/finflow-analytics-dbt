WITH date_base AS (
    SELECT *
    FROM {{ ref('src_dates') }}
),

final AS (
    SELECT 
        date_key,
        full_date,
        year,
        quarter,
        month,
        day,
        day_of_week,
        is_weekend,
        is_holiday,
        fiscal_year,
        created_at,
        updated_at,
        ingestion_timestamp,
        -- Add derived columns
        CASE 
            WHEN month IN (12, 1, 2) THEN 'Winter'
            WHEN month IN (3, 4, 5) THEN 'Spring'
            WHEN month IN (6, 7, 8) THEN 'Summer'
            ELSE 'Fall'
        END as season,
        CONCAT('Q', quarter, ' ', year) as quarter_name,
        FORMAT_DATE('%B', full_date) as month_name,
        FORMAT_DATE('%Y-%m', full_date) as year_month
    FROM date_base
)

SELECT * FROM final
