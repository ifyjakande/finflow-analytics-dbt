WITH location_base AS (
    SELECT *
    FROM {{ ref('src_locations') }}
),

final AS (
    SELECT 
        location_key,
        location_id,
        location_name,
        address,
        city,
        state,
        region,
        country,
        country_name,
        country_code,
        currency_code,
        postal_code,
        timezone,
        latitude,
        longitude,
        created_at,
        updated_at,
        ingestion_timestamp,
        -- Add derived columns
        CASE 
            WHEN country_code = 'US' THEN 'Domestic'
            ELSE 'International'
        END as location_type,
        CONCAT(city, ', ', state, ', ', country) as full_location_name
    FROM location_base
)

SELECT * FROM final
