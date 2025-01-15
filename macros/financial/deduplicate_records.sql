{%- macro deduplicate_records(relation, key_column, order_by_column='updated_at') -%}
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY {{ key_column }}
                ORDER BY {{ order_by_column }} DESC
            ) as row_num
        FROM {{ relation }}
    )
    WHERE row_num = 1
{%- endmacro -%}
