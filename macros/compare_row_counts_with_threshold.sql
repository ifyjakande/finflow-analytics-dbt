{% test compare_row_counts_with_threshold(model, compare_model, group_by_columns, threshold_percent=5) %}

with a as (
    select 
        {% for column in group_by_columns %}
        {{ column }}{% if not loop.last %},{% endif %}
        {% endfor %},
        count(*) as row_count_a
    from {{ model }}
    group by {% for column in group_by_columns %}
        {{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
),

b as (
    select 
        {% for column in group_by_columns %}
        {{ column }}{% if not loop.last %},{% endif %}
        {% endfor %},
        count(*) as row_count_b
    from {{ compare_model }}
    group by {% for column in group_by_columns %}
        {{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
),

final as (
    select 
        a.row_count_a,
        b.row_count_b,
        abs(a.row_count_a - b.row_count_b) as difference,
        (abs(a.row_count_a - b.row_count_b)::float / nullif(greatest(a.row_count_a, b.row_count_b), 0) * 100) as difference_percent
    from a 
    full outer join b using ({% for column in group_by_columns %}
        {{ column }}{% if not loop.last %},{% endif %}
        {% endfor %})
    where difference_percent > {{ threshold_percent }}
)

select * from final

{% endtest %}