-- macros/test_credit_utilization.sql
{% test credit_utilization(model, column_name) %}
    select *
    from {{ model }}
    where not(
        {{ column_name }} is null or 
        ({{ column_name }} >= 0 and {{ column_name }} <= 100)
    )
{% endtest %}