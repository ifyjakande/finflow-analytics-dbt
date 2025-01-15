{#
    This macro calculates the percentage growth rate between two values.
    
    Args:
        current_value (numeric): The current period's value
        previous_value (numeric): The previous period's value
    
    Returns:
        numeric: The percentage growth rate, rounded to 2 decimal places
        
    Behavior:
        - Returns NULL if previous_value is NULL or 0 (to avoid division by zero)
        - Calculates ((current - previous) / |previous|) * 100
        - Uses absolute value in denominator to ensure proper sign
        - Rounds result to 2 decimal places
    
    Example Usage:
        -- Calculate month-over-month revenue growth
        SELECT 
            date_month,
            revenue,
            {{ calculate_growth_rate('revenue', 'LAG(revenue) OVER (ORDER BY date_month)') }}
                as revenue_growth_pct
        FROM monthly_revenue
        
        -- Calculate year-over-year growth
        SELECT 
            customer_id,
            year,
            total_spend,
            {{ calculate_growth_rate('total_spend', 'prev_year_spend') }}
                as yoy_growth_pct
        FROM customer_yearly_metrics
#}

{% macro calculate_growth_rate(current_value, previous_value) %}
    CASE
        WHEN {{ previous_value }} IS NULL OR {{ previous_value }} = 0 
        THEN NULL
        ELSE ROUND(
            ({{ current_value }} - {{ previous_value }}) / 
            ABS({{ previous_value }}) * 100,
            2
        )
    END
{% endmacro %}