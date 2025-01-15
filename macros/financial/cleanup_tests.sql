{# Macro to clean up dbt test result tables that are stored in BigQuery #}
{% macro cleanup_test_results() %}
    {# First, construct a SQL query to generate DROP TABLE statements #}
    {% set sql %}
        -- This query generates DROP TABLE commands for each test result table
        -- Concatenates the full table path: `project.dataset.table`
        SELECT 'DROP TABLE `' || table_catalog || '.' || table_schema || '.' || table_name || '`' as drop_commands
        FROM {{ target.database }}.{{ target.schema }}.INFORMATION_SCHEMA.TABLES 
        -- Filter for tables that start with 'source_'
        WHERE table_name LIKE 'source_%'
        -- And contain either 'unique' or 'not_null' in their names
        AND (
            table_name LIKE '%unique%' 
            OR table_name LIKE '%not_null%'
        )
    {% endset %}

    {# Execute the query and store results #}
    {% set results = run_query(sql) %}
    
    {# Only execute the DROP statements if we're in execution mode (not parsing) #}
    {% if execute %}
        {# Loop through each row in the results #}
        {% for row in results %}
            {# Extract the DROP command from the first column #}
            {% set drop_command = row[0] %}
            {# Execute the DROP command #}
            {% do run_query(drop_command) %}
        {% endfor %}
    {% endif %}

{% endmacro %}
