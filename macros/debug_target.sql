{% macro print_target_schema() %}
    {% do log("Current target schema: " ~ target.schema, info=True) %}
{% endmacro %}
