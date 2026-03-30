{% macro cent_to_dollars(amount, decimals=2) %}
    ROUND({{ amount }} / 100, {{ decimals }})
{% endmacro %}