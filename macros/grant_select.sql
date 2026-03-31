{% macro grant_select(schema=target.schema, role=target.role) %}

    {% set sql %}
        GRANT USAGE ON SCHEMA {{ schema }} TO ROLE {{ role }};
        GRANT SELECT ON ALL TABLES IN SCHEMA {{ schema }} TO ROLE {{ role }};
        GRANT SELECT ON ALL VIEWS IN SCHEMA {{ schema }} TO ROLE {{ role }};
    {% endset %}

    {{ log('Granting access to schema ' ~ target.schema ~ ' for role ' ~ target.role, info=True) }}
    {% do run_query(sql) %}
    {{ log('Privilege granted.', info=True) }}

{% endmacro %}