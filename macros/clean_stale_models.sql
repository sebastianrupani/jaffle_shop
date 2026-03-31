{% macro clean_stale_models(database=target.database, schema=target.schema, days=7, dry_run=True) %}

    {% set get_drop_commands_query %}
        select
         case 
            when table_type = 'VIEW' THEN table_type
            ELSE 'TABLE' END AS drop_type,

         'DROP ' || drop_type || ' {{ database | upper }}.' || table_schema || '.' || table_name 

        FROM {{ database }}.information_schema.tables
        WHERE table_schema = UPPER('{{ schema }}')
          AND last_altered <= CURRENT_DATE() - {{ days }}
    {% endset %}

    {{ log('\nGenerating cleanup queries...\n', info=True) }}
    
    {% if execute %}
        {% set drop_queries = run_query(get_drop_commands_query).columns[1].values() %}
        {% for drop_query in drop_queries %}
            {% if not dry_run %}
                {{ log('Dropping table/view with command: ' ~ drop_query, info=True) }}
                {% do run_query(drop_query) %}    
            {% else %}
                {{ log(drop_query, info=True) }}
            {% endif %}
        {% endfor %}
    {% endif %}
  
{% endmacro %}