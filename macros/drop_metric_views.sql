{% macro drop_metric_views() %}
  {# 
    Utility macro to drop all metric views defined in models.
    Run with: dbt run-operation drop_metric_views
  #}
  
  {% if execute %}
    {% for node in graph.nodes.values() %}
      {% if node.resource_type == 'model' %}
        {% set mv = node.meta.get('metric_view', {}) %}
        
        {% if mv.get('enabled', false) %}
          {% set catalog = node.database %}
          {% set schema = node.schema %}
          {% set view_name = mv.name %}
          
          {{ log("Dropping metric view: " ~ catalog ~ "." ~ schema ~ "." ~ view_name, info=True) }}
          
          {% set drop_sql %}
            DROP VIEW IF EXISTS {{ catalog }}.{{ schema }}.{{ view_name }};
          {% endset %}
          
          {% do run_query(drop_sql) %}
          
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}

