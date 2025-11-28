{% macro generate_metric_views() %}
  {# 
    Generates Unity Catalog Metric Views from model metadata.
    Based on: https://github.com/databricks/dbt-databricks/issues/1106
    
    Requires Databricks Runtime 16.4+ and Unity Catalog.
    Metric Views are currently in Public Preview.
    
    Two modes:
    1. Raw YAML mode: Provide the full Databricks YAML directly in `yaml` field
       - Follows official syntax: https://docs.databricks.com/aws/en/metric-views/data-modeling/syntax
       - Source placeholder {{source}} is replaced with actual table reference
    
    2. Structured mode: Define dimensions/measures as structured properties
       - Simpler for basic metric views
       - Limited feature support
  #}
  
  {% if execute %}
    {% for node in graph.nodes.values() %}
      {% if node.resource_type == 'model' and node.config.get('meta', {}).get('metric_view') is defined %}
        {% set mv = node.config.get('meta', {}).get('metric_view') %}
        
        {# Also check node.meta for schema.yml defined metadata #}
        {% if mv is none or mv.get('enabled') is not true %}
          {% set mv = node.meta.get('metric_view', {}) %}
        {% endif %}
        
        {% if mv.get('enabled', false) %}
          {% set catalog = node.database %}
          {% set schema = node.schema %}
          {% set view_name = mv.name %}
          {% set table_name = node.alias or node.name %}
          {% set description = mv.get('description', '') %}
          {% set source_ref = catalog ~ '.' ~ schema ~ '.' ~ table_name %}
          
          {{ log("Creating metric view: " ~ catalog ~ "." ~ schema ~ "." ~ view_name, info=True) }}
          
          {# Determine YAML content #}
          {% if mv.yaml is defined %}
            {# ===== RAW YAML MODE ===== #}
            {# User provides full Databricks YAML - replace __SOURCE__ placeholder #}
            {% set yaml_content = mv.yaml | replace('__SOURCE__', source_ref) %}
          {% else %}
            {# ===== STRUCTURED MODE ===== #}
            {# Build YAML from structured properties (basic support) #}
            {% set version = mv.get('version', '0.1') %}
            {% set yaml_lines = [] %}
            {% do yaml_lines.append('version: ' ~ version) %}
            {% do yaml_lines.append('source: ' ~ source_ref) %}
            
            {# Add filter if defined #}
            {% if mv.filter is defined %}
              {% do yaml_lines.append('filter: ' ~ mv.filter) %}
            {% endif %}
            
            {# Add dimensions #}
            {% if mv.dimensions is defined and mv.dimensions | length > 0 %}
              {% do yaml_lines.append('dimensions:') %}
              {% for dim in mv.dimensions %}
                {% do yaml_lines.append('  - name: ' ~ dim.name) %}
                {% do yaml_lines.append('    expr: ' ~ (dim.expr if dim.expr is defined else dim.name)) %}
              {% endfor %}
            {% endif %}
            
            {# Add measures #}
            {% if mv.measures is defined and mv.measures | length > 0 %}
              {% do yaml_lines.append('measures:') %}
              {% for meas in mv.measures %}
                {% do yaml_lines.append('  - name: ' ~ meas.name) %}
                {% do yaml_lines.append('    expr: ' ~ meas.expr) %}
              {% endfor %}
            {% endif %}
            
            {% set yaml_content = yaml_lines | join('\n') %}
          {% endif %}
          
          {# Use CREATE OR REPLACE to preserve materialization state #}
          {# This is incremental-friendly - doesn't destroy existing materialized view caches #}
          {% set create_sql %}
CREATE OR REPLACE VIEW {{ catalog }}.{{ schema }}.{{ view_name }}
{% if description %}COMMENT '{{ description }}'{% endif %}
WITH METRICS
LANGUAGE YAML
AS $$
{{ yaml_content }}
$$
          {% endset %}
          
          {{ log("Executing CREATE OR REPLACE METRIC VIEW...", info=True) }}
          {% do run_query(create_sql) %}
          
          {{ log("Successfully created metric view: " ~ view_name, info=True) }}
          
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}
