{#
  Override dbt's default schema name generation so that models land in
  BRONZE / SILVER / GOLD directly — not <target_schema>_BRONZE, etc.

  If no custom_schema_name is provided the model falls back to the
  target's default schema (e.g. PUBLIC).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | upper }}
    {%- endif -%}
{%- endmacro %}
