-- generate_schema_name.sql
-- Override dbt's default schema name generation.
--
-- By default, dbt concatenates the target schema (from profiles.yml) with
-- the custom schema name defined in dbt_project.yml, producing names like
-- "public_marts" or "public_staging".
--
-- This macro uses ONLY the custom schema name when one is provided, so
-- the tables are created directly in `staging` and `marts` schemas.
-- When no custom schema is provided, it falls back to the target schema.

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
