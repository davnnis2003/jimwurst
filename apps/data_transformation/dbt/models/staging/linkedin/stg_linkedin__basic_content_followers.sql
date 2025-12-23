{{ config(materialized='view', enabled=var('enable_linkedin_models', false), tags=['staging', 'linkedin', 'basic_content']) }}

{% set rel = adapter.get_relation(database=target.database, schema='s_linkedin', identifier='basic_content_2025_12_17_2025_12_23_jimmypang_followers') %}
{% if not rel %}
  {{ exceptions.warn("s_linkedin.basic_content_..._followers not found; skipping stg_linkedin__basic_content_followers.") }}
  select null::text as notice where false
{% else %}

select * from {{ source('linkedin', 'basic_content_2025_12_17_2025_12_23_jimmypang_followers') }}

{% endif %}

