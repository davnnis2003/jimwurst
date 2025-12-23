{{ config(materialized='view', enabled=var('enable_linkedin_models', false), tags=['staging', 'linkedin']) }}

{% set rel = adapter.get_relation(database=target.database, schema='s_linkedin', identifier='complete_connections') %}
{% if not rel %}
  {{ exceptions.warn("s_linkedin.complete_connections not found; skipping stg_linkedin__connections.") }}
  select null::text as notice where false
{% else %}

with source as (
    select * from {{ source('linkedin', 'complete_connections') }}
),
renamed as (
    select
        concat_ws(' ', "First Name", "Last Name") as connection_name,
        "Company" as company,
        "Position" as position,
        "Email Address" as email_address,
        cast("Connected On" as date) as connected_on
    from source
)

select * from renamed

{% endif %}

