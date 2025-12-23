{{ config(materialized='view', enabled=var('enable_linkedin_models', false), tags=['staging', 'linkedin']) }}

{% set rel = adapter.get_relation(database=target.database, schema='s_linkedin', identifier='complete_messages') %}
{% if not rel %}
  {{ exceptions.warn("s_linkedin.complete_messages not found; skipping stg_linkedin__messages.") }}
  select null::text as notice where false
{% else %}

with source as (
    select * from {{ source('linkedin', 'complete_messages') }}
),
renamed as (
    select
        "CONVERSATION NAME" as conversation_name,
        "FROM" as sender,
        "TO" as recipient,
        "CONTENT" as body,
        cast("DATE" as timestamp) as sent_at
    from source
)

select * from renamed

{% endif %}

