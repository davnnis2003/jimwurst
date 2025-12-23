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
        conversation_title as conversation_name,
        "from" as sender,
        sender_profile_url,
        "to" as recipient,
        recipient_profile_urls,
        content as body,
        cast("date" as timestamp) as sent_at,
        subject,
        folder,
        attachments
    from source
)

select * from renamed

{% endif %}

