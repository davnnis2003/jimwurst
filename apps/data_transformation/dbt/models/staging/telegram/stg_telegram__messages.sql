-- stg_telegram__messages.sql
with source as (
    select * from {{ source('telegram', 'messages') }}
),
renamed as (
    select
        *  -- Add specific column renames as needed
    from source
)
select * from renamed