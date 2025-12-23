-- stg_bolt__rides.sql
with source as (
    select * from {{ source('bolt', 'rides') }}
),
renamed as (
    select
        *  -- Add specific column renames as needed
    from source
)
select * from renamed