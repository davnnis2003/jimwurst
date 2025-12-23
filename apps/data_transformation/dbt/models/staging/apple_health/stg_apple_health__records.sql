-- stg_apple_health__records.sql
with source as (
    select * from {{ source('apple_health', 'records') }}
),
renamed as (
    select
        *  -- Add specific column renames as needed based on actual data
    from source
)
select * from renamed