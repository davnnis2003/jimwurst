-- stg_linkedin__connections.sql
with source as (
    select * from {{ source('linkedin', 'connections') }}
),
renamed as (
    select
        *  -- Add specific column renames as needed
    from source
)
select * from renamed