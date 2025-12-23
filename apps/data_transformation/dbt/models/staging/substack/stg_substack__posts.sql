-- stg_substack__posts.sql
with source as (
    select * from {{ source('substack', 'posts') }}
),
renamed as (
    select
        *  -- Add specific column renames as needed
    from source
)
select * from renamed