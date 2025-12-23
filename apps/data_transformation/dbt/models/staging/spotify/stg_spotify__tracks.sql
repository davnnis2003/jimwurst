-- stg_spotify__tracks.sql
with source as (
    select * from {{ source('spotify', 'tracks') }}
),
renamed as (
    select
        id as track_id,
        name as track_name,
        duration_ms / 1000.0 as duration_seconds,
        case
            when popularity > 80 then 'high'
            else 'low'
        end as popularity_category
    from source
)
select * from renamed