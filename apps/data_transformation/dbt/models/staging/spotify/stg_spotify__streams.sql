{{ config(materialized='view', enabled=var('enable_spotify_models', false), tags=['staging', 'spotify']) }}

{% set rel = adapter.get_relation(database=target.database, schema='s_spotify', identifier='streaminghistory0') %}
{% if not rel %}
  {{ exceptions.warn("s_spotify.streaminghistory0 not found; skipping stg_spotify__streams.") }}
  select null::text as notice where false
{% else %}

with source as (
    select * from {{ source('spotify', 'streaminghistory0') }}
),
renamed as (
    select
        cast(nullif(endtime, '') as timestamp) as played_at_utc,
        nullif(trackname, '') as track_name,
        nullif(artistname, '') as artist_name,
        msplayed / 1000.0 as seconds_played
    from source
)

select * from renamed

{% endif %}

