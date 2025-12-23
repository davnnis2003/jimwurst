-- spotify_tracks.sql
select
    played_at,
    track_name,
    artist_name,
    album_name,
    (ms_played::numeric / 1000.0) as seconds_played,
    platform,
    conn_country,
    reason_start,
    reason_end,
    shuffle,
    skipped,
    offline
from {{ ref('stg_spotify__tracks') }}
where track_name is not null  -- Filter out non-track entries like podcasts