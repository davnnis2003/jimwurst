-- stg_spotify__tracks.sql
with source as (
    select * from {{ source('spotify', 'streaming_history_audio_2015_2017_0') }}
    union all
    select * from {{ source('spotify', 'streaming_history_audio_2017_1') }}
    union all
    select * from {{ source('spotify', 'streaming_history_audio_2017_2018_2') }}
    union all
    select * from {{ source('spotify', 'streaming_history_audio_2018_3') }}
    union all
    select * from {{ source('spotify', 'streaming_history_audio_2018_4') }}
    union all
    select * from {{ source('spotify', 'streaming_history_audio_2018_2019_5') }}
    union all
    select * from {{ source('spotify', 'streaming_history_audio_2019_2020_6') }}
    union all
    select * from {{ source('spotify', 'streaming_history_audio_2020_7') }}
    union all
    select * from {{ source('spotify', 'streaming_history_audio_2020_2021_8') }}
    union all
    select * from {{ source('spotify', 'streaming_history_audio_2021_9') }}
    union all
    select * from {{ source('spotify', 'streaming_history_audio_2021_2022_10') }}
    union all
    select * from {{ source('spotify', 'streaming_history_audio_2022_11') }}
    union all
    select * from {{ source('spotify', 'streaming_history_audio_2022_2023_12') }}
    union all
    select * from {{ source('spotify', 'streaming_history_audio_2023_13') }}
    union all
    select * from {{ source('spotify', 'streaming_history_audio_2023_2024_14') }}
    union all
    select * from {{ source('spotify', 'streaming_history_audio_2024_15') }}
    union all
    select * from {{ source('spotify', 'streaming_history_audio_2024_2025_16') }}
    union all
    select * from {{ source('spotify', 'streaming_history_audio_2025_17') }}
),
renamed as (
    select
        ts as played_at,
        platform,
        ms_played,
        conn_country,
        ip_addr,
        master_metadata_track_name as track_name,
        master_metadata_album_artist_name as artist_name,
        master_metadata_album_album_name as album_name,
        spotify_track_uri,
        episode_name,
        episode_show_name,
        spotify_episode_uri,
        reason_start,
        reason_end,
        shuffle,
        skipped,
        offline,
        offline_timestamp,
        incognito_mode,
        audiobook_title,
        audiobook_chapter_title,
        audiobook_uri,
        audiobook_chapter_uri
    from source
)
select * from renamed