WITH delivers AS (
    SELECT
        post_id,
        delivered_at AS event_at,
        email,
        post_type,
        post_audience,
        is_active_subscription,
        'delivery' AS event_type,
        NULL AS country,
        NULL AS city,
        NULL AS region,
        NULL AS device_type,
        NULL AS client_os,
        NULL AS client_type,
        NULL AS user_agent,
        _source_folder
    FROM {{ ref('stg_substack__post_delivers') }}
),

opens AS (
    SELECT
        post_id,
        opened_at AS event_at,
        email,
        post_type,
        post_audience,
        is_active_subscription,
        'open' AS event_type,
        country,
        city,
        region,
        device_type,
        client_os,
        client_type,
        user_agent,
        _source_folder
    FROM {{ ref('stg_substack__post_opens') }}
),

unified AS (
    SELECT * FROM delivers
    UNION ALL
    SELECT * FROM opens
),

deduplicated AS (
    -- Deduplicate overlapping events across different export folders
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY post_id, email, event_at, event_type 
            ORDER BY _source_folder DESC -- Heuristic: take newest export folder
        ) as row_num
    FROM unified
)

SELECT
    md5(
        coalesce(post_id, '') || '|' || 
        coalesce(email, '') || '|' || 
        coalesce(event_at::text, '') || '|' || 
        coalesce(event_type, '')
    ) AS event_key,
    post_id,
    event_at,
    email,
    post_type,
    post_audience,
    is_active_subscription,
    event_type,
    country,
    city,
    region,
    device_type,
    client_os,
    client_type,
    user_agent,
    _source_folder
FROM deduplicated
WHERE row_num = 1
