WITH source AS (
    SELECT * FROM {{ source('substack', 'posts') }}
),

renamed AS (
    SELECT
        post_id,
        {{ dbt.safe_cast("post_date", api.Column.translate_type("timestamp")) }} AS post_at,
        is_published::boolean AS is_published,
        {{ dbt.safe_cast("email_sent_at", api.Column.translate_type("timestamp")) }} AS email_sent_at,
        {{ dbt.safe_cast("inbox_sent_at", api.Column.translate_type("timestamp")) }} AS inbox_sent_at,
        type AS post_type,
        audience AS post_audience,
        title,
        subtitle,
        podcast_url,
        _source_folder
    FROM source
),

deduplicated AS (
    -- Handle duplicates across multiple exports by taking the latest one (or just one)
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY post_at DESC NULLS LAST) as row_num
    FROM renamed
)

SELECT 
    * EXCLUDE (row_num)
FROM deduplicated
WHERE row_num = 1
