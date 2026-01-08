{{ config(
    tags=['marts', 'growth', 'content']
) }}

with linkedin_daily as (
    select
        posted_date::date as activity_date,
        count(*) as linkedin_posts_published
    from {{ ref('fct_linkedin_posts') }}
    group by 1
),

substack_daily as (
    select
        (post_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin')::date as activity_date,
        count(*) as substack_posts_published
    from {{ ref('dim_substack__posts') }}
    where is_published = true
    group by 1
),

final as (
    select
        coalesce(l.activity_date, s.activity_date)::date as date_berlin,
        extract(dow from coalesce(l.activity_date, s.activity_date)) as day_of_week,
        extract(day from coalesce(l.activity_date, s.activity_date)) as day_of_month,
        extract(month from coalesce(l.activity_date, s.activity_date)) as month,
        extract(year from coalesce(l.activity_date, s.activity_date)) as year,
        (extract(dow from coalesce(l.activity_date, s.activity_date)) in (0, 6)) as is_weekend,
        coalesce(l.linkedin_posts_published, 0) as linkedin_posts_published,
        coalesce(s.substack_posts_published, 0) as substack_posts_published
    from linkedin_daily l
    full outer join substack_daily s on l.activity_date = s.activity_date
)

select * from final
order by date_berlin desc
