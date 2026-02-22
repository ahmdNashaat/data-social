-- models/marts/facts/fact_post_metrics.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Fact table: daily snapshot of post performance metrics.
-- Grain: one row per (post_id, platform, date).
--
-- Used for: content performance dashboards, trend analysis,
--           top-performing posts rankings.
-- ─────────────────────────────────────────────────────────────────────────────

{{
    config(
        materialized = 'table',
        tags         = ['facts', 'gold', 'daily'],
        description  = 'Daily performance snapshot per post across all platforms.'
    )
}}

with posts as (

    select * from {{ ref('int_posts_enriched') }}

),

-- Daily aggregation of engagement events per post
daily_engagements as (

    select
        post_id,
        platform,
        cast(engagement_timestamp as date)  as metric_date,

        count(*)                            as total_engagements,
        count(case when engagement_type = 'LIKE'    then 1 end) as likes,
        count(case when engagement_type = 'RETWEET' then 1 end) as retweets,
        count(case when engagement_type = 'REPLY'   then 1 end) as replies,
        count(case when engagement_type = 'QUOTE'   then 1 end) as quotes,
        count(case when engagement_type = 'VIEW'    then 1 end) as views,

        sum(engagement_value)               as total_engagement_value

    from {{ ref('fact_engagements') }}
    group by 1, 2, 3

),

combined as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['p.post_id', 'p.platform', 'p.post_date']) }}
                                                    as metric_sk,

        -- Keys
        p.post_id,
        p.user_id,
        p.platform,

        -- Date dimension key
        cast(to_char(p.post_date, 'YYYYMMDD') as integer)
                                                    as date_sk,

        -- Metrics from source (snapshot at ingestion)
        p.likes_count                               as snapshot_likes,
        p.shares_count                              as snapshot_shares,
        p.comments_count                            as snapshot_comments,
        p.impressions_count                         as snapshot_impressions,

        -- Aggregated from engagement events
        coalesce(de.likes,    0)                    as event_likes,
        coalesce(de.retweets, 0)                    as event_shares,
        coalesce(de.replies,  0)                    as event_replies,
        coalesce(de.quotes,   0)                    as event_quotes,
        coalesce(de.views,    0)                    as event_views,
        coalesce(de.total_engagements, 0)           as total_engagement_events,
        coalesce(de.total_engagement_value, 0)      as total_engagement_value,

        -- Derived
        p.engagement_rate_pct,
        p.engagement_tier,
        p.total_engagement,

        -- Context
        p.post_date,
        p.post_year,
        p.post_month,
        p.post_day,
        p.post_day_of_week,
        p.posted_on_weekend,
        p.has_hashtags,

        -- Timestamps
        p.posted_at,
        p.ingested_at,
        current_timestamp                           as gold_loaded_at

    from posts p
    left join daily_engagements de
        on  p.post_id   = de.post_id
        and p.platform  = de.platform
        and p.post_date = de.metric_date

)

select * from combined
