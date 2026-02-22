-- models/marts/dimensions/dim_posts.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Post dimension: one row per unique post across all platforms.
-- SCD Type 1 — overwrites on re-run.
-- ─────────────────────────────────────────────────────────────────────────────

{{
    config(
        materialized = 'table',
        tags         = ['dimensions', 'gold'],
        description  = 'One row per post (tweet, video, Reddit post) across all platforms.'
    )
}}

with posts as (

    select * from {{ ref('int_posts_enriched') }}

),

final as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['post_id', 'platform']) }}
                                            as post_sk,

        -- Natural keys
        post_id,
        user_id,
        platform,

        -- Content (first 500 chars preview)
        left(content, 500)                  as content_preview,
        content_length,
        hashtags,

        -- Engagement snapshot (at last ingestion)
        likes_count,
        shares_count,
        comments_count,
        impressions_count,
        total_engagement,
        engagement_rate_pct,
        engagement_tier,
        has_hashtags,
        post_day_of_week,
        posted_on_weekend,

        -- Date context
        post_date,
        post_year,
        post_month,
        post_day,
        post_hour,
        posted_at,

        -- Pipeline
        ingested_at,
        silver_batch_id,
        current_timestamp                   as gold_loaded_at

    from posts

)

select * from final
