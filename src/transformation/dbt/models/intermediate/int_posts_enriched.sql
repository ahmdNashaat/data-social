-- models/intermediate/int_posts_enriched.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Enriches the unified posts with:
--   - Date dimension keys
--   - Engagement rate calculation
--   - Content classification
--   - Hashtag extraction
-- ─────────────────────────────────────────────────────────────────────────────

with posts as (

    select * from {{ ref('stg_all__posts') }}

),

enriched as (

    select
        -- Keys
        post_id,
        user_id,
        platform,

        -- Content
        content,
        hashtags,

        -- Date parts (for joining to dim_date)
        post_date,
        extract(year  from posted_at)   as post_year,
        extract(month from posted_at)   as post_month,
        extract(day   from posted_at)   as post_day,
        extract(dow   from posted_at)   as post_day_of_week,   -- 0=Sunday
        extract(hour  from posted_at)   as post_hour,

        -- Is weekend?
        case
            when extract(dow from posted_at) in (0, 6) then true
            else false
        end                             as posted_on_weekend,

        -- Engagement metrics
        likes_count,
        shares_count,
        comments_count,
        impressions_count,

        -- Total engagement
        likes_count + shares_count + comments_count
                                        as total_engagement,

        -- Engagement rate (avoid div/0)
        case
            when impressions_count > 0
            then round(
                (likes_count + shares_count + comments_count)::decimal
                / impressions_count * 100,
                4
            )
            else 0
        end                             as engagement_rate_pct,

        -- Content tier by engagement
        case
            when likes_count + shares_count + comments_count >= 10000 then 'viral'
            when likes_count + shares_count + comments_count >= 1000  then 'high'
            when likes_count + shares_count + comments_count >= 100   then 'medium'
            else 'low'
        end                             as engagement_tier,

        -- Has hashtags?
        case
            when hashtags is not null and array_length(hashtags, 1) > 0
            then true
            else false
        end                             as has_hashtags,

        -- Timestamps
        posted_at,
        ingested_at,
        silver_batch_id

    from posts

)

select * from enriched
