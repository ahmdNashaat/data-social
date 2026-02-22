-- models/staging/stg_twitter__tweets.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Staging model for Twitter tweets.
-- 1:1 mapping from Silver source with light renaming and type casting.
-- No business logic here — that belongs in intermediate models.
-- ─────────────────────────────────────────────────────────────────────────────

with source as (

    select * from {{ source('silver', 'tweets') }}

),

renamed as (

    select
        -- Identifiers
        tweet_id                                    as post_id,
        user_id,
        platform,

        -- Content
        content,
        hashtags,
        mentions,
        language,
        coalesce(is_retweet, false)                 as is_retweet,
        coalesce(is_reply, false)                   as is_reply,
        media_type,
        coalesce(content_length, 0)                 as content_length,
        coalesce(hashtag_count, 0)                  as hashtag_count,
        coalesce(has_media, false)                  as has_media,

        -- Engagement metrics
        coalesce(likes_count, 0)                    as likes_count,
        coalesce(retweets_count, 0)                 as retweets_count,
        coalesce(replies_count, 0)                  as replies_count,
        coalesce(impressions_count, 0)              as impressions_count,
        coalesce(quote_count, 0)                    as quote_count,
        coalesce(engagement_total, 0)               as engagement_total,

        -- Author snapshot
        coalesce(user_follower_count, 0)            as author_follower_count,
        user_account_type                           as author_account_type,
        coalesce(user_is_verified, false)           as author_is_verified,

        -- Timestamps
        created_at                                  as posted_at,
        ingested_at,

        -- Pipeline metadata
        silver_batch_id,
        silver_processed_at

    from source

)

select * from renamed
