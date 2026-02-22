-- models/staging/stg_all__posts.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Staging model for the unified cross-platform posts table.
-- Covers Twitter, YouTube, and Reddit.
-- ─────────────────────────────────────────────────────────────────────────────

with source as (

    select * from {{ source('silver', 'posts') }}

),

renamed as (

    select
        post_id,
        user_id,
        platform,
        content,
        hashtags,
        posted_at,

        coalesce(likes_count, 0)        as likes_count,
        coalesce(shares_count, 0)       as shares_count,
        coalesce(comments_count, 0)     as comments_count,
        coalesce(impressions_count, 0)  as impressions_count,

        -- Derived
        cast(posted_at as date)         as post_date,

        ingested_at,
        silver_batch_id

    from source

)

select * from renamed
