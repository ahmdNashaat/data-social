-- models/marts/facts/fact_engagements.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Fact table: one row per engagement event.
-- Grain: one engagement (like / retweet / reply / quote / view) per post per user.
--
-- This is the primary fact table for engagement analysis.
-- ─────────────────────────────────────────────────────────────────────────────

{{
    config(
        materialized = 'table',
        tags         = ['facts', 'gold', 'daily'],
        description  = 'One row per engagement event across all platforms.'
    )
}}

with engagements as (

    select * from {{ ref('stg_twitter__engagements') }}

),

posts as (

    select
        post_id,
        user_id         as author_user_id,
        platform,
        post_date,
        post_year,
        post_month,
        post_day
    from {{ ref('int_posts_enriched') }}

),

-- Build date surrogate key (YYYYMMDD integer format)
joined as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['e.engagement_id']) }}
                                            as engagement_sk,

        -- Natural keys
        e.engagement_id,
        e.post_id,
        e.engaging_user_id,
        e.post_author_id,
        e.platform,

        -- Date dimension key
        cast(
            to_char(e.engaged_at::date, 'YYYYMMDD')
            as integer
        )                                   as date_sk,

        -- Engagement details
        e.engagement_type,
        e.engaged_at                        as engagement_timestamp,
        e.engagement_value,
        e.engagement_content,

        -- Post context (denormalized for query performance)
        p.post_date,

        -- Pipeline metadata
        e.ingested_at,
        e.silver_batch_id,
        current_timestamp                   as gold_loaded_at

    from engagements e
    left join posts p
        on  e.post_id  = p.post_id
        and e.platform = p.platform

)

select * from joined
