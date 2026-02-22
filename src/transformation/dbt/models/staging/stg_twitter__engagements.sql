-- models/staging/stg_twitter__engagements.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Staging model for Twitter engagement events.
-- ─────────────────────────────────────────────────────────────────────────────

with source as (

    select * from {{ source('silver', 'engagements') }}

),

renamed as (

    select
        engagement_id,
        post_id,
        engaging_user_id,
        post_author_id,
        platform,
        engagement_type,
        content                                     as engagement_content,
        coalesce(engagement_value, 1.0)             as engagement_value,
        created_at                                  as engaged_at,
        ingested_at,
        silver_batch_id

    from source

)

select * from renamed
