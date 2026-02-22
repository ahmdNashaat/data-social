# Data Dictionary — Social Media Analytics Platform

> Auto-maintained alongside dbt model changes. Last updated: see git log.

## Gold Layer — Fact Tables

### `fact_engagements`
One row per engagement event (like, share, comment, retweet, view).

| Column | Type | Description | Nullable |
|--------|------|-------------|----------|
| `engagement_id` | VARCHAR(36) | Surrogate key — UUID | NO |
| `post_sk` | BIGINT | FK → dim_posts | NO |
| `user_sk` | BIGINT | FK → dim_users | NO |
| `platform_sk` | BIGINT | FK → dim_platforms | NO |
| `date_sk` | INTEGER | FK → dim_date (YYYYMMDD) | NO |
| `engagement_type` | VARCHAR(20) | LIKE, SHARE, COMMENT, RETWEET, VIEW | NO |
| `engagement_timestamp` | TIMESTAMP | UTC timestamp of event | NO |
| `engagement_value` | DECIMAL(10,2) | Weighted score | YES |
| `ingestion_timestamp` | TIMESTAMP | Load time | NO |
| `batch_id` | VARCHAR(36) | Pipeline run ID for lineage | NO |

### `fact_post_metrics`
Daily snapshot of aggregated post performance.

| Column | Type | Description | Nullable |
|--------|------|-------------|----------|
| `metric_sk` | BIGINT | Surrogate key | NO |
| `post_sk` | BIGINT | FK → dim_posts | NO |
| `user_sk` | BIGINT | FK → dim_users (author) | NO |
| `date_sk` | INTEGER | FK → dim_date | NO |
| `total_likes` | BIGINT | Cumulative likes at EOD | NO |
| `total_shares` | BIGINT | Cumulative shares at EOD | NO |
| `total_comments` | BIGINT | Cumulative comments at EOD | NO |
| `total_views` | BIGINT | Cumulative views at EOD | YES |
| `engagement_rate` | DECIMAL(8,4) | (likes+shares+comments)/impressions*100 | YES |
| `reach` | BIGINT | Unique accounts reached | YES |

---

## Gold Layer — Dimension Tables

### `dim_users`
Social media accounts — brands and general users. SCD Type 2.

| Column | Type | Description |
|--------|------|-------------|
| `user_sk` | BIGINT | Surrogate key |
| `user_id` | VARCHAR(36) | Natural key from source |
| `platform_id` | VARCHAR(10) | twitter, youtube, reddit |
| `username` | VARCHAR(100) | Handle/username |
| `display_name` | VARCHAR(200) | Display name |
| `follower_count` | BIGINT | Follower count at time of record |
| `account_type` | VARCHAR(20) | BRAND, INFLUENCER, USER |
| `is_brand` | BOOLEAN | True if managed brand account |
| `is_verified` | BOOLEAN | Platform-verified account |
| `valid_from` | DATE | SCD Type 2 — record start date |
| `valid_to` | DATE | SCD Type 2 — NULL means current |
| `is_current` | BOOLEAN | True if current record |

### `dim_date`
Standard date dimension. Pre-populated for 10 years.

| Column | Type | Description |
|--------|------|-------------|
| `date_sk` | INTEGER | Surrogate key (YYYYMMDD format) |
| `full_date` | DATE | Calendar date |
| `year` | SMALLINT | Calendar year |
| `quarter` | SMALLINT | Quarter (1-4) |
| `month` | SMALLINT | Month (1-12) |
| `month_name` | VARCHAR(10) | January, February, ... |
| `week_of_year` | SMALLINT | ISO week number |
| `day_of_week` | SMALLINT | 1=Monday, 7=Sunday |
| `day_name` | VARCHAR(10) | Monday, Tuesday, ... |
| `is_weekend` | BOOLEAN | True for Saturday/Sunday |
| `is_holiday` | BOOLEAN | Public holiday flag |

---

## Silver Layer — Cleaned Tables

### `silver.posts`
Deduplicated, standardized posts across all platforms.

| Column | Type | Source |
|--------|------|--------|
| `post_id` | VARCHAR(36) | Platform source ID |
| `platform` | VARCHAR(20) | twitter, youtube, reddit |
| `user_id` | VARCHAR(36) | Author ID |
| `content` | TEXT | Post content / title |
| `hashtags` | ARRAY(VARCHAR) | Extracted hashtags |
| `created_at_utc` | TIMESTAMP | Normalized to UTC |
| `ingested_at` | TIMESTAMP | Bronze ingestion time |
| `batch_id` | VARCHAR(36) | Pipeline run ID |

---

*For full column documentation, run `make dbt-docs` and open http://localhost:8085*
