-- macros/get_engagement_weight.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Returns the engagement weight for a given engagement type.
-- Used to calculate weighted engagement scores consistently across models.
-- ─────────────────────────────────────────────────────────────────────────────

{% macro get_engagement_weight(engagement_type_col) %}
    case {{ engagement_type_col }}
        when 'LIKE'    then 1.0
        when 'REPLY'   then 2.0
        when 'RETWEET' then 3.0
        when 'QUOTE'   then 4.0
        when 'VIEW'    then 0.1
        else 1.0
    end
{% endmacro %}


-- macros/safe_divide.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Safe division that returns 0 instead of NULL when denominator is 0.
-- ─────────────────────────────────────────────────────────────────────────────

{% macro safe_divide(numerator, denominator) %}
    case
        when {{ denominator }} = 0 or {{ denominator }} is null
        then 0
        else {{ numerator }}::decimal / {{ denominator }}
    end
{% endmacro %}


-- macros/date_to_sk.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Converts a date to a YYYYMMDD integer surrogate key.
-- ─────────────────────────────────────────────────────────────────────────────

{% macro date_to_sk(date_col) %}
    cast(strftime({{ date_col }}, '%Y%m%d') as integer)
{% endmacro %}
