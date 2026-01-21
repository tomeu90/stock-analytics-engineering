-- Test: Overall asset rating score consistency
--
-- Purpose:
--   Validates that the overall asset rating score is calculated
--   exactly as the weighted combination of its component scores.
--
-- Business context:
--   The overall score is the primary ranking signal used downstream
--   for asset comparison and screening. Any deviation from the
--   documented formula would invalidate analytical results.
--   Differences greater than 0.0001 are considered invalid.
--
-- Severity: error

select *
from {{ ref('fct_asset_ratings') }}
where abs(
    rt_overall_score -
    round(
        (0.5 * rt_value_strength_score) +
        (0.3 * rt_technical_strength_score) +
        (0.08 * rt_fundamental_strength_score) +
        (0.10 * rt_momentum_strength_score) +
        (0.02 * rt_sentiment_strength_score)
        ,
        4
    )
) > 0.0001