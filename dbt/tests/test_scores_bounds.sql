-- Test: Asset rating scores are within [0, 1]
--
-- Purpose:
--   Ensures that all normalized asset rating scores remain within
--   the expected unit interval [0, 1].
--
-- Business context:
--   All strength scores in fct_asset_ratings are designed to be
--   normalized probabilities or percentile-like measures.
--   Values outside this range indicate:
--     - broken normalization logic
--     - upstream data drift
--     - incorrect scaling or joins
--
-- Severity: error

select *
from {{ ref('fct_asset_ratings') }}
where
    rt_value_strength_score < 0 or rt_value_strength_score > 1
 or rt_technical_strength_score < 0 or rt_technical_strength_score > 1
 or rt_fundamental_strength_score < 0 or rt_fundamental_strength_score > 1
 or rt_momentum_strength_score < 0 or rt_momentum_strength_score > 1
 or rt_short_squeeze_score < 0 or rt_short_squeeze_score > 1
 or rt_overall_score < 0 or rt_overall_score > 1