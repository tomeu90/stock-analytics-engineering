{% docs fct_asset_ratings %}

### Asset Strength Scoring Model

This model produces **composite quantitative scores** for financial assets
(e.g. stocks), combining **value**, **technical**, **fundamental**, **momentum**,
and **news** signals into standardized metrics. Also, it contains an scores
that rates de potential risk of **short-squeezes**.

Each row represents **one asset ticker**, identified by a surrogate key.

#### Scoring methodology
All scores are calculated using **normalized real-time factors**, weighted
according to their perceived predictive importance, and rounded to 4 decimals.

The final output is intended for:
- Asset ranking
- Screening strategies
- Quantitative research
- Portfolio construction signals

{% enddocs %}
