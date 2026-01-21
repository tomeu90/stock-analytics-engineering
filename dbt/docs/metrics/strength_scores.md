{% docs metric_value_strength_score %}

Composite **value investing signal** measuring how attractively priced an asset is
relative to its fundamentals.

Weighted factors include:
- Market capitalization
- Price-to-earnings ratios
- PEG ratio
- EV/EBITDA
- Free cash flow metrics
- Analyst recommendations

Higher values indicate stronger relative value.

{% enddocs %}


{% docs metric_technical_strength_score %}

Measures **price action and trend quality** using technical indicators.

Includes:
- Trend regime score
- Moving average distances
- RSI (14)
- MACD histogram
- Volatility and Bollinger Z-score

Higher scores indicate stronger technical momentum and trend confirmation.

{% enddocs %}


{% docs metric_short_squeeze_score %}

Estimates the **likelihood of short-squeeze dynamics**.

Incorporates:
- Short interest rate
- Days-to-cover (short ratio)
- Float size
- Relative trading volume

Higher values suggest increased squeeze risk.

{% enddocs %}


{% docs metric_short_squeeze_momentum_score %}

Hybrid score combining:
- Short squeeze pressure (70%)
- Technical momentum (30%)

Designed to capture **timing-sensitive squeeze setups**.

{% enddocs %}


{% docs metric_fundamental_strength_score %}

Evaluates overall **company financial health and growth quality**.

Includes profitability, efficiency, liquidity, leverage, growth, and earnings surprise signals.

Higher values indicate stronger long-term fundamentals.

{% enddocs %}


{% docs metric_momentum_strength_score %}

Normalized momentum indicator capturing **price acceleration trends** over recent periods.

Often used as a confirmation signal in multi-factor strategies.

{% enddocs %}


{% docs metric_sentiment_strength_score %}

Metric representing the **strength of sentiment** for an article or asset, 
calculated as:

*metric_sentiment_strength_score = compound_sentiment_score * sentiment_score_certainty*

This combines both the **direction and intensity of sentiment** (`compound_sentiment_score`) 
with the **confidence in that sentiment** (`sentiment_score_certainty`), 
producing a normalized indicator of sentiment impact.

Often used as a signal in sentiment-driven or multi-factor strategies.

{% enddocs %}


{% docs metric_overall_strength_score %}

Primary **composite ranking score** combining:

- Value (50%)
- Technicals (30%)
- Fundamentals (8%)
- Momentum (10%)
- Sentiment (2%)

Used as the final signal for **asset comparison and ranking**.

{% enddocs %}
