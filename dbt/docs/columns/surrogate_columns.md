{% docs column_sk_asset_ticker %}

Surrogate key uniquely identifying an asset ticker.

Generated using a deterministic hash of the ticker symbol to ensure:
- Consistent joins
- Warehouse-safe keys
- Source-agnostic identity

{% enddocs %}