{% docs test_grain_unique %}

Ensures that a model adheres to its declared **data grain** by enforcing
uniqueness across a specified set of columns.

### What this test validates
- No duplicate rows exist for the given grain
- Joins have not caused row explosions
- Upstream models have not silently changed granularity

### Typical use cases
- Fact tables (e.g. one row per entity per day)
- Snapshot-like models
- Metrics aggregated at a known dimensional level

### Example
```yaml
tests:
  - grain_unique:
      columns: ['sk_asset_ticker', 'audit_loaded_at']
```
{% enddocs %}