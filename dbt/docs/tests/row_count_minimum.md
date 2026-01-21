{% docs test_row_count_minimum %}

Ensures that a model produces at least a **minimum expected number of rows**.

### What this protects against
- Empty or partially populated models
- Upstream source outages
- Overly restrictive filters
- Failed joins that eliminate rows

### When to use
- Fact tables
- Aggregated marts
- Any model where row count below a threshold is invalid

### Example
```yaml
tests:
  - row_count_minimum:
      min_rows: 100
```
{% enddocs %}