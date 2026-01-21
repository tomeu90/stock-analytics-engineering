{% docs test_normalized_range %}

Validates that a numeric column remains within an expected **normalized range**.

This test is commonly used for:
- Scores
- Ratios
- Percentile-like metrics
- Probabilities

### Why this matters
Values outside the expected range often indicate:
- Broken normalization logic
- Incorrect joins
- Upstream scaling changes
- Data drift

### Default behavior
- Minimum value: `0`
- Maximum value: `1`

### Example
```yaml
columns:
  - name: rt_overall_score
    tests:
      - normalized_range
```
{% enddocs %}
