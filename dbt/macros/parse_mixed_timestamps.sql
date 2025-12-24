{% macro parse_mixed_timestamps(field) %}
coalesce(
  {{ ts_iso(field) }},
  {{ ts_slash(field) }}
)
{% endmacro %}
