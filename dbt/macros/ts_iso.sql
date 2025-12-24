{% macro ts_iso(field) %}
try_to_timestamp({{ field }}, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
try_to_timestamp({{ field }}, "yyyy-MM-dd'T'HH:mm:ss"),
try_to_timestamp({{ field }}, "yyyy-MM-dd HH:mm:ss"),
try_to_timestamp({{ field }}, "yyyy-MM-dd")
{% endmacro %}
