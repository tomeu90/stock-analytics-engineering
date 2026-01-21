{% macro ts_slash(field) %}
try_to_timestamp({{ field }}, "M/d/yyyy HH:mm:ss"),
try_to_timestamp({{ field }}, "M/d/yyyy"),
try_to_timestamp({{ field }}, "d/M/yyyy HH:mm:ss"),
try_to_timestamp({{ field }}, "d/M/yyyy")
{% endmacro %}
