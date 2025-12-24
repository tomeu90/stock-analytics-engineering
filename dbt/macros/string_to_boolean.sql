{% macro string_to_boolean(field) %}
    case
        when lower({{ field }}) = 'yes' then true
        when lower({{ field }}) = 'no' then false
        else null
    end
{% endmacro %}
