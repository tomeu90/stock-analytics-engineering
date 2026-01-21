-- Documentation path:
-- docs/tests/normalized_range.md

{% test normalized_range(model, column_name, min_value=0, max_value=1) %}

select *
from {{ model }}
where {{ column_name }} < {{ min_value }}
   or {{ column_name }} > {{ max_value }}

{% endtest %}