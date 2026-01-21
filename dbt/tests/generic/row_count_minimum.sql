-- Documentation path:
-- docs/tests/row_count_minimum.md

{% test row_count_minimum(model, min_rows) %}

select
    count(*) as row_count
from {{ model }}
having count(*) < {{ min_rows }}

{% endtest %}