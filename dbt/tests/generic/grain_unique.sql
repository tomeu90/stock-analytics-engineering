-- Documentation path:
-- docs/tests/grain_unique.md

{% test grain_unique(model, columns) %}

select
    {{ columns | join(', ') }},
    count(*) as row_count
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1

{% endtest %}