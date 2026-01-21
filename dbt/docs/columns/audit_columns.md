{% docs column_audit_loaded_at %}

Batch or system date (YYYYMMDD formated as integer) indicating when the record was loaded into the data warehouse.

Used for lineage tracking and freshness validation.

{% enddocs %}


{% docs column_audit_created_at %}

Timestamp indicating when this model record was created.

Primarily used for auditing and debugging downstream transformations.

{% enddocs %}