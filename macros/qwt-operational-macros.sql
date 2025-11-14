{% macro macro_grant_select(role) %}

{% set vsql_setaccess %}
grant usage on database {{target.database}} to role {{role}};
grant usage on schema {{target.schema}} to role {{role}};
grant select on all tables in schema {{target.schema}} to role {{role}};
grant select on all views in schema {{target.schema}} to role {{role}};
{% endset %}

{% do run_query(vsql_setaccess) %}
{% do log ("Access Given", info=true) %}

{%- endmacro %}

{% macro change_warehouse_size(whname, whsize) %}

{% set vsql_wh_size %}
ALTER WAREHOUSE {{whname}} SET WAREHOUSE_SIZE = {{whsize}};
{% endset %}

{% do run_query(vsql_wh_size) %}
{% do log ("Warehouse size modified", info=true) %}

{%- endmacro %}

