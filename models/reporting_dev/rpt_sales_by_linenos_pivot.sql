{{config(materialized='view', schema='reporting_dev')}}

{# array input
{% set v_linenumbers = [1,2,3] %}
#}
{% set v_linenumbers = get_line_numbers() %}


select
orderid,
{% for linenum in v_linenumbers  %}
sum(case when lineno = {{linenum}} then linesalesamount end) as lineno{{linenum}}_sales,
{% endfor %}
sum(linesalesamount) as total_sales
from {{ref("fct_orders")}}
group by 1

{#
select
orderid,
sum(case when lineno = 1 then linesalesamount end) as lineno1_sales,
sum(case when lineno = 2 then linesalesamount end) as lineno2_sales,
sum(case when lineno = 3 then linesalesamount end) as lineno3_sales,
sum(linesalesamount) as total_sales
from QWT_DEV.STAGING_DEV_DATAMARTS_DEV.FCT_ORDERS
group by 1;
#}