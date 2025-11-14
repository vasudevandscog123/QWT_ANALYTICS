{{ config(materialized="incremental", unique_key="orderid") }}

select orderid, orderdate, customerid, employeeid, shipperid, freight
from {{source("qwt_raw", "raw_orders")}}

{% if is_incremental() %}

where orderdate > (select max(orderdate) from {{this}})
{% endif %}