{{config(materialized='table')}}
select 
orderid,
lineno,
shipperid,
customerid,
productid,
employeeid,
cast(replace(shipmentdate,' 0:00', '') as date) as shipmentdate,
status  
from {{source("qwt_raw", "raw_shipments")}}
