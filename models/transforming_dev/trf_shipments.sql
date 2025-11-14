{{config(materialized='table', schema='transforming_dev')}}
select ship.orderid, ship.lineno, ship.shipmentdate, ship.status, lkp.companyname as shipmentcompany
from
{{ref("shipments_snapshot")}} as ship
join {{ref("lkp_shippers")}} as lkp
on ship.shipperid = lkp.shipperid
where ship.dbt_valid_to is null
