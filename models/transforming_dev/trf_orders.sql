{{config(materialized = 'table',schema=env_var('DBT_TRANSFORMING_SCHEMA', 'TRANSFORMING_DEV'))}}
select 
o.orderid,
od.lineno,
o.customerid,
o.employeeid,
o.shipperid,
od.productid,
o.freight,
od.unitprice,
od.quantity,
od.discount,
o.orderdate,
round((od.unitprice * od.quantity) *(1-od.discount),2) as linesalesamount,
round(p.unitcost * od.quantity, 2) as costofgoodssold,
round(((od.unitprice * od.quantity) * (1-od.discount)) - (p.unitcost * od.quantity),2) as margin
from
{{ref("stg_orders")}} as o
inner join {{ref("stg_orderdetails")}} as od
on o.orderid = od.orderid
inner join {{ref("stg_products")}} as p
on p.productid = od.productid
