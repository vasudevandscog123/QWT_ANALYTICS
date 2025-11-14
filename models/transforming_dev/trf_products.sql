{{config(materialized='table', schema='transforming_dev')}}

select 
p.productid,
p.productname,
c.categoryname,
s.companyname as suppliercompany,
s.contactname as suppliercontact,
s.city as suppliercity,
s.country as suppliercountyr,
p.quantityperunit,
p.unitcost,
p.unitprice,
p.unitsinstock,
p.unitsonorder,
iff(p.unitsinstock>p.unitsonorder, 'ProductAvailable', 'ProductNonAvailable') as productavaialbility

from {{ref("stg_products")}} as p
join {{ref("stg_suppliers")}} as s on p.supplierid=s.supplierid
join {{ref("lkp_categories")}} as c on p.categoryid= c.categoryid
