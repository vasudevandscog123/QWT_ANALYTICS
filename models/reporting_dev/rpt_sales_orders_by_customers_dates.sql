{{config(materialzed='view', schema=env_var('DBT_REPORTING_SCHEMA', 'REPORTING_DEV'), pre_hook="use warehouse compute_wh;", post_hook="create or replace view env_var('DBT_QWT_DB', 'QWT_DEV').env_var('DBT_REPORTING_SCHEMA', 'REPORTING_DEV').rpt_sales_orders_by_customers_dates_TEST_POST_HOOK as select * from  reporting_dev.rpt_sales_orders_by_customers_dates;")}}


with 
orders 
as (select customerid, min(orderdate) as firstorderdate, max(orderdate) recentorderdate, count(distinct orderid) total_orders, sum(quantity) as totalquantity, sum(linesalesamount) as totalsales  
from {{ref("fct_orders")}} o
group by customerid
)
,
customers
as ( select customerid, companyname, contactname from {{ref("dim_customers")}})
,
daydate
as ( select DATE_DAY, DAY_OF_WEEK_NAME from {{ref("dim_date")}})

select 
companyname, contactname, 
firstorderdate, first.DAY_OF_WEEK_NAME as firstorderday, 
recentorderdate, recent.DAY_OF_WEEK_NAME as recentorderday, 
total_orders, totalquantity, totalsales
from orders as o
inner join customers as c 
on c.customerid=o.customerid
inner join daydate as first 
on first.DATE_DAY=o.firstorderdate
inner join daydate recent 
on recent.DATE_DAY=o.recentorderdate
