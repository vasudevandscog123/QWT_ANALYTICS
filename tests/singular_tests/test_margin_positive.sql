select 
orderid,
sum(margin) as totalmargin
from {{ref("fct_orders")}}
group by orderid having  totalmargin<=0