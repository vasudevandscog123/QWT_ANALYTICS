import snowflake.snowpark.functions as F

def average_order(x, y):
    return x/y

def model(dbt, session):
    dbt.config(materialized='table', schema= "reporting_dev")
    df_orders = dbt.ref("fct_orders")
    df_customers = dbt.ref("dim_customers")
    df_date = dbt.ref("dim_date")
    
    df_grouped_orders = df_orders.groupBy("customerid").agg(
        F.min("orderdate").alias("firstorderdate"),
        F.max("orderdate").alias("recentorderdate"),
        F.countDistinct("orderid").alias("totalorders"),
        F.sum("quantity").alias("totalquantity"),
        F.sum("linesalesamount").alias("totalsales"),
        F.avg("margin").alias("avgmargin")
    )

    df_customer_orders = (
        df_grouped_orders
        .join(df_customers, df_grouped_orders.customerid==df_customers.customerid, "left")
        .select(
            df_customers.customerid.alias("customerid"),
            df_customers.companyname.alias("companyname"),
            df_customers.contactname.alias("contactname"),
            df_grouped_orders.customerid.alias("firstorderdate"),
            df_grouped_orders.customerid.alias("recentorderdate"),
            df_grouped_orders.customerid.alias("totalorders"),
            df_grouped_orders.customerid.alias("totalquantity"),
            df_grouped_orders.customerid.alias("totalsales"),
            df_grouped_orders.customerid.alias("avgmargin")

        )
    )

    df_customer_orders = df_customer_orders.withColumn("averageordervalue", average_order(df_customer_orders["totalsales"], df_customer_orders["totalorders"])
    )

    return df_customer_orders

