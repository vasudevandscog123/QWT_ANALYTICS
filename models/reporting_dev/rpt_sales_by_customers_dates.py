import snowflake.snowpark.functions as F
import pandas as pd
import holidays

def is_holiday(dt):
    us_holidays =  holidays.US()
    is_holiday = dt in us_holidays
    return is_holiday

def average_order(x, y):
    return x/y

def model(dbt, session):
    dbt.config(materialized='table')
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

    df_customer_orders = df_grouped_orders.join(df_customers, df_grouped_orders.customerid==df_customers.customerid, "left")
    df_customer_orders = df_customer_orders.select(
        F.col("companyname"),
        F.col("contactname"),
        F.col("firstorderdate"),
        F.col("recentorderdate"),
        F.col("totalorders"),
        F.col("totalquantity"),
        F.col("totalsales"),
        F.col("avgmargin")

    )
    df_customer_orders = df_customer_orders.withColumn("averageordervalue", average_order(df_customer_orders["totalsales"], df_customer_orders["totalorders"])
    )

    df_customer_orders_date_first = df_customer_orders.join(
        df_date,
        df_date.DATE_DAY==df_customer_orders.firstorderdate,
        "left"
    )
    df_customer_orders_date_first = df_customer_orders_date_first.select(
        F.col("companyname"),
        F.col("contactname"),
        F.col("firstorderdate"),
        F.col("DAY_OF_WEEK_NAME").alias("firstorderday"),
        F.col("recentorderdate"),
        F.col("totalorders"),
        F.col("totalquantity"),
        F.col("totalsales"),
        F.col("avgmargin"),
        F.col("averageordervalue")
    )

    df_customer_orders_date = df_customer_orders_date_first.join(
        df_date,
        df_date.DATE_DAY==df_customer_orders.recentorderdate,
        "left"
    )
    df_customer_orders_date = df_customer_orders_date.select(
        F.col("companyname"),
        F.col("contactname"),
        F.col("firstorderdate"),
        F.col("firstorderday"),
        F.col("recentorderdate"),
        F.col("DAY_OF_WEEK_NAME").alias("recentorderday"),
        F.col("totalorders"),
        F.col("totalquantity"),
        F.col("totalsales"),
        F.col("avgmargin"),
        F.col("averageordervalue")
    )

    df_customer_orders_date = df_customer_orders_date.filter(F.col("firstorderdate").isNotNull())
    df_customer_orders_date_holidays = df_customer_orders_date.to_pandas()
    df_customer_orders_date_holidays["isfirtorderdateholiday"] =  df_customer_orders_date_holidays["FIRSTORDERDATE"].apply(is_holiday)

    return df_customer_orders_date_holidays
