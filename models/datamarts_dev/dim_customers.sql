{{config(materialized='view', schema='datamarts_dev') }}
select * from {{ref("trf_customers")}}