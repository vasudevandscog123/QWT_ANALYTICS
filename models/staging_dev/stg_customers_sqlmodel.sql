{{ config(materialized='table', schema = env_var('DBT_STAGING_SCHEMA', 'STAGING_DEV')) }}

select * from {{source("qwt_raw","raw_customers")}}
