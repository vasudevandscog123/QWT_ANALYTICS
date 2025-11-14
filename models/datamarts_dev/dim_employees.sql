{{config(materialized='view', schema=env_var('DBT_DATAMARTS_SCHEMA', 'DATAMARTS_DEV'))}}
select * from {{ref("trf_employees")}}