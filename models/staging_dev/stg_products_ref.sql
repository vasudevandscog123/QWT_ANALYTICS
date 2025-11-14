{{ config(materialized='view', transient=true, alias='stg_products_ref_alias') }}
select * from {{ref('stg_products')}}
