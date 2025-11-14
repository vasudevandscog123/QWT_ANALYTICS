{% snapshot shipments_snapshot %}
{{
    config
    (

    target_database = env_var('DBT_QWT_DB', 'QWT_DEV'),
    target_schema =  env_var('DBT_SNAPSHOTS_SCHEMA', 'SNAPSHOTS_DEV') ,
    unique_key = ['orderid', 'lineno'],
    strategy = 'timestamp',
    updated_at = 'shipmentdate'
    )
}}
select * from {{ref("stg_shipments")}}
{% endsnapshot %}