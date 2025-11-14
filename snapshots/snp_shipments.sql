{% snapshot shipments_snapshot %}
{{
    config
    (

    target_database = 'qwt_dev',
    target_schema = 'snapshots_dev',
    unique_key = ['orderid', 'lineno'],
    strategy = 'timestamp',
    updated_at = 'shipmentdate'
    )
}}
select * from {{ref("stg_shipments")}}
{% endsnapshot %}