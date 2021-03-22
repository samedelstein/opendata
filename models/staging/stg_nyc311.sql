SELECT * FROM {{ ref('stg_nyc311') }} where UniqueKey not in (select UniqueKey from {{ ref('base_nyc311') }})

union all

select {{ dbt_utils.current_timestamp() }} as RowCreatedDateTime, {{ dbt_utils.current_timestamp() }} AS RowUpdatedDateTime, * from {{ ref('base_nyc311') }}  where UniqueKey not in (select UniqueKey from {{ ref('stg_nyc311') }} )

UNION ALL

select stg.RowCreatedDateTime, {{ dbt_utils.current_timestamp() }}, base.* from {{ ref('stg_nyc311') }}  stg join {{ ref('base_nyc311') }}  base on stg.UniqueKey = base.UniqueKey



