SELECT * FROM opendatadbt.dbt_sedelstein.stg_nyc311 where UniqueKey not in (select UniqueKey from opendatadbt.dbt_sedelstein.base_nyc311)

union all

select {{ dbt_utils.current_timestamp() }} as RowCreatedDateTime, {{ dbt_utils.current_timestamp() }} AS RowUpdatedDateTime, * from opendatadbt.dbt_sedelstein.base_nyc311  where UniqueKey not in (select UniqueKey from opendatadbt.dbt_sedelstein.stg_nyc311 )

UNION ALL

select stg.RowCreatedDateTime, {{ dbt_utils.current_timestamp() }}, base.* from opendatadbt.dbt_sedelstein.stg_nyc311  stg join opendatadbt.dbt_sedelstein.base_nyc311 base on stg.UniqueKey = base.UniqueKey



