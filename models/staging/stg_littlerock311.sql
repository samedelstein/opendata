SELECT * 
FROM opendatadbt.dbt_sedelstein.stg_littlerock311 
where UniqueKey not in (select UniqueKey from {{ref('base_littlerock311')}})

union all

select  {{ dbt_utils.current_timestamp() }} as RowCreatedDateTime, 
        {{ dbt_utils.current_timestamp() }} AS RowUpdatedDateTime, 
        * 
        from {{ref('base_littlerock311')}}  
        where UniqueKey not in (select UniqueKey from opendatadbt.dbt_sedelstein.stg_littlerock311 )

UNION ALL

select  stg.RowCreatedDateTime, 
        {{ dbt_utils.current_timestamp() }}, 
        base.* 
    from opendatadbt.dbt_sedelstein.stg_littlerock311  stg 
    join {{ref('base_littlerock311')}} base 
        on stg.UniqueKey = base.UniqueKey


