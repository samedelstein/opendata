with stg311 as (

    select distinct(city), state, OpenDataSource  from {{ ref('stg_nyc311') }}

)

select   {{ dbt_utils.surrogate_key(
      'city',
      'state',
      'OpenDataSource'
  ) }} as CityKey,
  city,
  state,
  OpenDataSource 
  from stg311


