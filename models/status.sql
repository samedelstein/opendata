with stg311 as (

    select distinct(status)  from {{ ref('stg_nyc311') }}

)

select   {{ dbt_utils.surrogate_key(
      'status'
  ) }} as StatusKey,
    status
  from stg311