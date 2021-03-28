with stg311 as (

    select distinct(status)  from {{ ref('stg_311') }}

)

select   {{ dbt_utils.surrogate_key(
      'status'
  ) }} as StatusKey,
    status as StatusName
  from stg311