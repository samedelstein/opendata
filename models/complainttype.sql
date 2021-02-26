with stg311 as (

    select distinct(complainttype)  from {{ ref('stg_nyc311') }}

)

select   {{ dbt_utils.surrogate_key(
      'complainttype' 
  ) }} as complaintTypeKey,
    complainttype
  from stg311