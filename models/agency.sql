with stg311 as (

    select distinct(agencyname), agencyabbreviation  from {{ ref('stg_311') }}

)

select   {{ dbt_utils.surrogate_key(
      'agencyname', 'agencyabbreviation' 
  ) }} as agencyKey,
    agencyname, 
    agencyabbreviation 
  from stg311