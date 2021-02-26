with stg311 as (
    SELECT 
  createddate
  ,closeddate
  ,citykey
  , complainttypekey
  , agencykey
  , statuskey
  , DATE_DIFF(DATE (closeddate)
  ,DATE( createddate),  DAY) as DateDiff 
FROM {{ ref('stg_311') }} stg 
join {{ ref('city') }} city 
  on stg.city = city.city and stg.state = city.state
join {{ ref('complainttype') }} ct
  on stg.complainttype = ct.complainttype
join {{ ref('status') }} s
  on stg.status = s.status
join {{ ref('agency') }} a
  on stg.agencyname = a.agencyname
  and stg.agencyabbreviation = a.agencyabbreviation
)
select   * 
  from stg311