with stg311 as (
    SELECT 
  createddateKey
  ,closeddateKey
  ,citykey
  , complainttypekey
  , agencykey
  , statuskey
  , CASE WHEN ClosedDate IS NOT NULL THEN DATE_DIFF(DATE (closeddate) ,DATE( createddate),  DAY) ELSE NULL END as DaysToClose
  , CASE WHEN ClosedDate IS NULL THEN DATE_DIFF(Current_Date, DATE( createddate),  DAY) ELSE NULL END as DaysOpened
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