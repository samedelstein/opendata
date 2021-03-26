with stg311 as (
    SELECT 
  createddateKey
  ,closeddateKey
  ,ifnull(citykey, '-1') citykey
  , ifnull(complainttypekey, '-1') complainttypekey
  , ifnull(agencykey, '-1') agencykey
  , ifnull(statuskey, '-1') statuskey
  , CASE WHEN ClosedDate IS NOT NULL THEN DATE_DIFF(DATE (closeddate) ,DATE( createddate),  DAY) ELSE NULL END as DaysToClose
  , CASE WHEN ClosedDate IS NULL THEN DATE_DIFF(Current_Date, DATE( createddate),  DAY) ELSE NULL END as DaysOpened
  , CASE WHEN ct.complainttype LIKE '%Pothole%' OR ct.complainttype LIKE '%Pavement_Defect%' THEN 1 ELSE 0 END AS PotholeComplaint
  , Coordinates
FROM {{ ref('stg_311') }} stg 
left join {{ ref('city') }} city 
  on stg.city = city.city and stg.state = city.state
left join {{ ref('complainttype') }} ct
  on stg.complainttype = ct.complainttype
left join {{ ref('status') }} s
  on stg.status = s.status
left join {{ ref('agency') }} a
  on stg.agencyname = a.agencyname

)
select   * 
  from stg311
