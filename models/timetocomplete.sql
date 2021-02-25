with boston311 as (
    SELECT 
  createddate
  ,closeddate
  ,citykey
  , complainttypekey
  , agencykey
  , statuskey
  , DATE_DIFF(DATE (closeddate)
  ,DATE( createddate),  DAY) as DateDiff 
FROM {{ ref('stg_boston311') }} boston 
join {{ ref('city') }} city 
  on boston.city = city.city and boston.state = city.state
join {{ ref('complainttype') }} ct
  on boston.complainttype = ct.complainttype
join {{ ref('status') }} s
  on boston.status = s.status
join {{ ref('agency') }} a
  on boston.agencyname = a.agencyname
  and boston.agencyabbreviation = a.agencyabbreviation
),

 nyc311 as (
    SELECT 
  createddate
  ,closeddate
  ,citykey
  , complainttypekey
  , agencykey
  , statuskey
  , DATE_DIFF(DATE (closeddate)
  ,DATE( createddate),  DAY) as DateDiff 
FROM {{ ref('stg_nyc311') }} boston 
join {{ ref('city') }} city 
  on boston.city = city.city and boston.state = city.state
join {{ ref('complainttype') }} ct
  on boston.complainttype = ct.complainttype
join {{ ref('status') }} s
  on boston.status = s.status
join {{ ref('agency') }} a
  on boston.agencyname = a.agencyname
  and boston.agencyabbreviation = a.agencyabbreviation
),

 chicago311 as (
    SELECT 
  createddate
  ,closeddate
  ,citykey
  , complainttypekey
  , agencykey
  , statuskey
  , DATE_DIFF(DATE (closeddate)
  ,DATE( createddate),  DAY) as DateDiff 
FROM {{ ref('stg_chicago311') }} boston 
join {{ ref('city') }} city 
  on boston.city = city.city and boston.state = city.state
join {{ ref('complainttype') }} ct
  on boston.complainttype = ct.complainttype
join {{ ref('status') }} s
  on boston.status = s.status
join {{ ref('agency') }} a
  on boston.agencyname = a.agencyname
  and boston.agencyabbreviation = a.agencyabbreviation
),

timetocomplete as (
select * from nyc311

UNION DISTINCT 

select * from chicago311

UNION DISTINCT 

select * from boston311
)

select   * 
  from timetocomplete