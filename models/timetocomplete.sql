{{ config(
    full_refresh = true
) }}

with mindistances as (
  select  concat(UniqueKey, stg.OpenDataSource) as UniqueKey
          , min(st_distance(stg.Coordinates, r.road_geom)) as mindistance
  from {{ ref('stg_311') }} stg 
  join `opendatadbt.dbt_sedelstein.roads` r on ST_DWITHIN(stg.Coordinates, r.road_geom, 15)
  group by uniquekey
),

distances as (
         select  concat(UniqueKey, stg.OpenDataSource) as UniqueKey
          , st_distance(stg.Coordinates, r.road_geom) as distance
          , road_id
  from {{ ref('stg_311') }} stg 
  join `opendatadbt.dbt_sedelstein.roads` r on ST_DWITHIN(stg.Coordinates, r.road_geom, 15)
),

roadselect as (
        select  d.uniquekey
                , road_id 
                , row_number() over (partition by d.uniquekey order by road_id) as rn
        from distances d 
        join mindistances m 
                on d.uniquekey = m.uniquekey and d.distance = m.mindistance
                ),

  stg311_coordinates as (
  SELECT 
  RowCreatedDateTime
  ,RowUpdatedDateTime
  ,concat(stg.UniqueKey, stg.OpenDataSource) as UniqueKey
  , createddateKey
  ,closeddateKey
  ,ifnull(citykey, '-1') citykey
  , ifnull(complainttypekey, '-1') complainttypekey
  --, ifnull(agencykey, '-1') agencykey
  , ifnull(statuskey, '-1') statuskey
  , CASE WHEN ClosedDate IS NOT NULL THEN DATE_DIFF(DATE (closeddate) ,DATE( createddate),  DAY) ELSE NULL END as DaysToClose
  , CASE WHEN ClosedDate IS NULL THEN DATE_DIFF(Current_Date, DATE( createddate),  DAY) ELSE NULL END as DaysOpened
  , CASE WHEN ClosedDate IS NULL THEN DATE_DIFF(Current_Date, DATE( createddate),  DAY) 
          ELSE DATE_DIFF(DATE (closeddate) ,DATE( createddate),  DAY) END as TotalDays
  , CASE WHEN ct.complainttypename LIKE '%Pothole%' OR ct.complainttypename LIKE '%Pavement_Defect%' THEN 1 ELSE 0 END AS PotholeComplaint
  , Latitude
  , Longitude
  , Coordinates
  , geo_id
  , r.road_id
FROM {{ ref('stg_311') }} stg 
left join {{ ref('city') }} city 
  on stg.city = city.cityname and stg.state = city.state
left join {{ ref('complainttype') }} ct
  on stg.complainttype = ct.complainttypename
left join {{ ref('status') }} s
  on stg.status = s.statusname
--left join {{ ref('agency') }} a
--  on stg.agencyname = a.agencyname
join `opendatadbt.dbt_sedelstein.censustracts` c on st_contains(c.tract_geom, stg.coordinates)
join roadselect r on concat(stg.UniqueKey, stg.OpenDataSource) = r.uniquekey and rn = 1


),

stg311_nocoordinates as (
  SELECT 
  RowCreatedDateTime
  ,RowUpdatedDateTime
  ,concat(UniqueKey, stg.OpenDataSource) as UniqueKey
  , createddateKey
  ,closeddateKey
  ,ifnull(citykey, '-1') citykey
  , ifnull(complainttypekey, '-1') complainttypekey
  --, ifnull(agencykey, '-1') agencykey
  , ifnull(statuskey, '-1') statuskey
  , CASE WHEN ClosedDate IS NOT NULL THEN DATE_DIFF(DATE (closeddate) ,DATE( createddate),  DAY) ELSE NULL END as DaysToClose
  , CASE WHEN ClosedDate IS NULL THEN DATE_DIFF(Current_Date, DATE( createddate),  DAY) ELSE NULL END as DaysOpened
  , CASE WHEN ClosedDate IS NULL THEN DATE_DIFF(Current_Date, DATE( createddate),  DAY) 
          ELSE DATE_DIFF(DATE (closeddate) ,DATE( createddate),  DAY) END as TotalDays
  , CASE WHEN ct.complainttypename LIKE '%Pothole%' OR ct.complainttypename LIKE '%Pavement_Defect%' THEN 1 ELSE 0 END AS PotholeComplaint
  , Latitude
  , Longitude
  , Coordinates
  , cast(NULL as String) as geo_id
  , cast(NULL as String) as road_id
FROM {{ ref('stg_311') }} stg 
left join {{ ref('city') }} city 
  on stg.city = city.cityname and stg.state = city.state
left join {{ ref('complainttype') }} ct
  on stg.complainttype = ct.complainttypename
left join {{ ref('status') }} s
  on stg.status = s.statusname
--left join {{ ref('agency') }} a
--  on stg.agencyname = a.agencyname
where coordinates is null
),

stg311 as (
    select * from stg311_coordinates

    union all 

    select * from  stg311_nocoordinates
)

select * from stg311 