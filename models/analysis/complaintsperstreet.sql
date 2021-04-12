


  WITH points AS (
  SELECT *
  FROM {{ref('timetocomplete')}} where potholecomplaint = 1

), lines AS (
  SELECT * from `opendatadbt.dbt_sedelstein.roads`
),

roadcount as (
SELECT count(*) as roadcount, road_id
FROM points, lines
WHERE ST_DWITHIN(points.Coordinates, lines.road_geom, 10)
group by road_id)

select  roadcount
        , r.road_id
        , road_geom
        , full_name
        , route_type
        , mtfcc_feature_class_code
        , name_lsad
        , state_name 
from roadcount r join `opendatadbt.dbt_sedelstein.roads` roads on r.road_id = roads.road_id