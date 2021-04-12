with points as (
    select * from {{ref('timetocomplete')}} 
),

tracts as (
    select tract_geom, geo_id from `opendatadbt.dbt_sedelstein.censustracts`

)


    SELECT t.geo_id, p.UniqueKey  FROM tracts t, points p 
    WHERE st_contains(t.tract_geom, p.coordinates) 
