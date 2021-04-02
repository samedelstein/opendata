with points as (
    select * from {{ref('timetocomplete')}} 
),

tracts as (
    select tract_geom, geo_id from `opendatadbt.dbt_sedelstein.censustracts`

)


    SELECT count(t.geo_id) as Count_Tract, t.geo_id, p.complainttypekey, p.createddatekey, p.closeddatekey, p.statuskey, p.citykey  FROM tracts t, points p 
    WHERE st_contains(t.tract_geom, p.coordinates) 
    group by p.complainttypekey, p.createddatekey, p.statuskey, t.geo_id, p.citykey, p.closeddatekey
