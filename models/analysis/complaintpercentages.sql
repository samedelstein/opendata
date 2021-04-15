with places as (
select  count(*) placescount
        , c.county_fips_code
        , avg(daystoclose) placesavgdaystoclose
        , avg(daysopened) placesavgdaysopened
        , avg(totaldays) placesavgtotaldays
from `opendatadbt.dbt_sedelstein.censustracts` c 
join {{ref('timetocomplete')}} t 
    on c.geo_id = t.geo_id
group by c.county_fips_code
),

tracts as (
select  count(*) tractcount
        , c.geo_id
        , c.county_fips_code
        , avg(daystoclose) tractavgdaystoclose
        , avg(daysopened) tractavgdaysopened
        , avg(totaldays) tractavgtotaldays
from `opendatadbt.dbt_sedelstein.censustracts` c 
join {{ref('timetocomplete')}} t 
    on c.geo_id= t.geo_id
group by c.geo_id, c.county_fips_code
)

select  tractcount
        , placescount
        , t.geo_id
        , p.county_fips_code
        , tractavgdaystoclose
        , placesavgdaystoclose
        , tractavgdaysopened
        , tractavgtotaldays
        , placesavgdaysopened
        , placesavgtotaldays
from tracts t 
left join places p 
on t.county_fips_code = p.county_fips_code 
