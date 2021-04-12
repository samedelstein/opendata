with places as (
select  count(*) placescount
        , c.county_fips_code
        , avg(daystoclose) placesavgdaystoclose
        , avg(daysopened) placesavgdaysopened
        , avg(totaldays) placesavgtotaldays
from {{ref('censustimetocomplete')}} ctc
join `opendatadbt.dbt_sedelstein.censustracts` c 
    on ctc.geo_id = c.geo_id 
join {{ref('timetocomplete')}} t 
    on ctc.uniquekey = t.uniquekey
group by c.county_fips_code
),

tracts as (
select  count(*) tractcount
        , ctc.geo_id
        , c.county_fips_code
        , avg(daystoclose) tractavgdaystoclose
        , avg(daysopened) tractavgdaysopened
        , avg(totaldays) tractavgtotaldays
from {{ref('censustimetocomplete')}} ctc
join `opendatadbt.dbt_sedelstein.censustracts` c 
    on ctc.geo_id = c.geo_id 
join {{ref('timetocomplete')}} t 
    on ctc.uniquekey = t.uniquekey
group by ctc.geo_id, c.county_fips_code
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
