with tractpop as (
    select sum(total_pop) tractpop, geo_id, county_fips_code from `opendatadbt.dbt_sedelstein.censustracts` c
    group by geo_id, county_fips_code
),

placepop as (
        select sum(total_pop) placepop, county_fips_code from `opendatadbt.dbt_sedelstein.censustracts` c
    group by county_fips_code
)

select  t.geo_id
        , t.county_fips_code
        , placepop
        , tractpop
from tractpop t 
left join placepop p on t.county_fips_code = p.county_fips_code

