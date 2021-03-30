{{ config(materialized='view') }}

with tracts as (
    SELECT geo_id, county_fips_code, state_fips_code, tract_geom FROM `bigquery-public-data.geo_census_tracts.census_tracts_texas` where county_fips_code = '015'

union all

SELECT geo_id, county_fips_code, state_fips_code, tract_geom FROM `bigquery-public-data.geo_census_tracts.census_tracts_arkansas` where county_fips_code = '119'

union all

SELECT geo_id, county_fips_code, state_fips_code, tract_geom FROM `bigquery-public-data.geo_census_tracts.census_tracts_massachusetts` where county_fips_code = '025'

union all

SELECT geo_id, county_fips_code, state_fips_code, tract_geom FROM `bigquery-public-data.geo_census_tracts.census_tracts_tennessee` where county_fips_code = '065'

union all

SELECT geo_id, county_fips_code, state_fips_code, tract_geom FROM `bigquery-public-data.geo_census_tracts.census_tracts_illinois` where county_fips_code = '031'

union all

SELECT geo_id, county_fips_code, state_fips_code, tract_geom FROM `bigquery-public-data.geo_census_tracts.census_tracts_illinois` where county_fips_code = '031'

union all

SELECT geo_id, county_fips_code, state_fips_code, tract_geom FROM `bigquery-public-data.geo_census_tracts.census_tracts_missouri` where county_fips_code = '095'

union all

SELECT geo_id, county_fips_code, state_fips_code, tract_geom FROM `bigquery-public-data.geo_census_tracts.census_tracts_new_york` where county_fips_code in ('005', '047', '061', '085', '081')

union all 

SELECT geo_id, county_fips_code, state_fips_code, tract_geom FROM `bigquery-public-data.geo_census_tracts.census_tracts_california` where county_fips_code = '075')

select t.*, acs.total_pop, acs.households from `bigquery-public-data.census_bureau_acs.censustract_2018_5yr` acs
inner join tracts t 
on acs.geo_id = t.geo_id
