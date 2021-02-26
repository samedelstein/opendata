with nyc311 as (

    select *  from {{ ref('stg_nyc311') }}

),

chicago311 as (

    select *  from {{ ref('stg_chicago311') }}

),

boston311 as (

    select *   from {{ ref('stg_boston311') }}

),

 stg311 as (
select * from nyc311

UNION DISTINCT 

select * from chicago311

UNION DISTINCT 

select * from boston311
)

select   *
  from stg311