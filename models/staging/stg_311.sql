with nyc311 as (

    select *  from {{ ref('stg_nyc311') }}

),

chicago311 as (

    select *  from {{ ref('stg_chicago311') }}

),

boston311 as (

    select *   from {{ ref('stg_boston311') }}

),

 kc311 as (

    select *  from {{ ref('stg_kansascity311') }}

),

 littlerock311 as (

    select *  from {{ ref('stg_littlerock311') }}

),

 chattanooga311 as (

    select *  from {{ ref('stg_chattanooga311') }}

),

 stg311 as (
select * from nyc311

UNION DISTINCT 

select * from chicago311

UNION DISTINCT 

select * from boston311

UNION DISTINCT 

select * from kc311

UNION DISTINCT 

select * from chattanooga311

UNION DISTINCT 

select * from littlerock311
)

select   *
  from stg311