SELECT   
    ticket_id as UniqueKey
    , PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',ticket_created_date_time) as CreatedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',ticket_created_date_time))) AS CreatedDateKey
    , PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',ticket_closed_date_time) as ClosedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',ticket_closed_date_time))) AS ClosedDateKey
    , cast(null as string) as AgencyAbbreviation
    , issue_type as AgencyName
    , issue_sub_category as ComplaintType
    , cast(REGEXP_REPLACE(JSON_EXTRACT(	human_address, "$.zip"), '"','') as string) as Zip
    , REGEXP_REPLACE(JSON_EXTRACT(	human_address, "$.address"), '"','')  as Address
    , REGEXP_REPLACE(JSON_EXTRACT(	human_address, "$.city") , '"','')  as City
    , 'AR' as State
    , CASE WHEN ticket_status = 'Open' then 'Open'
        WHEN ticket_status = 'Closed' then 'Closed' END as Status
    , cast(Latitude as float64) AS Latitude
    , cast(Longitude as float64) AS Longitude
    , 'Little Rock' as OpenDataSource 
FROM `opendatadbt.311.littlerock311`