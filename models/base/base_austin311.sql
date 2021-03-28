SELECT   
    sr_number as UniqueKey
    , PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',sr_created_date) as CreatedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',sr_created_date))) AS CreatedDateKey
    , PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',sr_closed_date) as ClosedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',sr_closed_date))) AS ClosedDateKey
    , cast(null as string) as AgencyAbbreviation
    , cast(null as string) as AgencyName
    , sr_type_desc as ComplaintType
    , cast(sr_location_zip_code as string) as Zip
    , cast(sr_location as string)  as Address
    , case when sr_location_city is null then 'Austin' else sr_location_city End  as City
    , 'TX' as State
    , case when sr_status_desc = 'Open' then 'Open'
            when sr_status_desc = 'Closed' then 'Closed'
            when sr_status_desc = 'Work In Progress' then 'In Progress'
            when sr_status_desc = 'New' then 'Pending'
            when sr_status_desc = 'Resolved' then 'Closed'
            when sr_status_desc = 'Duplicate (open)' then 'Open' 
            when sr_status_desc = 'Duplicate (closed)' then 'Closed' End as Status
    , cast(sr_location_lat as float64) AS Latitude
    , cast(sr_location_long as float64) AS Longitude
    , 'Austin' as OpenDataSource 
FROM `opendatadbt.311.austin311`