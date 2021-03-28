SELECT   
    service_request_id as UniqueKey
    , PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',requested_datetime) as CreatedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',requested_datetime))) AS CreatedDateKey
    , PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',closed_date) as ClosedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',closed_date))) AS ClosedDateKey
    , cast(agency_responsible as string) as AgencyAbbreviation
    , service_name as AgencyName
    , service_details as ComplaintType
    , cast(null as string) as Zip
    , cast(address as string)  as Address
    , case when neighborhoods_sffind_boundaries is null then 'SF' else neighborhoods_sffind_boundaries End  as City
    , 'CA' as State
    , CASE WHEN status_description = 'Open' then 'Open'
        WHEN status_description = 'Closed' then 'Closed'
        ELSE cast(null as string) END as Status
    , cast(lat as float64) AS Latitude
    , cast(long as float64) AS Longitude
    , 'SF' as OpenDataSource 
FROM `opendatadbt.311.sf311`