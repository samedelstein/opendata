SELECT   
    service_request_key as UniqueKey
    , PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',created_date) as CreatedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',created_date))) AS CreatedDateKey
    , PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',completed_at) as ClosedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',completed_at))) AS ClosedDateKey
    , cast(null as string) as AgencyAbbreviation
    , cast(department as string) as AgencyName
    , request_type as ComplaintType
    , cast(null as string) as Zip
    , cast(null as string)  as Address
    , 'Chattanooga'   as City
    , 'TN' as State
    , CASE WHEN status_code = 'O-OPEN' then 'Open'
        WHEN status_code = 'O-CLOSED' then 'Closed'
        WHEN status_code = 'O-NEW' then 'Pending' 
        ELSE cast(null as string) END as Status
    , cast(Latitude as float64) AS Latitude
    , cast(Longitude as float64) AS Longitude
    , 'Chattanooga' as OpenDataSource 
FROM `opendatadbt.311.chattanooga311`