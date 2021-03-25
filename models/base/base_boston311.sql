SELECT   
    case_enquiry_id AS UniqueKey
    ,PARSE_DATETIME('%Y-%m-%d %H:%M:%S',open_dt) as CreatedDate
    ,format_date('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S',open_dt))) AS CreatedDateKey
    , PARSE_DATETIME('%Y-%m-%d %H:%M:%S',closed_dt) as ClosedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S',closed_dt))) AS ClosedDateKey
    , department as AgencyAbbreviation
    , subject as AgencyName
    , case_title as ComplaintType
    , location_zipcode as Zip
    , location as Address
    , UPPER(neighborhood) as City
    , 'MA' as State
    , case_status as Status
    , cast(Latitude as float64) AS Latitude
    , cast(Longitude as float64) AS Longitude
    , 'Boston' as OpenDataSource 
FROM `opendatadbt.311.boston311`
