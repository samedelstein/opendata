SELECT 
    case_id as UniqueKey
    ,PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',Creation_Date) as CreatedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',Creation_Date))) AS CreatedDateKey
    , PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',Closed_Date) as ClosedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',Closed_Date))) AS ClosedDateKey
    , department as AgencyAbbreviation
    , Work_Group as AgencyName
    , Request_Type as ComplaintType
    , Zip_Code as Zip
    , Street_Address as Address
    , 'Kansas City' as City
    , 'MO' as State
    , CASE WHEN Status = 'OPEN' then 'Open'
        WHEN Status = 'CANC' then 'Canceled'
        WHEN Status = 'ASSIG' then 'Assigned'
        WHEN Status = 'RESOL' then 'Closed'
        WHEN Status = 'DUP' then 'Duplicate' END as Status
    , cast(ycoordinate as float64) AS Latitude
    , cast(xcoordinate as float64) AS Longitude
    , 'Kansas City' as OpenDataSource 
FROM `opendatadbt.311.kc311`
