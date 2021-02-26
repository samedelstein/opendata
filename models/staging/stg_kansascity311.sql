SELECT   
    PARSE_DATETIME('%m/%d/%Y',Creation_Date) as CreatedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%m/%d/%Y',Creation_Date))) AS CreatedDateKey
    , PARSE_DATETIME('%m/%d/%Y',Closed_Date) as ClosedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%m/%d/%Y',Closed_Date))) AS ClosedDateKey
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
    , Latitude
    , Longitude  
    , 'Kansas City' as OpenDataSource 
FROM `opendatadbt.311.kc311`
