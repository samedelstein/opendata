SELECT     
    PARSE_DATETIME('%m/%d/%Y %H:%M:%S %p',Created_Date) as CreatedDate
    , PARSE_DATETIME('%m/%d/%Y %H:%M:%S %p',Closed_Date) as ClosedDate
    , cast(null as string) as AgencyAbbreviation
    , OWNER_DEPARTMENT as AgencyName
    , SR_TYPE as ComplaintType
    , Zip_Code as Zip
    , Street_Address as Address
    , UPPER(City) as City
    , 'IL' AS State
    , CASE WHEN Status = 'Completed' THEN 'Closed' ELSE Status End AS Status
    , Latitude
    , Longitude 
    , 'Chicago' as OpenDataSource
    FROM `opendatadbt.311.chicago311` 