SELECT     
    PARSE_DATETIME('%m/%d/%Y %H:%M:%S %p',Created_Date) as CreatedDate
    , PARSE_DATETIME('%m/%d/%Y %H:%M:%S %p',Closed_Date) as ClosedDate
    , NULL as AgencyAbbreviation
    , OWNER_DEPARTMENT as AgencyName
    , SR_TYPE as ComplaintType
    , Zip_Code as Zip
    , Street_Address as Address
    , City
    , Status
    , Latitude
    , Longitude 
    FROM `opendatadbt.311.chicago311` 