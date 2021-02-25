SELECT   
    PARSE_DATETIME('%Y-%m-%d %H:%M:%S',open_dt) as CreatedDate
    , PARSE_DATETIME('%Y-%m-%d %H:%M:%S',closed_dt) as ClosedDate
    , department as AgencyAbbreviation
    , subject as AgencyName
    , case_title as ComplaintType
    , location_zipcode as Zip
    , location as Address
    , neighborhood as City 
    , case_status as Status
    , Latitude
    , Longitude   
FROM `opendatadbt.311.boston311`