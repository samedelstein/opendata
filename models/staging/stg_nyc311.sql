SELECT 
    PARSE_DATETIME('%m/%d/%Y %H:%M:%S %p',Created_Date) as CreatedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%m/%d/%Y %H:%M:%S %p',Created_Date))) AS CreatedDateKey
    , PARSE_DATETIME('%m/%d/%Y %H:%M:%S %p',Closed_Date) as ClosedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%m/%d/%Y %H:%M:%S %p',Closed_Date))) AS ClosedDateKey
    , CASE WHEN Agency Like '%OF SPECIAL ENFORCEMENT%' THEN 'MAYORS OFFICE OF SPECIAL ENFORCEMENT' ELSE Agency END as AgencyAbbreviation
    , CASE WHEN Agency_Name Like '%Office of Special Enforcement%' THEN 'MAYORS OFFICE OF SPECIAL ENFORCEMENT' ELSE Agency_Name END as AgencyName
    , Descriptor as ComplaintType
    , Incident_Zip as Zip
    , Incident_Address as Address
    , UPPER(City) as City
    , 'NY' AS State
    , Status
    , Latitude
    , Longitude
    , 'NYC' as OpenDataSource
FROM `opendatadbt.311.nyc311` 