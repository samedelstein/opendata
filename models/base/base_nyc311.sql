SELECT 
    unique_key AS UniqueKey
    , PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',Created_Date) as CreatedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',Created_Date))) AS CreatedDateKey
    , PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',Closed_Date) as ClosedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',Closed_Date))) AS ClosedDateKey
    , CASE WHEN Agency Like '%OF SPECIAL ENFORCEMENT%' THEN 'MAYORS OFFICE OF SPECIAL ENFORCEMENT' ELSE Agency END as AgencyAbbreviation
    , CASE WHEN Agency_Name Like '%Office of Special Enforcement%' THEN 'MAYORS OFFICE OF SPECIAL ENFORCEMENT' ELSE Agency_Name END as AgencyName
    , Descriptor as ComplaintType
    , Incident_zip as Zip
    , Incident_Address as Address
    , UPPER(City) as City
    , 'NY' AS State
    , Status
    , cast(Latitude as float64) AS Latitude
    , cast(Longitude as float64) AS Longitude
    , 'NYC' as OpenDataSource
FROM `opendatadbt.311.nyc311` 