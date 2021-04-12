SELECT     
    sr_number as UniqueKey
    , PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',Created_Date) as CreatedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',Created_Date))) AS CreatedDateKey
    , PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',Closed_Date) as ClosedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S.000',Closed_Date))) AS ClosedDateKey
    , cast(null as string) as AgencyAbbreviation
    , OWNER_DEPARTMENT as AgencyName
    , SR_TYPE as ComplaintType
    , cast(Zip_Code as String) as Zip
    , Street_Address as Address
    , case when City is null then 'Chicago' else UPPER(City) End as City
    , 'IL' AS State
    , CASE WHEN Status = 'Completed' THEN 'Closed' ELSE Status End AS Status
    , cast(Latitude as float64) AS Latitude
    , cast(Longitude as float64) AS Longitude
    , 'Chicago' as OpenDataSource
    FROM `opendatadbt.311.chicago311` 