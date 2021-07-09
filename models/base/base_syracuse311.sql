SELECT 
    cast(id as string) as UniqueKey
    , PARSE_DATETIME('%Y-%m-%dT%H:%M:%S-04:00',created_at) as CreatedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S-04:00',created_at))) AS CreatedDateKey
    , PARSE_DATETIME('%Y-%m-%dT%H:%M:%S-04:00',closed_at) as ClosedDate
    , format_date('%Y%m%d', DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S-04:00',closed_at))) AS ClosedDateKey
    , cast(null as string) as AgencyAbbreviation
    , request_type.organization as AgencyName
    , summary as ComplaintType
    , cast(null as string) as Zip
    , address as Address
    , 'Syracuse' as City    
    , 'NY' as State
    , CASE WHEN status = 'Open' then 'Open'
        WHEN status = 'Acknowledged' then 'Open'
        WHEN status = 'Closed' then 'Closed' END as Status
    , cast(lat as float64) AS Latitude
    , cast(lng as float64) AS Longitude
    , 'Syracuse' as OpenDataSource 
FROM `opendatadbt.tap_scf.scf_issues`
