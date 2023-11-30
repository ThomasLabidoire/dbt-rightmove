SELECT
    CAST(`Postcode area` AS string) AS postcode_area,
    CAST(`Area covered` AS string) AS name,
    CAST(latitude AS float64) AS latitude,
    CAST(longitude AS float64) AS longitude,
    CAST(ST_GEOGPOINT(longitude, latitude) AS geography) AS geo_point
    
FROM {{ ref("postcode_area_raw") }}