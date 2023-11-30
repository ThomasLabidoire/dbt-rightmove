SELECT
    CAST(postcode AS string) AS postcode_district,
    CAST(REGEXP_EXTRACT(postcode, r'^[A-Z]{1,2}') AS string) AS postcode_area,
    CAST(CASE WHEN ARRAY_LENGTH(SPLIT(town_area, ',')) >= 1 THEN SPLIT(town_area, ',')[0] END AS string) AS name,
    CAST(latitude AS float64) AS latitude,
    CAST(longitude AS float64) AS longitude,
    CAST(ST_GEOGPOINT(latitude, longitude) AS geography) AS geo_point

FROM {{ ref("postcode_district_raw") }}