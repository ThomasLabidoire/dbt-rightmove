SELECT
    CAST(postcode AS string) AS postcode_district,
    CAST(COALESCE(population, 0) AS int64) AS population_count,
    CAST(COALESCE(households, 0) AS int64) AS household_count
    
FROM {{ ref("postcode_district_raw") }}