SELECT 
    CAST(REGEXP_EXTRACT(url, r'\d{8}') AS integer) AS property_id,
    CAST(COALESCE(REGEXP_EXTRACT(agent_url, r'\d{6}'), REGEXP_EXTRACT(agent_url, r'\d{5}'), REGEXP_EXTRACT(agent_url, r'\d{4}'), REGEXP_EXTRACT(agent_url, r'\d{3}')) AS integer) AS agent_id,
    CAST(TO_HEX(MD5(address)) AS string) AS property_location_id,
    CAST(TO_HEX(MD5(agent_address)) AS string) AS agent_location_id,
    CAST(TO_HEX(MD5(type)) AS string) AS property_type_id,
    CAST(price AS float64) AS price,
    CAST(type AS string) AS property_type_name,
    CAST(address AS string) AS property_address,
    CAST(url AS string) AS property_url,
    CAST(agent_url AS string) AS agent_url,
    COALESCE(CAST(number_bedrooms AS int64), 0) AS bedroom_count,
    CAST(TRIM(Full_Description) AS string) AS property_full_description,
    CAST(agent_name AS string) AS agent_name,
    CAST(agent_address AS string) AS agent_address,
    CAST(postcode AS string) AS property_postcode,
    CAST(longitude AS float64) AS property_longitude,
    CAST(latitude AS float64) AS property_latitude,
    CAST(propertyType AS string) AS property_category,
    CAST(propertySubType AS string) AS property_sub_category,
    CAST(SUBSTRING(added, 1, 4) || '-' || SUBSTRING(added, 5, 2) || '-' || SUBSTRING(added, 7, 2) AS date) AS property_added_date,
    CAST(NULLIF(maxSizeFt, 0) AS float64) AS property_size_in_square_feet,
    CAST(NULLIF(maxSizeFt, 0) * 0.092903 AS float64) AS property_size_in_square_meters,
    COALESCE(CAST(retirement AS bool), FALSE) AS is_retirement_housing_available,
    CAST(preOwned AS string) AS ownership_type

FROM {{ ref("property_raw") }}

QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(REGEXP_EXTRACT(url, r'\d{8}') AS integer) ORDER BY CAST(COALESCE(SUBSTRING(added, 1, 4) || '-' || SUBSTRING(added, 5, 2) || '-' || SUBSTRING(added, 7, 2), '1970-01-01') AS date) DESC) = 1