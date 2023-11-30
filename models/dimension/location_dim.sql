WITH all_addresses AS (
    SELECT 
        property_location_id AS location_id,
        property_address AS address,
        'property' AS type,
        COALESCE(property_postcode, REGEXP_EXTRACT(property_address, r'[A-Z]{1,2}[0-9]{1,2}[A-Z]?\s?[0-9][A-Z]{2}'), REGEXP_EXTRACT(property_address, r'[A-Z]{1,2}[0-9]{1,2}[A-Z]?')) AS postcode,
        property_longitude AS longitude,
        property_latitude AS latitude

    FROM {{ ref("property_base") }}

    UNION ALL

    SELECT
        agent_location_id AS location_id,
        agent_address AS address,
        'agent' AS type,
        COALESCE(REGEXP_EXTRACT(agent_address, r'[A-Z]{1,2}[0-9]{1,2}[A-Z]?\s?[0-9][A-Z]{2}'), REGEXP_EXTRACT(agent_address, r'[A-Z]{1,2}[0-9]{1,2}[A-Z]?')) AS postcode,
        NULL AS longitude,
        NULL AS latitude

    FROM {{ ref("property_base") }}
)

SELECT
    location_id,
    CASE WHEN ARRAY_LENGTH(SPLIT(postcode, ' ')) >= 1 THEN SPLIT(postcode, ' ')[0] END AS postcode_district,
    postcode,
    address,
    latitude,
    longitude

FROM all_addresses

QUALIFY ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY CASE WHEN type = 'property' THEN 0 ELSE 1 END, CASE WHEN postcode_district IS NOT NULL THEN 0 ELSE 1 END, CASE WHEN postcode IS NOT NULL THEN 0 ELSE 1 END) = 1