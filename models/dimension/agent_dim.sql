SELECT 
    agent_id,
    agent_location_id AS location_id,
    agent_name AS name,
    agent_url AS url

FROM {{ ref("property_base") }}

WHERE agent_id IS NOT NULL

QUALIFY ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY property_added_date DESC) = 1