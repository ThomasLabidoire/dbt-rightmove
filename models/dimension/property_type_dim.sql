SELECT 
    property_type_id,
    property_type_name AS name,
    property_category AS category,
    property_sub_category AS sub_category,
    bedroom_count

FROM {{ ref("property_base") }}

WHERE property_type_id IS NOT NULL

QUALIFY ROW_NUMBER() OVER (PARTITION BY property_type_id ORDER BY property_added_date DESC) = 1