SELECT
    property_id,
    agent_id,
    property_location_id AS location_id,
    property_type_id,
    property_added_date,
    price,
    property_url AS url,
    property_full_description AS description,
    property_size_in_square_feet AS size_in_square_feet,
    property_size_in_square_meters AS size_in_square_meters,
    is_retirement_housing_available,
    ownership_type

FROM {{ ref("property_base") }}