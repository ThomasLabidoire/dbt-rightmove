WITH property_per_district AS (
    SELECT 
        lo.postcode_district,
        COUNT(pdi.property_id) AS property_count,
        COUNT(CASE WHEN pdi.price IS NOT NULL THEN pdi.property_id END) AS property_with_price_count,
        COUNT(CASE WHEN COALESCE(ptd.bedroom_count) > 0 AND pdi.price IS NOT NULL THEN pdi.property_id END) AS property_with_price_and_bedroom_count,
        SUM(pdi.price) AS market_value,
        SUM(ptd.bedroom_count) AS bedroom_count

    FROM {{ ref("property_dim") }} pdi

    JOIN {{ ref("property_type_dim") }} ptd
        ON pdi.property_type_id = ptd.property_type_id

    JOIN {{ ref("location_dim") }} lo
        ON pdi.location_id = lo.location_id

    GROUP BY 1
),

agents_per_district AS (
    SELECT 
        lo.postcode_district,
        COUNT(ad.agent_id) AS agent_count

    FROM {{ ref("agent_dim") }} ad

    JOIN {{ ref("location_dim") }} lo
        ON ad.location_id = lo.location_id

    GROUP BY 1
)

SELECT
    pdd.postcode_district,
    pdd.name AS district_name,
    pdd.geo_point AS district_geo_point,
    pad.postcode_area,
    pad.name AS area_name,
    pad.geo_point AS area_geo_point,
    pdf.population_count,
    pdf.household_count,
    COALESCE(pdi.property_count, 0) AS property_count,
    COALESCE(pdi.property_with_price_count, 0) AS property_with_price_count,
    COALESCE(pdi.property_with_price_and_bedroom_count, 0) AS property_with_price_and_bedroom_count,
    COALESCE(pdi.market_value, 0) AS market_value,
    COALESCE(pdi.bedroom_count, 0) AS bedroom_count,
    COALESCE(adi.agent_count, 0) AS agent_count

FROM {{ ref("postcode_district_dim") }} pdd

JOIN {{ ref("postcode_district_facts") }} pdf
    ON pdd.postcode_district = pdf.postcode_district

JOIN {{ ref("postcode_area_dim") }} pad
    ON pdd.postcode_area = pad.postcode_area

LEFT JOIN property_per_district pdi
    ON pdd.postcode_district = pdi.postcode_district

LEFT JOIN agents_per_district adi
    ON pdd.postcode_district = adi.postcode_district