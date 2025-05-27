WITH location_hub_sat AS (
    SELECT
        s.load_date,
        s.location_code,
        s.city_id,
        s.state_id,
        s.country_id,
        s.district_id,
        s.zone_id,
        s.region_id,
        s.area_id,
        h.location_id,
        h.location_hk
    FROM {{ ref("hub_location") }} AS h
    INNER JOIN {{ ref("sat_location") }} AS s
        ON h.location_hk = s.location_hk
    QUALIFY lead(s.load_date) OVER (
        PARTITION BY h.location_hk
        ORDER BY s.load_date
    ) IS NULL
),

star_location AS (
    SELECT
        location_hub_sat.location_id,
        location_hub_sat.location_hk,
        location_hub_sat.location_code,
        location_hub_sat.city_id,
        location_hub_sat.state_id,
        location_hub_sat.country_id,
        location_hub_sat.district_id,
        location_hub_sat.zone_id,
        location_hub_sat.region_id,
        location_hub_sat.area_id
    FROM location_hub_sat
)

SELECT *
FROM star_location
