WITH location_hub_sat AS (
    SELECT
        h.location_id,
        s.*
    FROM ref("hub_location") h
    INNER JOIN ref("sat_location") s
    ON h.location_hk = s.location_hk
    QUALIFY lead(s.load_date) OVER (
        PARTITION BY h.location_hk
        ORDER BY s.load_date
    ) IS NULL
),
star_location AS (
    SELECT
        LOCATION_ID,
        load_date,
        location_code,
        city_id,
        state_id,
        country_id,
        district_id,
        zone_id,
        region_id,
        area_id
    FROM location_hub_sat
)
SELECT * 
FROM star_location