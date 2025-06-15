1. Launch Performance Over Time
SELECT total_successful_launches/total_launches
FROM agg_spacex_launches
ORDER BY aggregation_year ASC;

2. Top Payload Masses
SELECT *
FROM spacex_launches
ORDER BY payload_mass DESC
LIMIT 5

3. Time Between Engine Test And Actual Launch
SELECT aggregation_year, average_delay_hours
FROM agg_spacex_launches
-- TODO: add max delay time to agg table

4. Launch Site Utilization
-- TODO: didn't find what "launch site" refers to in the payload, but if I would have found it and saved it under "launch_site" in the fact table:
SELECT launch_site, count(1) as n_launches, AVG(payload_mass) as average_payload_mass
FROM spacex_launches
GROUP BY launch_site
