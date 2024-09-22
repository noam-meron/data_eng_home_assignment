-- AGGREGATE_DATA
WITH filtered_data AS (
  SELECT
    m.event_date,
    m.application_id,
    m.country,
    SUM(m.impressions) AS total_impressions,
    SUM(m.clicks) AS total_clicks,
    SUM(m.revenue) AS total_revenue
  FROM mediation_raw_data m
  JOIN applications a
    ON m.application_id = a.application_id
  WHERE
    a.is_eligible = 1
    AND (a.creation_date > '2023-01-01' OR a.record_updated_at > '2023-01-01')
  GROUP BY
    m.event_date, m.application_id, m.country
  HAVING
    total_revenue > 400
),
total_revenue_all_apps AS (
  SELECT
    SUM(revenue) AS total_revenue_all
  FROM mediation_raw_data
)

SELECT
  f.event_date,
  f.application_id,
  f.country,
  f.total_impressions,
  f.total_clicks,
  f.total_revenue,
  ROUND((f.total_revenue * 100.0 / t.total_revenue_all), 2) AS revenue_percentage
FROM filtered_data f
JOIN total_revenue_all_apps t
ORDER BY f.event_date, f.application_id, f.country;
