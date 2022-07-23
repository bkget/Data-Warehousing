{{ config(materialized='table') }}
SELECT
    distinct(type) as truck_type,
    AVG(avg_speed) as total_avg_speed, 
    count(type) as count_track_type

FROM 
  traffic
  
GROUP BY
  type