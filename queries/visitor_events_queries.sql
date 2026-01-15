-- BigQuery Queries for Visitor Events
-- Database: dataland-backend
-- Dataset: dl_staging
-- Table: visitor_events

-- ============================================
-- 1. GET ALL EVENTS FOR A SPECIFIC VISITOR (by device_id)
-- ============================================
-- Replace 'YOUR_DEVICE_ID' with the actual device ID

SELECT 
    event_timestamp,
    device_id,
    ticket_id,
    position_x,
    position_y,
    zone,
    heart_rate,
    skin_conductivity,
    skin_temperature,
    accelerometer_x,
    accelerometer_y,
    accelerometer_z
FROM 
    `dataland-backend.dl_staging.visitor_events`
WHERE 
    device_id = 'YOUR_DEVICE_ID'
ORDER BY 
    event_timestamp DESC;


-- ============================================
-- 2. GET RECENT EVENTS FOR A VISITOR (last 24 hours)
-- ============================================
SELECT 
    event_timestamp,
    device_id,
    ticket_id,
    position_x,
    position_y,
    zone,
    heart_rate,
    skin_conductivity,
    skin_temperature,
    accelerometer_x,
    accelerometer_y,
    accelerometer_z
FROM 
    `dataland-backend.dl_staging.visitor_events`
WHERE 
    device_id = 'YOUR_DEVICE_ID'
    AND event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
ORDER BY 
    event_timestamp DESC;


-- ============================================
-- 3. GET VISITOR EVENTS WITH SPECIFIC TIME RANGE
-- ============================================
SELECT 
    event_timestamp,
    device_id,
    ticket_id,
    position_x,
    position_y,
    zone,
    heart_rate,
    skin_conductivity,
    skin_temperature,
    accelerometer_x,
    accelerometer_y,
    accelerometer_z
FROM 
    `dataland-backend.dl_staging.visitor_events`
WHERE 
    device_id = 'YOUR_DEVICE_ID'
    AND event_timestamp BETWEEN TIMESTAMP('2025-08-01 00:00:00') 
                            AND TIMESTAMP('2025-08-05 23:59:59')
ORDER BY 
    event_timestamp DESC;


-- ============================================
-- 4. GET VISITOR BIOMETRIC DATA SUMMARY
-- ============================================
SELECT 
    device_id,
    ticket_id,
    COUNT(*) as total_events,
    MIN(event_timestamp) as first_event,
    MAX(event_timestamp) as last_event,
    AVG(heart_rate) as avg_heart_rate,
    MIN(heart_rate) as min_heart_rate,
    MAX(heart_rate) as max_heart_rate,
    AVG(skin_conductivity) as avg_skin_conductivity,
    AVG(skin_temperature) as avg_skin_temperature
FROM 
    `dataland-backend.dl_staging.visitor_events`
WHERE 
    device_id = 'YOUR_DEVICE_ID'
    AND heart_rate IS NOT NULL
GROUP BY 
    device_id, ticket_id;


-- ============================================
-- 5. GET VISITOR MOVEMENT PATTERN (Position History)
-- ============================================
SELECT 
    event_timestamp,
    device_id,
    ticket_id,
    position_x,
    position_y,
    zone,
    LAG(position_x) OVER (PARTITION BY device_id ORDER BY event_timestamp) as prev_x,
    LAG(position_y) OVER (PARTITION BY device_id ORDER BY event_timestamp) as prev_y,
    LAG(zone) OVER (PARTITION BY device_id ORDER BY event_timestamp) as prev_zone,
    TIMESTAMP_DIFF(
        event_timestamp, 
        LAG(event_timestamp) OVER (PARTITION BY device_id ORDER BY event_timestamp), 
        SECOND
    ) as seconds_since_last_event
FROM 
    `dataland-backend.dl_staging.visitor_events`
WHERE 
    device_id = 'YOUR_DEVICE_ID'
    AND position_x IS NOT NULL 
    AND position_y IS NOT NULL
ORDER BY 
    event_timestamp DESC;


-- ============================================
-- 6. GET VISITOR ZONE VISITS SUMMARY
-- ============================================
SELECT 
    device_id,
    zone,
    COUNT(*) as visit_count,
    MIN(event_timestamp) as first_visit,
    MAX(event_timestamp) as last_visit,
    AVG(heart_rate) as avg_heart_rate_in_zone
FROM 
    `dataland-backend.dl_staging.visitor_events`
WHERE 
    device_id = 'YOUR_DEVICE_ID'
    AND zone IS NOT NULL
GROUP BY 
    device_id, zone
ORDER BY 
    visit_count DESC;


-- ============================================
-- 7. GET VISITOR ACTIVITY LEVEL (based on accelerometer)
-- ============================================
SELECT 
    event_timestamp,
    device_id,
    ticket_id,
    accelerometer_x,
    accelerometer_y,
    accelerometer_z,
    SQRT(
        POW(accelerometer_x, 2) + 
        POW(accelerometer_y, 2) + 
        POW(accelerometer_z, 2)
    ) as activity_magnitude,
    heart_rate,
    zone
FROM 
    `dataland-backend.dl_staging.visitor_events`
WHERE 
    device_id = 'YOUR_DEVICE_ID'
    AND accelerometer_x IS NOT NULL
ORDER BY 
    event_timestamp DESC;


-- ============================================
-- 8. GET MULTIPLE VISITORS' EVENTS
-- ============================================
SELECT 
    event_timestamp,
    device_id,
    ticket_id,
    position_x,
    position_y,
    zone,
    heart_rate,
    skin_conductivity,
    skin_temperature
FROM 
    `dataland-backend.dl_staging.visitor_events`
WHERE 
    device_id IN ('DEVICE_ID_1', 'DEVICE_ID_2', 'DEVICE_ID_3')
ORDER BY 
    device_id, event_timestamp DESC;


-- ============================================
-- 9. GET VISITOR EVENTS BY TICKET ID
-- ============================================
SELECT 
    event_timestamp,
    device_id,
    ticket_id,
    position_x,
    position_y,
    zone,
    heart_rate,
    skin_conductivity,
    skin_temperature
FROM 
    `dataland-backend.dl_staging.visitor_events`
WHERE 
    ticket_id = 'YOUR_TICKET_ID'
ORDER BY 
    event_timestamp DESC;


-- ============================================
-- 10. GET VISITOR HEALTH ALERTS (abnormal readings)
-- ============================================
SELECT 
    event_timestamp,
    device_id,
    ticket_id,
    zone,
    heart_rate,
    skin_conductivity,
    skin_temperature,
    CASE 
        WHEN heart_rate > 120 THEN 'High Heart Rate'
        WHEN heart_rate < 50 THEN 'Low Heart Rate'
        WHEN skin_temperature > 38 THEN 'High Temperature'
        WHEN skin_temperature < 35 THEN 'Low Temperature'
        ELSE 'Normal'
    END as health_status
FROM 
    `dataland-backend.dl_staging.visitor_events`
WHERE 
    device_id = 'YOUR_DEVICE_ID'
    AND (
        heart_rate > 120 OR 
        heart_rate < 50 OR 
        skin_temperature > 38 OR 
        skin_temperature < 35
    )
ORDER BY 
    event_timestamp DESC;


-- ============================================
-- 11. GET LATEST EVENT FOR EACH VISITOR
-- ============================================
WITH latest_events AS (
    SELECT 
        device_id,
        MAX(event_timestamp) as latest_timestamp
    FROM 
        `dataland-backend.dl_staging.visitor_events`
    GROUP BY 
        device_id
)
SELECT 
    ve.*
FROM 
    `dataland-backend.dl_staging.visitor_events` ve
INNER JOIN 
    latest_events le 
    ON ve.device_id = le.device_id 
    AND ve.event_timestamp = le.latest_timestamp
ORDER BY 
    ve.event_timestamp DESC;


-- ============================================
-- 12. GET VISITOR PATH THROUGH ZONES
-- ============================================
WITH zone_transitions AS (
    SELECT 
        device_id,
        ticket_id,
        zone,
        event_timestamp,
        LAG(zone) OVER (PARTITION BY device_id ORDER BY event_timestamp) as previous_zone,
        LEAD(zone) OVER (PARTITION BY device_id ORDER BY event_timestamp) as next_zone
    FROM 
        `dataland-backend.dl_staging.visitor_events`
    WHERE 
        device_id = 'YOUR_DEVICE_ID'
        AND zone IS NOT NULL
)
SELECT 
    device_id,
    ticket_id,
    event_timestamp,
    previous_zone,
    zone as current_zone,
    next_zone
FROM 
    zone_transitions
WHERE 
    zone != IFNULL(previous_zone, '')  -- Only show zone changes
ORDER BY 
    event_timestamp;


-- ============================================
-- 13. HOURLY ACTIVITY SUMMARY FOR A VISITOR
-- ============================================
SELECT 
    DATE(event_timestamp) as event_date,
    EXTRACT(HOUR FROM event_timestamp) as hour,
    device_id,
    COUNT(*) as events_count,
    COUNT(DISTINCT zone) as zones_visited,
    AVG(heart_rate) as avg_heart_rate,
    AVG(skin_temperature) as avg_temperature,
    MIN(position_x) as min_x,
    MAX(position_x) as max_x,
    MIN(position_y) as min_y,
    MAX(position_y) as max_y
FROM 
    `dataland-backend.dl_staging.visitor_events`
WHERE 
    device_id = 'YOUR_DEVICE_ID'
GROUP BY 
    event_date, hour, device_id
ORDER BY 
    event_date DESC, hour DESC;


-- ============================================
-- 14. FIND VISITORS IN SPECIFIC ZONE AT GIVEN TIME
-- ============================================
SELECT DISTINCT
    device_id,
    ticket_id,
    zone,
    MAX(event_timestamp) as last_seen
FROM 
    `dataland-backend.dl_staging.visitor_events`
WHERE 
    zone = 'YOUR_ZONE_NAME'
    AND event_timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE) 
                            AND CURRENT_TIMESTAMP()
GROUP BY 
    device_id, ticket_id, zone
ORDER BY 
    last_seen DESC;


-- ============================================
-- 15. CALCULATE DISTANCE TRAVELED BY VISITOR
-- ============================================
WITH position_changes AS (
    SELECT 
        device_id,
        event_timestamp,
        position_x,
        position_y,
        LAG(position_x) OVER (PARTITION BY device_id ORDER BY event_timestamp) as prev_x,
        LAG(position_y) OVER (PARTITION BY device_id ORDER BY event_timestamp) as prev_y
    FROM 
        `dataland-backend.dl_staging.visitor_events`
    WHERE 
        device_id = 'YOUR_DEVICE_ID'
        AND position_x IS NOT NULL 
        AND position_y IS NOT NULL
)
SELECT 
    device_id,
    COUNT(*) as position_updates,
    SUM(
        SQRT(
            POW(position_x - prev_x, 2) + 
            POW(position_y - prev_y, 2)
        )
    ) as total_distance_traveled,
    MIN(event_timestamp) as start_time,
    MAX(event_timestamp) as end_time,
    TIMESTAMP_DIFF(MAX(event_timestamp), MIN(event_timestamp), MINUTE) as duration_minutes
FROM 
    position_changes
WHERE 
    prev_x IS NOT NULL 
    AND prev_y IS NOT NULL
GROUP BY 
    device_id;

