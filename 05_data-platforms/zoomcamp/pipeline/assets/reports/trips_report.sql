/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table

columns:
  - name: date
    type: DATE
    description: "Date of the trip (derived from pickup_datetime)"
    primary_key: true
  - name: pickup_location_id
    type: BIGINT
    description: "Pickup location ID"
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: "Number of trips"
    checks:
      - name: non_negative
  - name: total_fare
    type: DOUBLE
    description: "Total fare amount"

@bruin */

SELECT
    DATE(pickup_datetime) AS date,
    pickup_location_id,
    COUNT(*) AS trip_count,
    SUM(fare_amount) AS total_fare
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY date, pickup_location_id