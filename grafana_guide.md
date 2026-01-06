# Grafana Configuration Guide

## 1. Login to Grafana
- **URL**: [http://localhost:3000](http://localhost:3000)
- **Default Credentials**: `admin` / `admin` (You will be asked to skip changing password).

## 2. Connect Data Source (PostgreSQL)
1. Go to **Connections** > **Data Sources** > **+ Add new data source**.
2. Search for **PostgreSQL**.
3. Configure the connection:
   - **Host**: `postgres:5432`
   - **Database**: `traffic_data`
   - **User**: `spark`
   - **Password**: `spark`
   - **TLS/SSL Mode**: `disable` (internal docker network).
4. Click **Save & Test**. You should see "Database Connection OK".

## 3. Creating the Dashboard
1. Go to **Dashboards** > **New Dashboard**.
2. **Add a Visualization**.

### KPI 1: Average Traffic by Zone (Bar Chart)
- **Table**: `zone_metrics`
- **Query**:
  ```sql
  SELECT
    processing_time AS "time",
    zone,
    avg_vehicle_count
  FROM zone_metrics
  WHERE $__timeFilter(processing_time)
  ORDER BY processing_time DESC
  ```
- **Visual**: Bar Chart.

### KPI 2: Average Speed (Gauge or Stat)
- **Table**: `road_metrics`
- **Query**:
  ```sql
  SELECT
    avg(avg_speed)
  FROM road_metrics
  WHERE $__timeFilter(processing_time)
  ```
- **Visual**: Stat.

### KPI 3: Congestion Rate (Pie Chart)
- **Table**: `congestion_alerts`
- **Query**:
  ```sql
  SELECT
    zone,
    sum(congestion_events)
  FROM congestion_alerts
  WHERE $__timeFilter(processing_time)
  GROUP BY zone
  ```

### Visualization: Timeline / Graph
- **Query**:
  ```sql
  SELECT
    processing_time AS "time",
    avg_vehicle_count,
    zone
  FROM zone_metrics
  WHERE $__timeFilter(processing_time)
  ORDER BY processing_time
  ```
- **Visual**: Time Series.

## 4. Interpreting Results
- **Peaks**: Identify hours with highest `avg_vehicle_count`.
- **Critical Zones**: Use the congestion alerts panel to spot recurring problem areas.
