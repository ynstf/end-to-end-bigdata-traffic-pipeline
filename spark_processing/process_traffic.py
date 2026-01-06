from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, window, from_json, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("TrafficDataProcessing") \
        .getOrCreate()

    print("Spark Session created.")

    # Define Schema matching the JSON events
    schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("road_id", StringType(), True),
        StructField("road_type", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("vehicle_count", IntegerType(), True),
        StructField("average_speed", DoubleType(), True),
        StructField("occupancy_rate", DoubleType(), True),
        StructField("event_time", StringType(), True)
    ])

    # Read data from HDFS
    # Using recursiveFileLookup to read all nested files under the traffic directory
    input_path = "hdfs://namenode:9000/data/raw/traffic"
    print(f"Reading data from {input_path}")
    
    try:
        # Read JSON files
        df = spark.read.option("recursiveFileLookup", "true").schema(schema).json(input_path)
    except Exception as e:
        print(f"Error reading data: {e}")
        return

    if df.rdd.isEmpty():
        print("No data found to process.")
        return

    # Convert event_time to timestamp for better processing if needed, 
    # but for simple aggregations, using it as is or converting is fine.
    # Let's simple aggregate by zone and road.
    
    print("Processing: Average Traffic by Zone")
    avg_traffic_zone = df.groupBy("zone").agg(
        avg("vehicle_count").alias("avg_vehicle_count"),
        avg("occupancy_rate").alias("avg_occupancy_rate")
    )
    print("Processing: Average Speed by Road")
    avg_speed_road = df.groupBy("road_id", "road_type").agg(
        avg("average_speed").alias("avg_speed")
    )

    print("Processing: High Congestion Zones (> 20% occupancy)")
    congestion_zones = df.filter(col("occupancy_rate") > 20) \
        .groupBy("zone").count().withColumnRenamed("count", "congestion_events")
    
    # Persist dataframes since they are written to multiple destinations (HDFS JSON, Parquet, Postgres)
    # This prevents re-computation for each action.
    avg_traffic_zone.cache()
    avg_speed_road.cache()
    congestion_zones.cache()

    # Add timestamp for Grafana time-series
    from pyspark.sql.functions import current_timestamp
    timestamp_col = current_timestamp()
    
    avg_traffic_zone = avg_traffic_zone.withColumn("processing_time", timestamp_col)
    avg_speed_road = avg_speed_road.withColumn("processing_time", timestamp_col)
    congestion_zones = congestion_zones.withColumn("processing_time", timestamp_col)

    # Save results to HDFS
    output_base = "hdfs://namenode:9000/data/processed/traffic"
    
    # Save Zone Stats
    avg_traffic_zone.write.mode("overwrite").json(f"{output_base}/zone_stats")
    print(f"Saved zone stats to {output_base}/zone_stats")

    # Save Road Stats
    avg_speed_road.write.mode("overwrite").json(f"{output_base}/road_stats")
    print(f"Saved road stats to {output_base}/road_stats")

    # Save Congestion Stats
    congestion_zones.write.mode("overwrite").json(f"{output_base}/congestion_stats")
    print(f"Saved congestion stats to {output_base}/congestion_stats")

    # --- Analytics Zone (Parquet) ---
    analytics_base = "hdfs://namenode:9000/data/analytics/traffic"
    
    # Save as Parquet for optimized analytics
    avg_traffic_zone.write.mode("overwrite").parquet(f"{analytics_base}/zone_stats")
    print(f"Saved zone_stats (Parquet) to {analytics_base}/zone_stats")
    
    avg_speed_road.write.mode("overwrite").parquet(f"{analytics_base}/road_stats")
    print(f"Saved road_stats (Parquet) to {analytics_base}/road_stats")
    
    # --- Exploitation Zone (PostgreSQL) ---
    # Add timestamp for time-series analysis in Grafana
    # Note: 'event_time' is already in the data, but let's encourage using a processing time or window time.
    # For simplicity, we just rely on the job run.
    
    jdbc_url = "jdbc:postgresql://postgres:5432/traffic_data"
    jdbc_properties = {
        "user": "spark",
        "password": "spark",
        "driver": "org.postgresql.Driver"
    }
    
    # Save Zone Stats
    avg_traffic_zone.write.jdbc(url=jdbc_url, table="zone_metrics", mode="append", properties=jdbc_properties)
    print(f"Saved zone_metrics to Postgres")

    # Save Road Stats
    avg_speed_road.write.jdbc(url=jdbc_url, table="road_metrics", mode="append", properties=jdbc_properties)
    print(f"Saved road_metrics to Postgres")
    
    # Save Congestion Alerts
    congestion_zones.write.jdbc(url=jdbc_url, table="congestion_alerts", mode="append", properties=jdbc_properties)
    print(f"Saved congestion_alerts to Postgres")
    
    spark.stop()

if __name__ == "__main__":
    main()
