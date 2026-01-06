import streamlit as st
import pandas as pd
from hdfs import InsecureClient
import plotly.express as px
import os
import json
import io

# Setup HDFS Client
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")

@st.cache_resource
def get_hdfs_client():
    try:
        # Note: InsecureClient needs the namenode host to be resolvable.
        # Inside docker, 'namenode' resolves. Locally, you might need /etc/hosts entry or port forwarding.
        return InsecureClient(HDFS_URL, user=HDFS_USER)
    except Exception as e:
        st.error(f"Failed to connect to HDFS: {e}")
        return None

def read_json_files(client, hdfs_path):
    """Reads all JSON part files from a directory in HDFS into a Pandas DataFrame."""
    try:
        files = client.list(hdfs_path)
        data = []
        for file in files:
            if file.startswith("part-") and file.endswith(".json"):
                with client.read(f"{hdfs_path}/{file}") as reader:
                    # Spark saves JSON lines, so we read line by line
                    content = reader.read().decode('utf-8')
                    for line in content.strip().split('\n'):
                        try:
                            data.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
        return pd.DataFrame(data)
    except Exception as e:
        st.warning(f"Could not read data from {hdfs_path}. Error: {e}")
        return pd.DataFrame()

def main():
    st.set_page_config(page_title="Urban Traffic Analytics", layout="wide")
    st.title("🚦 Urban Traffic Analytics Dashboard")
    st.markdown("Real-time monitoring and analytics of urban traffic flow.")

    client = get_hdfs_client()
    if not client:
        st.stop()

    # Load Data
    with st.spinner("Loading data from Data Lake..."):
        zone_stats = read_json_files(client, "/data/processed/traffic/zone_stats")
        road_stats = read_json_files(client, "/data/processed/traffic/road_stats")
        congestion_stats = read_json_files(client, "/data/processed/traffic/congestion_stats")

    # Layout: Top Metrics
    col1, col2, col3 = st.columns(3)
    
    total_vehicles = 0
    avg_speed_global = 0
    
    if not zone_stats.empty:
        total_vehicles = zone_stats['avg_vehicle_count'].sum() # Approximation for demo
        
    if not road_stats.empty:
        avg_speed_global = road_stats['avg_speed'].mean()

    col1.metric("Traffic Intensity Index", f"{int(total_vehicles)}")
    col2.metric("Global Average Speed", f"{avg_speed_global:.2f} km/h")
    col3.metric("Active Zones", len(zone_stats) if not zone_stats.empty else 0)

    # Charts
    st.markdown("---")
    
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Traffic Volume by Zone")
        if not zone_stats.empty:
            fig_vol = px.bar(zone_stats, x="zone", y="avg_vehicle_count", 
                             color="avg_vehicle_count", title="Average Vehicle Count per Zone",
                             color_continuous_scale="Viridis")
            st.plotly_chart(fig_vol, use_container_width=True)
        else:
            st.info("No zone data available.")

    with c2:
        st.subheader("Occupancy Rate by Zone")
        if not zone_stats.empty:
            fig_occ = px.bar(zone_stats, x="zone", y="avg_occupancy_rate",
                             color="avg_occupancy_rate", title="Average Occupancy (%)",
                             color_continuous_scale="Reds")
            st.plotly_chart(fig_occ, use_container_width=True)
        else:
            st.info("No occupancy data available.")

    # Congestion Alerts
    st.markdown("---")
    st.subheader("🚨 Congestion Alerts")
    
    if not congestion_stats.empty:
        # High congestion zones
        for _, row in congestion_stats.iterrows():
            st.error(f"High Congestion detected in **{row['zone']}**! ({row['congestion_events']} high occupancy events logged)")
    else:
        st.success("No active congestion alerts. Traffic flow is normal.")

    # Detailed Data View
    with st.expander("View Raw Data Statistics"):
        st.write("Zone Statistics", zone_stats)
        st.write("Road Statistics", road_stats)

if __name__ == "__main__":
    main()
