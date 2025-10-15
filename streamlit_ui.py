"""
Step 5: Streamlit UI for monitoring
Run: streamlit run 5_streamlit_ui.py --server.port 8501 --server.address 0.0.0.0
"""
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from datetime import datetime

# Configuration
DB_HOST = os.getenv('DB_HOST', 'timescaledb')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'neo_db')
DB_USER = os.getenv('DB_USER', 'neo_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'neo_password')

# Create database connection
@st.cache_resource
def get_db_engine():
    return create_engine(
        f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    )

@st.cache_data(ttl=60)
def load_raw_data():
    engine = get_db_engine()
    query = """
    SELECT * FROM neo_raw 
    ORDER BY close_approach_date DESC 
    LIMIT 10000
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=60)
def load_processed_data():
    engine = get_db_engine()
    query = """
    SELECT * FROM neo_processed 
    ORDER BY close_approach_date DESC 
    LIMIT 10000
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=60)
def load_daily_metrics():
    engine = get_db_engine()
    query = "SELECT * FROM neo_daily_metrics ORDER BY metric_date"
    return pd.read_sql(query, engine)

@st.cache_data(ttl=60)
def get_summary_stats():
    engine = get_db_engine()
    query = """
    SELECT 
        COUNT(*) as total_asteroids,
        SUM(CASE WHEN is_potentially_hazardous THEN 1 ELSE 0 END) as hazardous_count,
        AVG(risk_score) as avg_risk_score,
        SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomaly_count,
        MIN(close_approach_date) as start_date,
        MAX(close_approach_date) as end_date
    FROM neo_processed
    """
    return pd.read_sql(query, engine).iloc[0]

def main():
    st.set_page_config(
        page_title="NASA NEO Analytics Dashboard",
        page_icon="🌌",
        layout="wide"
    )
    
    st.title("🌌 NASA Near-Earth Object Analytics Dashboard")
    st.markdown("Real-time monitoring of asteroid approaches using Kafka, Spark & TimescaleDB")
    
    # Sidebar
    st.sidebar.header("Navigation")
    page = st.sidebar.radio("Go to", ["Overview", "Raw Data", "Processed Data", "Daily Metrics", "Top Risks"])
    
    st.sidebar.markdown("---")
    st.sidebar.info(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    if page == "Overview":
        show_overview()
    elif page == "Raw Data":
        show_raw_data()
    elif page == "Processed Data":
        show_processed_data()
    elif page == "Daily Metrics":
        show_daily_metrics()
    elif page == "Top Risks":
        show_top_risks()

def show_overview():
    st.header("📊 Overview")
    
    try:
        stats = get_summary_stats()
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Asteroids", f"{int(stats['total_asteroids']):,}")
        with col2:
            st.metric("Hazardous", f"{int(stats['hazardous_count']):,}")
        with col3:
            st.metric("Avg Risk Score", f"{stats['avg_risk_score']:.4f}")
        with col4:
            st.metric("Anomalies", f"{int(stats['anomaly_count']):,}")
        
        st.markdown(f"**Date Range:** {stats['start_date']} to {stats['end_date']}")
        
        st.markdown("---")
        
        daily = load_daily_metrics()
        
        if not daily.empty:
            st.subheader("📈 Daily Trends")
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.line(
                    daily, 
                    x='metric_date', 
                    y='asteroid_count',
                    title='Daily Asteroid Count',
                    labels={'metric_date': 'Date', 'asteroid_count': 'Count'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.line(
                    daily, 
                    x='metric_date', 
                    y='avg_risk_score',
                    title='Average Daily Risk Score',
                    labels={'metric_date': 'Date', 'avg_risk_score': 'Risk Score'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            col3, col4 = st.columns(2)
            
            with col3:
                fig = px.bar(
                    daily, 
                    x='metric_date', 
                    y='hazardous_count',
                    title='Hazardous Asteroids per Day',
                    labels={'metric_date': 'Date', 'hazardous_count': 'Count'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col4:
                fig = px.line(
                    daily, 
                    x='metric_date', 
                    y='min_miss_distance_km',
                    title='Closest Approach Distance (km)',
                    labels={'metric_date': 'Date', 'min_miss_distance_km': 'Distance (km)'}
                )
                st.plotly_chart(fig, use_container_width=True)
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")

def show_raw_data():
    st.header("📋 Raw Data Table")
    
    try:
        df = load_raw_data()
        
        st.write(f"Showing {len(df)} most recent records")
        
        # Filters
        col1, col2 = st.columns(2)
        with col1:
            hazardous_filter = st.checkbox("Show only hazardous")
        with col2:
            search = st.text_input("Search by name")
        
        # Apply filters
        filtered_df = df.copy()
        if hazardous_filter:
            filtered_df = filtered_df[filtered_df['is_potentially_hazardous'] == True]
        if search:
            filtered_df = filtered_df[filtered_df['name'].str.contains(search, case=False, na=False)]
        
        # Display table
        display_cols = [
            'id', 'name', 'close_approach_date', 'is_potentially_hazardous',
            'miss_distance_km', 'relative_velocity_km_s', 
            'estimated_diameter_km_min', 'estimated_diameter_km_max'
        ]
        st.dataframe(
            filtered_df[display_cols],
            use_container_width=True,
            height=600
        )
        
        # Download button
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"neo_raw_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")

def show_processed_data():
    st.header("⚙️ Processed Data Table")
    
    try:
        df = load_processed_data()
        
        st.write(f"Showing {len(df)} most recent processed records")
        
        # Filters
        col1, col2, col3 = st.columns(3)
        with col1:
            risk_level = st.multiselect(
                "Risk Level",
                options=["Very Low", "Low", "Medium", "High", "Very High"],
                default=[]
            )
        with col2:
            anomaly_filter = st.checkbox("Show only anomalies")
        with col3:
            hazardous_filter = st.checkbox("Show only hazardous", key="proc_haz")
        
        # Apply filters
        filtered_df = df.copy()
        if risk_level:
            filtered_df = filtered_df[filtered_df['risk_level'].isin(risk_level)]
        if anomaly_filter:
            filtered_df = filtered_df[filtered_df['is_anomaly'] == True]
        if hazardous_filter:
            filtered_df = filtered_df[filtered_df['is_potentially_hazardous'] == True]
        
        # Display table
        display_cols = [
            'id', 'name', 'close_approach_date', 'is_potentially_hazardous',
            'diameter_mean_km', 'miss_distance_km', 'relative_velocity_km_s',
            'risk_score', 'risk_level', 'is_anomaly'
        ]
        
        # Color code by risk level
        def highlight_risk(row):
            if row['risk_level'] == 'Very High':
                return ['background-color: #ff4444'] * len(row)
            elif row['risk_level'] == 'High':
                return ['background-color: #ff8844'] * len(row)
            elif row['risk_level'] == 'Medium':
                return ['background-color: #ffcc44'] * len(row)
            return [''] * len(row)
        
        st.dataframe(
            filtered_df[display_cols],
            use_container_width=True,
            height=600
        )
        
        # Download button
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"neo_processed_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")

def show_daily_metrics():
    st.header("📅 Daily Aggregated Metrics")
    
    try:
        df = load_daily_metrics()
        
        if df.empty:
            st.warning("No daily metrics available. Run 4_calculate_daily_metrics.py first.")
            return
        
        st.dataframe(
            df,
            use_container_width=True,
            height=600
        )
        
        # Download button
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"neo_daily_metrics_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")

def show_top_risks():
    st.header("⚠️ Top Risk Asteroids")
    
    try:
        df = load_processed_data()
        
        # Top 20 by risk score
        top_risk = df.nlargest(20, 'risk_score')
        
        st.subheader("Top 20 Highest Risk Asteroids")
        st.dataframe(
            top_risk[[
                'name', 'close_approach_date', 'risk_score', 'risk_level',
                'diameter_mean_km', 'miss_distance_km', 'relative_velocity_km_s',
                'is_potentially_hazardous', 'is_anomaly'
            ]],
            use_container_width=True
        )
        
        # Scatter plot
        st.subheader("Risk Score vs Miss Distance")
        fig = px.scatter(
            df,
            x='miss_distance_km',
            y='relative_velocity_km_s',
            color='risk_score',
            size='diameter_mean_km',
            hover_data=['name', 'risk_level'],
            color_continuous_scale='Reds',
            title='Asteroid Risk Profile'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Risk distribution
        col1, col2 = st.columns(2)
        
        with col1:
            risk_counts = df['risk_level'].value_counts()
            fig = px.pie(
                values=risk_counts.values,
                names=risk_counts.index,
                title='Risk Level Distribution'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.histogram(
                df,
                x='risk_score',
                nbins=30,
                title='Risk Score Distribution'
            )
            st.plotly_chart(fig, use_container_width=True)
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")

if __name__ == "__main__":
    main()