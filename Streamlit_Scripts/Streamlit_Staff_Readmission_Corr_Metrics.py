import streamlit as st
import pandas as pd
from deltalake import DeltaTable
import plotly.express as px
from pyspark.sql.functions import col, coalesce, lit, when


# 1. Page Configuration
#st.set_page_config(page_title="CMS Healthcare KPI Dashboard", layout="wide")
st.title("🏥 Healthcare Metrics Dashboard")

# 2. Base S3 Paths (Ensure these match your exact S3 bucket name)
GOLD_FACILITY_METRICS_PATH = "s3://healthcare-etoe-proj-gold-bucket/staffing_readmission_corr_metrics/facility_metrics/"
GOLD_CORRELATION_SUMMARY_PATH = "s3://healthcare-etoe-proj-gold-bucket/staffing_readmission_corr_metrics/correlation_summary/"

# Load data helper function (Cached to make the dashboard load instantly)
@st.cache_data
def load_gold_data():
    try:
        # Load detailed data for scatter plot
        dt_facility = DeltaTable(GOLD_FACILITY_METRICS_PATH)
        df_facility = dt_facility.to_pandas()
        
        # Load single correlation score
        dt_corr = DeltaTable(GOLD_CORRELATION_SUMMARY_PATH)
        df_corr = dt_corr.to_pandas()
        # Extract the single value from the first row
        corr_val = df_corr["Correlation_Coefficient"].iloc[0]
        
        return df_facility, corr_val
    except Exception as e:
        st.error("Failed to fetch data from AWS S3 Gold Layer. Please verify your credentials or network.")
        st.exception(e)
        return None, None
    
    # Fetch data
df, correlation_score = load_gold_data()

if df is not None:
    # 3. Sidebar Filter by State (Optional addition for great interactivity)
    states = ["All States"] + sorted(list(df["State"].dropna().unique()))
    selected_state = st.sidebar.selectbox("🗺️ Filter by State:", states)
    
    if selected_state != "All States":
        df_filtered = df[df["State"] == selected_state]
    else:
        df_filtered = df.copy()
    
    # df_filtered = df.copy()

    # 4. Display Macro Metrics Top Cards
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Total Facilities Analyzed", value=f"{len(df_filtered):,}")
    with col2:
        # Format the Pearson correlation coefficient calculated by your Glue job
        st.metric(label="Global Correlation Coefficient (r)", value=f"{correlation_score:.4f}")
    with col3:
        avg_staffing = df_filtered["Reported_Total_Nurse_Staffing_Hours_per_Resident_per_Day"].mean()
        st.metric(label="Avg Staffing Hours / Day", value=f"{avg_staffing:.2f} hrs")

    # 5. Generate Scatter Plot with a Trendline
    st.write("---")
    
    # Plotly handles the math for the linear trendline using the 'trendline="ols"' flag
    fig = px.scatter(
        df_filtered,
        x="Reported_Total_Nurse_Staffing_Hours_per_Resident_per_Day",
        y="Performance_Period_FY_2022_Risk_Standardized_Readmission_Rate",
        trendline="ols",
        trendline_color_override="red",  # Make the trendline pop out
        hover_name="Provider_Name",      # Hovering over a dot reveals the specific nursing home name
        hover_data=["State"],
        title= f"Nurse Staffing vs. Readmission Rate ({selected_state})",
        labels={
            "Reported_Total_Nurse_Staffing_Hours_per_Resident_per_Day": "Total Nurse Staffing Hours (per Resident/Day)",
            "Performance_Period_FY_2022_Risk_Standardized_Readmission_Rate": "Readmission Rate (%)"
        },
        opacity=0.6 # Make dots slightly transparent so overlapping clusters are readable
    )
    
    # Style customization
    fig.update_layout(
        plot_bgcolor="white",
        xaxis=dict(showgrid=True, gridcolor="whitesmoke"),
        yaxis=dict(showgrid=True, gridcolor="whitesmoke")
    )
    
    # Render Plotly to Streamlit
    st.plotly_chart(fig, use_container_width=True)

    # 6. Preview Data Raw Layout Option
    with st.expander("🔎 View Filtered Dataset Table"):
        st.dataframe(df_filtered, use_container_width=True)