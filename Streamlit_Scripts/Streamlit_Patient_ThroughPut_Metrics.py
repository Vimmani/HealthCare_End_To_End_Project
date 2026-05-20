import streamlit as st
import pandas as pd
from deltalake import DeltaTable
import plotly.express as px
from pyspark.sql.functions import col, coalesce, lit, when


# 1. Page Configuration
#st.set_page_config(page_title="CMS Healthcare KPI Dashboard", layout="wide")
st.title("🏥 Healthcare Metrics Dashboard")

# 2. Base S3 Paths (Ensure these match your exact S3 bucket name)
GOLD_BASE = "s3://healthcare-etoe-proj-gold-bucket/patient_throughput_metrics/"


# 3. Cached Data Loading Functions (Saves S3 read costs)
@st.cache_data(ttl=600)
def load_delta_table(s3_path):
    try:
        dt = DeltaTable(s3_path)
        df = dt.to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading path {s3_path.split('/')[-2]}: {e}")
        return pd.DataFrame()


# Fetch all tables up front
patient_df = load_delta_table(GOLD_BASE)

if not patient_df.empty:
    
    top10_df = patient_df.nlargest(10, "Patient_Throughput").sort_values(
        "Patient_Throughput", ascending=False
    )
    
   
    st.subheader("Top Ten Hospitals With Highest Patient Throughput")
    fig = px.bar(
                top10_df,
                x="Patient_Throughput",
                y="Provider_Name",
                orientation="h",
                text="Patient_Throughput",  # Displays the exact number inside/on the bar
                color="Patient_Throughput",  # Adds a color gradient based on value size
                color_continuous_scale="Blues",  # Uses a professional blue color palette
                labels={
                    "Patient_Throughput": "Total Patient Throughput",
                    "Provider_Name": "Nursing Home Facility",
                },
            )
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(top10_df[[ "CMS_Certification_Number_CCN","Provider_Name","Patient_Throughput","State","City/Town"]], use_container_width=True)