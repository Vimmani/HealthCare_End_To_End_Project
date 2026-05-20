import streamlit as st
import pandas as pd
from deltalake import DeltaTable
import plotly.express as px

# 1. Page Configuration
#st.set_page_config(page_title="CMS Healthcare KPI Dashboard", layout="wide")
st.title("🏥 Healthcare Metrics Dashboard")

# 2. Base S3 Paths (Ensure these match your exact S3 bucket name)
GOLD_BASE = "s3://healthcare-etoe-proj-gold-bucket/bed_utilization_rate_metrics/"
PATH_HOSPITAL = GOLD_BASE + "utilization_by_hospital/"
PATH_DEPT = GOLD_BASE + "utilization_by_department/"


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
df_hospital = load_delta_table(PATH_HOSPITAL)
df_dept = load_delta_table(PATH_DEPT)


# 4. Construct Dashboard if data exists
if not df_hospital.empty:
    # 1. Create the Multiselect dropdown component
    # By default, we pre-populate it with the first 5 hospitals so the chart isn't empty at start
    all_hospitals = list(df_hospital["Provider_Name"].unique())
    default_selection = all_hospitals[:5]


########################### Bed Utilization by Hospital  ###############################
# 1. First dropdown narrows down the pool (e.g., to ~50 states)
# with tab1:
    selected_state = st.selectbox(
        "1. Select State:", options=df_hospital["State"].unique()
    )

# 2. Filter the original dataset down to just that state's hospitals
    state_hospitals = df_hospital[df_hospital["State"] == selected_state]["Provider_Name"].unique()

# 3. Now st.multiselect only has to load a tiny fraction of the data!.
    selected_hospitals = st.multiselect("2. Select Specific Hospitals to Compare:", options=list(state_hospitals))

# Final dataframe filter
    df_filtered = df_hospital[df_hospital["Provider_Name"].isin(selected_hospitals)]

# 2. Filter your DataFrame based on the user's selected tags
# If they clear all tags, show everything so the chart doesn't break
    if not selected_hospitals:
        df_filtered = df_hospital.copy()
    else:
        df_filtered = df_hospital[df_hospital["Provider_Name"].isin(selected_hospitals)]

# 3. Create your clean horizontal bar chart using the filtered data
    if not df_filtered.empty:
       fig_hosp = px.bar(
          df_filtered,
          x="Bed_Utilization_By_Hospital",
          y="Provider_Name",
          orientation="h",
          color="Provider_Name",  # Matches the discrete coloring by hospital like your image
          labels={
              "Bed_Utilization_By_Hospital": "Avg Utilization (%)",
              "Provider_Name": "Hospital",
          },
        )

       fig_hosp.update_layout(showlegend=True)
       st.plotly_chart(fig_hosp, use_container_width=True)
       st.dataframe(df_hospital, use_container_width=True)
    else:
     st.warning("Please select at least one hospital facility.")

