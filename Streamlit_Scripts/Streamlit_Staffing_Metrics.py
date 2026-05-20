import streamlit as st
import pandas as pd
from deltalake import DeltaTable
import plotly.express as px


# 1. Page Configuration
#st.set_page_config(page_title="CMS Healthcare KPI Dashboard", layout="wide")
st.title("🏥 Healthcare Metrics Dashboard")

# 2. Base S3 Paths (Ensure these match your exact S3 bucket name)
GOLD_BASE = "s3://healthcare-etoe-proj-gold-bucket/gold_staffing_metrics/"
PATH_HOURS = GOLD_BASE + "onduty_hours_summary/"
PATH_HOSPITAL = GOLD_BASE + "nurse_ratio_summary/ratio_by_hospital/"
PATH_STATE = GOLD_BASE + "nurse_ratio_summary/ratio_by_state/"
PATH_DEPT = GOLD_BASE + "nurse_ratio_summary/ratio_by_department/"

# 3. Cached Data Loading Functions (Saves S3 read costs)
@st.cache_data(ttl=600)
def load_delta_table(s3_path):
    try:
        dt = DeltaTable(s3_path)
        df = dt.to_pandas()
        if 'WorkDate' in df.columns:
            df['WorkDate'] = pd.to_datetime(df['WorkDate'])
        return df
    except Exception as e:
        st.error(f"Error loading path {s3_path.split('/')[-2]}: {e}")
        return pd.DataFrame()

# Fetch all tables up front
df_hours = load_delta_table(PATH_HOURS)
df_hospital = load_delta_table(PATH_HOSPITAL)
df_state = load_delta_table(PATH_STATE)
df_dept = load_delta_table(PATH_DEPT)

# 4. Construct Dashboard if data exists
if not df_hours.empty:
    
    # Create functional UI tabs to split the metrics cleanly
    tab1, tab2, tab3, tab4 = st.tabs([
        "🕒 Onduty Hours", 
        "🏥 Ratio by Hospital", 
        "🗺️ Ratio by State", 
        "🏢 Ratio by Department"
    ])
    
    # --- TAB 1: ONDUTY HOURS ---
    with tab1:
        st.subheader("Total Onduty Staffing Hours Analysis")
        # Global metric cards
        total_hours = df_hours['Total_RN_Hours'].sum() if 'Total_RN_Hours' in df_hours.columns else 0
        st.metric(label="Total Aggregated RN Onduty Hours", value=f"{total_hours:,.2f}")
        
        # Time Series plot
        if 'WorkDate' in df_hours.columns and 'STATE' in df_hours.columns:
            fig_hours = px.line(df_hours, x='WorkDate', y='Total_RN_Hours', color='STATE', 
                                title="RN Hours Onduty Hours by State", markers=True)
            st.plotly_chart(fig_hours, use_container_width=True)
        st.dataframe(df_hours, use_container_width=True)

    # --- TAB 2: RATIO BY HOSPITAL ---
    with tab2:
        st.subheader("Average Nurse-to-Patient Ratio by Hospital Facility")
        if not df_hospital.empty:
            # Let user search/filter by specific Hospital Name
            all_hospitals = sorted(df_hospital['PROVNAME'].unique()) if 'PROVNAME' in df_hospital.columns else []
            selected_hosp = st.multiselect("Filter by Hospital Name", all_hospitals, default=all_hospitals[:5])
            
            filtered_hosp = df_hospital[df_hospital['PROVNAME'].isin(selected_hosp)] if selected_hosp else df_hospital
            
            fig_hosp = px.bar(filtered_hosp, x='PROVNAME', y='Avg_Nurse_Ratio_ByHospital', 
                              color='PROVNAME', title="Top Facilities Staffing Proportions")
            st.plotly_chart(fig_hosp, use_container_width=True)
            st.dataframe(filtered_hosp, use_container_width=True)

    # --- TAB 3: RATIO BY STATE ---
    with tab3:
        st.subheader("Average Nurse-to-Patient Ratio by State")
        if not df_state.empty:
            fig_state = px.bar(df_state, x='STATE', y='Avg_Nurse_Ratio_ByState', 
                               color='STATE', title="Statewide Staffing Comparison")
            st.plotly_chart(fig_state, use_container_width=True)
            st.dataframe(df_state, use_container_width=True)

    # --- TAB 4: RATIO BY DEPARTMENT ---
    with tab4:
        st.subheader("Average Nurse-to-Patient Ratio by Departments/ Nurse Types")
        if not df_dept.empty:
    
                st.dataframe(df_dept, use_container_width=True)
else:
    st.info("Dashboard loading failed. Check your local terminal to see AWS credential errors.")
