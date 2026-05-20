# This will act as consolidate trigger script of all different streamlit data visulaization scripts 

import streamlit as st

# Define your individual scripts as Page objects
bed_util_page = st.Page("Streamlit_BedUtil_Metrics.py", title="Bed Utilization", icon="🏥")
throughput_page = st.Page("Streamlit_Patient_ThroughPut_Metrics.py", title="Patient Throughput", icon="📊")
corr_page = st.Page("Streamlit_Staff_Readmission_Corr_Metrics.py", title="Staffing & Readmission Correlation", icon="📈")
staffing_page = st.Page("Streamlit_Staffing_Metrics.py", title="Staffing Overview", icon="👩‍⚕️")

# Create the sidebar navigation structure
pg = st.navigation([staffing_page, throughput_page, corr_page,bed_util_page])

# Configure global page properties
st.set_page_config(page_title="Healthcare Metrics Dashboard", layout="wide")

# Run the selected page dynamically.
pg.run()