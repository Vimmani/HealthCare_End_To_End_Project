import pandas as pd
from deltalake import DeltaTable

# --- PATHS ---
SILVER_FAC_PATH = "s3://healthcare-etoe-proj-silver-bucket/transactions/FY_2024_SNF_VBP_Facility_Performance/"
SILVER_PROVINFO_PATH = "s3://healthcare-etoe-proj-silver-bucket/transactions/NH_ProviderInfo_Oct2024/"
GOLD_PATH    = "s3://healthcare-etoe-proj-gold-bucket/patient_throughput_metrics/"

prov_df = DeltaTable(SILVER_PROVINFO_PATH).to_pandas()
fac_df = DeltaTable(SILVER_FAC_PATH).to_pandas()



#  Join the datasets with CCN
merged_df = pd.merge(prov_df,fac_df, on='CMS_Certification_Number_CCN', how='inner')

# Convert to Numeric
merged_df['Reported_Total_Nurse_Staffing_Hours_per_Resident_per_Day'] = pd.to_numeric(merged_df['Reported_Total_Nurse_Staffing_Hours_per_Resident_per_Day'], errors='coerce')
merged_df['Performance_Period_FY_2022_Risk_Standardized_Readmission_Rate'] = pd.to_numeric(merged_df['Performance_Period_FY_2022_Risk_Standardized_Readmission_Rate'], errors='coerce')

# Filter Outliers (Standard Reporting Error Cleanup)
# Filters out facilities reporting unrealistic staffing levels (e.g., <1 hr or >15 hrs)
merged_df = merged_df[(merged_df['Reported_Total_Nurse_Staffing_Hours_per_Resident_per_Day'] > 1) & (merged_df['Reported_Total_Nurse_Staffing_Hours_per_Resident_per_Day'] < 15)]

# Perform the Correlation
correlation = merged_df['Reported_Total_Nurse_Staffing_Hours_per_Resident_per_Day'].corr(merged_df['Performance_Period_FY_2022_Risk_Standardized_Readmission_Rate'])

# Output Results
print(f"Correlation between 2024 Staffing and 2022 Readmissions: {correlation:.4f}")
print(f"Analysis based on {len(merged_df)} valid facilities.")
                         
print(merged_df)
