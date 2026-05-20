import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import sum as _sum, avg , col, round
from pyspark.sql.functions import col, coalesce, lit, when, corr


# Initialize Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- PATHS ---
SILVER_FAC_PATH = "s3://healthcare-etoe-proj-silver-bucket/transactions/FY_2024_SNF_VBP_Facility_Performance/"
SILVER_PROVINFO_PATH = "s3://healthcare-etoe-proj-silver-bucket/transactions/NH_ProviderInfo_Oct2024/"
GOLD_PATH    = "s3://healthcare-etoe-proj-gold-bucket/staffing_readmission_corr_metrics/"

print("🚀 Step 1: Reading clean  Silver Layer...")
Prov_df = spark.read.format("delta").load(SILVER_PROVINFO_PATH)
Prov_df = Prov_df.withColumn("Reported_Total_Nurse_Staffing_Hours_per_Resident_per_Day", coalesce(col("Reported_Total_Nurse_Staffing_Hours_per_Resident_per_Day").cast("double"), lit(0.0)))


Fac_df = spark.read.format("delta").load(SILVER_FAC_PATH)
Fac_df = Fac_df.withColumn("Performance_Period_FY_2022_Risk_Standardized_Readmission_Rate", coalesce(col("Performance_Period_FY_2022_Risk_Standardized_Readmission_Rate").cast("double"), lit(0.0)))

merged_df = Prov_df.alias("prov").join(Fac_df.alias("fac"), on="CMS_Certification_Number_CCN", how="inner")


merged_df = merged_df.drop(
    col("fac.State"), 
    col("fac.Provider_Name"), 
    col("fac.ZIP_Code"), 
    col("fac.Provider_Address"), 
    col("fac.City/Town")
)

# Filter Outliers (Standard Reporting Error Cleanup)
# Filters out facilities reporting unrealistic staffing levels (e.g., <1 hr or >15 hrs)
merged_df = merged_df.filter(
    (col("Reported_Total_Nurse_Staffing_Hours_per_Resident_per_Day") > 1) & 
    (col("Reported_Total_Nurse_Staffing_Hours_per_Resident_per_Day") < 15)
)

# Perform the Correlation
correlation_df = merged_df.select(
    corr(
        "Reported_Total_Nurse_Staffing_Hours_per_Resident_per_Day", 
        "Performance_Period_FY_2022_Risk_Standardized_Readmission_Rate"
    ).alias("Correlation_Coefficient")
)

# --- Save the detailed dataset for your Streamlit visual charts ---
merged_df.coalesce(1).write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_PATH + "facility_metrics/")

# --- Save the isolated correlation summary value ---
correlation_df.coalesce(1).write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_PATH + "correlation_summary/")
    
job.commit()