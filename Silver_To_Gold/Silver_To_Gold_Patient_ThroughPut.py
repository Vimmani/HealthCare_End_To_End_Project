# "Filter "Skilled_Nursing_Facility_Quality_Reporting_Program_Provider_Data_Oct2024.csv with Measure Code for S_005_02_DTC_VOLUME.
# S_005_02_DTC_VOLUME (Discharge to Community):  It specifically counts patients who completed their rehab and went home. 
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import Window
from pyspark.sql.functions import sum as _sum, avg , col, round
from pyspark.sql.functions import col, coalesce, lit, when, row_number


# Initialize Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- PATHS ---
SILVER_SKILLED_PATH = "s3://healthcare-etoe-proj-silver-bucket/transactions/Skilled_Nursing_Facility_Quality_Reporting_Program_Provider_Data_Oct2024/"
SILVER_PROVINFO_PATH = "s3://healthcare-etoe-proj-silver-bucket/transactions/NH_ProviderInfo_Oct2024/"
GOLD_PATH    = "s3://healthcare-etoe-proj-gold-bucket/patient_throughput_metrics/"


print("🚀 Step 1: Reading clean Master data from Silver Layer...") 
Provdier_df = spark.read.format("delta").load(SILVER_PROVINFO_PATH)
# Define the Window Spec to pick the latest ingested record per facility
window_spec_prov = Window.partitionBy("CMS_Certification_Number_CCN").orderBy(Provdier_df["silver_ingested_at"].desc())
Provdier_df = Provdier_df.withColumn("row_num", row_number().over(window_spec_prov)).filter(col("row_num") == 1).drop("row_num")

Provdier_df = Provdier_df.withColumn("Number_of_Certified_Beds", coalesce(col("Number_of_Certified_Beds").cast("double"), lit(0.0)))

Skilled_df = spark.read.format("delta").load(SILVER_SKILLED_PATH)
#"Filter Measure Code for S_005_02_DTC_VOLUME It specifically counts patients who completed their rehab and went home
Skilled_df = Skilled_df.filter(col("Measure_Code") == "S_005_02_DTC_VOLUME")

# Define the Window Spec to pick the latest ingested record per facility.
window_spec_skill = Window.partitionBy("CMS_Certification_Number_CCN").orderBy(Skilled_df["silver_ingested_at"].desc())
Skilled_df = Skilled_df.withColumn("row_num", row_number().over(window_spec_skill)).filter(col("row_num") == 1).drop("row_num")


Skilled_df = Skilled_df.withColumn("Score", coalesce(col("Score").cast("double"), lit(0.0)))


merged_df = Provdier_df.alias("prov").join(Skilled_df.alias("skilled"), on='CMS_Certification_Number_CCN', how='inner')


merged_df = merged_df.withColumn("Patient_Throughput", round(col("Score") / col("Number_of_Certified_Beds"), 2))

merged_df = merged_df.drop(
    col("skilled.State"), 
    col("skilled.Provider_Name"), 
    col("skilled.ZIP_Code"), 
    col("skilled.Telephone_Number"), 
    col("skilled.County/Parish"), 
    col("skilled.City/Town"),
    col("skilled.silver_ingested_at")
)

merged_df.coalesce(1).write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_PATH)
    
job.commit()