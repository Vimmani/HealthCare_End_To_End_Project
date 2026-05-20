import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import sum as _sum, avg , col, round
from pyspark.sql.functions import col, coalesce, lit, when


# Initialize Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- PATHS ---
SILVER_PROVIDER_INFO_PATH = "s3://healthcare-etoe-proj-silver-bucket/transactions/NH_ProviderInfo_Oct2024/"
SILVER_QUALITY_MDS_PATH = "s3://healthcare-etoe-proj-silver-bucket/transactions/NH_QualityMsr_MDS_Oct2024/"
GOLD_PATH    = "s3://healthcare-etoe-proj-gold-bucket/bed_utilization_rate_metrics/"


print("🚀 Step 1: Reading clean Master data from Silver Layer...")
provdier_df = spark.read.format("delta").load(SILVER_PROVIDER_INFO_PATH)
QualityMsr_df = spark.read.format("delta").load(SILVER_QUALITY_MDS_PATH)

# -----------------------------------------------------------
# METRICS : Bed Utilization Rates By Hospital And Department
# -----------------------------------------------------------
# Bed utilization Rate % = (Avg No Of Residents per Day)/Number OF Certified Beds X 100

merged_df = provdier_df.alias("prov_df").join(QualityMsr_df.alias("Qual_df"), on ='CMS_Certification_Number_CCN', how='inner')

merged_df = merged_df.withColumn("Bed_Utilization_Rate", (col("Average_Number_of_Residents_per_Day") / col("Number_OF_Certified_Beds")) * 100)
# delete one df State to avoid ambiguity
merged_df = merged_df.drop("Qual_df.State") 

# Bed Utilization Rates By Hospital
bed_util_by_hospital = merged_df.groupBy("prov_df.Provider_Name", "prov_df.State").agg(round(avg(col("Bed_Utilization_Rate")), 2).alias("Bed_Utilization_By_Hospital"))

# Bed Utilization Rates By Department
bed_util_by_department = merged_df.groupBy("Qual_df.Resident_type").agg(round(avg(col("Bed_Utilization_Rate")), 2).alias("Bed_Utilization_By_Department"))

bed_util_by_hospital.coalesce(1).write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_PATH + "utilization_by_hospital/")

bed_util_by_department.coalesce(1).write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_PATH+ "utilization_by_department/")


print("✅ Both Gold Summary Tables written successfully!")
job.commit()