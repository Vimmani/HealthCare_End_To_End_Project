# Because AWS Glue has its own proprietary Python wrapper libraries (like awsglue.context.GlueContext), running the exact Glue script locally will throw a ModuleNotFoundError.
# To bypass this, you can configure your local script to run on standard open-source PySpark and Delta Lake.
# Step 1: Install Local Spark and Delta LibrariesYou need to install  the open-source Spark engine and Delta extensions onto your computer via your VS Code 
# terminal:bashpip install pyspark delta-spark
# Step 2: Create a Local Test Version of the ScriptCreate a new file in VS Code named silver_master_test.py. We will swap out the awsglue initializers with a standard 
# local SparkSession builder configured to pull the required Delta Lake engine

# This script process the master data file alone.
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, trim, upper, to_date, coalesce, lit, when

# Initialize Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- CONFIGURATION ---
# Point directly to the folder populated by your Controller Job
RAW_MASTER_PATH = "s3://healthcare-etoe-proj-raw-bucket/raw/master/"
SILVER_MASTER_PATH = "s3://healthcare-etoe-proj-silver-bucket/master_staffing_delta/"

print("🚀 Step 1: Reading Master CSV from S3 Raw...")
# Load the raw CSV into a Spark DataFrame
raw_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(RAW_MASTER_PATH)

print("🧹 Step 2: Applying Silver Layer Transformations...")
# Perform data cleansing and standardization
cleaned_df = raw_df \
    .filter(col("PROVNUM").isNotNull()) \
    .withColumn("PROVNUM", trim(col("PROVNUM"))) \
    .withColumn("STATE", upper(trim(col("STATE")))) \
    .withColumn("WorkDate", to_date(col("WorkDate"), "yyyy-MM-dd")) \
    .dropDuplicates(["PROVNUM", "WorkDate"])
    
cleaned_df = cleaned_df \
    .withColumn("TotalNursing_Hours", 
                coalesce(col("Hrs_RN"), lit(0)) + 
                coalesce(col("Hrs_LPN"), lit(0)) + 
                coalesce(col("Hrs_CNA"), lit(0))) \
    .withColumn("TotalAdminNursing_Hours", 
                coalesce(col("Hrs_Rnadmin"), lit(0)) + 
                coalesce(col("Hrs_LPNadmin"), lit(0))) \
    .withColumn("RN_Ratio", when(col("MDScensus") > 0, col("Hrs_RN") / col("MDScensus")).otherwise(lit(0))) \
    .withColumn("CNA_Ratio", when(col("MDScensus") > 0, col("Hrs_CNA") / col("MDScensus")).otherwise(lit(0))) \
    .withColumn("LPN_Ratio", when(col("MDScensus") > 0, col("Hrs_LPN") / col("MDScensus")).otherwise(lit(0)))

print("💾 Step 3: Writing directly to S3 Silver in Delta Format...")
# Save clean dataset. Using 'overwrite' ensures a clean master snapshot.
# 'mergeSchema' protects the pipeline if Google Drive adds columns later.
cleaned_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("mergeSchema", "true") \
    .save(SILVER_MASTER_PATH)
print("✅ Silver Master Job completed successfully.")

job.commit()
