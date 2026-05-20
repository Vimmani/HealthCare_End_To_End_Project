#This below script may not run in local VS Code :  Because AWS Glue has its own proprietary Python wrapper libraries (like awsglue.context.GlueContext), running the exact Glue script locally will throw a ModuleNotFoundError.
# Handling - NH_ProviderInfo_Oct2024.csv

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, trim, current_timestamp, coalesce, lit

# Initialize Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- S3 PATH CONFIGURATIONS ---
# Specific source file and targeted destination subfolder
INPUT_FILE_PATH = "s3://healthcare-etoe-proj-raw-bucket/raw/transactions/NH_ProviderInfo_Oct2024.csv"
OUTPUT_DELTA_PATH = "s3://healthcare-etoe-proj-silver-bucket/transactions/NH_ProviderInfo_Oct2024/"

print("🚀 Step 1: Reading NH_ProviderInfo_Oct2024.csv with Glue Bookmarks...")
# Read using Glue DynamicFrame to natively leverage Job Bookmarks
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [INPUT_FILE_PATH]},
    format="csv",
    format_options={"withHeader": True},
    transformation_ctx="bookmark_ctx_provider_info" # Unique identifier for tracking this file's state
)

# Process only if the bookmark indicates new/updated data is present
if dynamic_frame.count() > 0:
    print("🧹 Step 2: Applying targeted schema conversions and space cleaning...")
    df = dynamic_frame.toDF() # Converting in to spark df
    
    # Global Trim: Standardize whitespace across ALL incoming columns dynamically
    # Number_of_Certified_Beds
    for col_name in df.columns:
        df = df.withColumn(col_name, trim(col(col_name)))
        column_name_format = col_name.replace(" ","_").replace("-","_").replace("(","").replace(")","")
        df = df.withColumnRenamed(col_name,column_name_format)
        
        

    # Targeted Datatype Transformations
    # 1. Cast CCN directly to String
    # 2. Cast metrics to Float (Numeric) so they are completely safe for division later
    # 3. Average_Number_of_Residents_per_Day have missing values but I am not converting them to 0.0 as in the gold layer division it will drag the oveall bed utilization rate. If you want to do that
     #.withColumn("Average_Number_of_Residents_per_Day", coalesce(col("Average_Number_of_Residents_per_Day"), lit(0.0))) \
    cleaned_df = df \
        .withColumn("CMS_Certification_Number_CCN", col("CMS_Certification_Number_CCN").cast("string")) \
        .withColumn("Average_Number_of_Residents_per_Day", col("Average_Number_of_Residents_per_Day").cast("float")) \
        .withColumn("Number_of_Certified_Beds", col("Number_of_Certified_Beds").cast("float")) \
        .withColumn("silver_ingested_at", current_timestamp())
        
    #cleaned_df = cleaned_df.withColumnRenamed("CMS Certification Number (CCN)", "CMS_Certification_Number_CCN")
        
    print("💾 Step 3: Appending data directly to Silver S3 Delta Layer...")
    # Mode append acts dynamically with Job Bookmarks to continuously layer increments over time
    cleaned_df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(OUTPUT_DELTA_PATH)
        
    print("✅ Ingestion and transformation successful for Provider Info.")
else:
    print("skip ⏭️ No new incremental blocks discovered by Bookmarks for this dataset.")

# Commit bookmark tracking state
job.commit()