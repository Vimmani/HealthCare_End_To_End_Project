# This script handles 'NH_QualityMsr_MDS_Oct2024.csv',

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, trim, current_timestamp

#Intilize the Glue COntext
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
gluecontext = GlueContext(sc)
spark = gluecontext.spark_session
job = Job(gluecontext)
job.init(args['JOB_NAME'], args)


# FilesLocations
INPUT_FILE_PATH = "s3://healthcare-etoe-proj-raw-bucket/raw/transactions/NH_QualityMsr_MDS_Oct2024.csv"
OUTPUT_DELTA_PATH = "s3://healthcare-etoe-proj-silver-bucket/transactions/NH_QualityMsr_MDS_Oct2024/"


# REading the input data file in to glue dynamic dataframe
dynamic_df = gluecontext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [INPUT_FILE_PATH]},
    format="csv",
    format_options={"withHeader": True},
    transformation_ctx="bookmark_ctx_QualityMsr_MDS_info" # Unique identifier for tracking this file's state
)

# Now check if the file is modified if so convert it to spark df

if dynamic_df.count() > 0:
    print("🧹 Step 2: Applying targeted schema conversions and space cleaning...")
    df = dynamic_df.toDF() # Converting in to spark df

    for col_name in df.columns:
            df = df.withColumn(col_name, trim(col(col_name)))
            column_name_format = col_name.replace(" ","_").replace("-","_").replace("(","").replace(")","")
            df = df.withColumnRenamed(col_name,column_name_format)

     # Global Trim: Standardize whitespace across ALL incoming columns dynamically
    for col_name in df.columns:
        # df = df.withColumn(col_name, trim(col(col_name)))
        # column_name_format = col_name.replace(" ","_").replace("-","_").replace("(","").replace(")","")
        # df = df.withColumnRenamed(col_name,column_name_format)

     cleaned_df = df \
        .withColumn("CMS_Certification_Number_CCN", col("CMS_Certification_Number_CCN").cast("string")) \
        .withColumn("silver_ingested_at", current_timestamp())
    
    #  # Mode append acts dynamically with Job Bookmarks to continuously layer increments over time
    print("💾 Step 3: Appending data directly to Silver S3 Delta Layer...")
    cleaned_df.write.format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .save(OUTPUT_DELTA_PATH)
 
  
    print("✅ Ingestion and transformation successful for Provider Info.")
else:
    print("skip ⏭️ No new incremental blocks discovered by Bookmarks for this dataset.")

# Commit bookmark tracking state
job.commit()
            
        

