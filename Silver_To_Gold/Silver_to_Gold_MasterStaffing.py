#This below script may not run in local VS Code :  Because AWS Glue has its own proprietary Python wrapper libraries (like awsglue.context.GlueContext), running the exact Glue script locally will throw a ModuleNotFoundError.
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import sum as _sum, avg as _avg, col
from pyspark.sql.functions import col, trim, upper, to_date, coalesce, lit, when

# Initialize Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- PATHS ---
SILVER_MASTER_PATH = "s3://healthcare-etoe-proj-silver-bucket/master_staffing_delta/"
GOLD_HOURS_PATH    = "s3://healthcare-etoe-proj-gold-bucket/gold_staffing_metrics/onduty_hours_summary/"
GOLD_RATIO_PATH    = "s3://healthcare-etoe-proj-gold-bucket/gold_staffing_metrics/nurse_ratio_summary/"

print("🚀 Step 1: Reading clean Master data from Silver Layer...")
silver_df = spark.read.format("delta").load(SILVER_MASTER_PATH)

# ----------------------------------------------------
# AGGREGATION 1: Total Hours On Duty Nurse Summary
# ----------------------------------------------------
print("📊 Step 2: Computing Total On Duty Nurse Hours Summary Table...")
hours_df = silver_df.groupBy("STATE", "WorkDate") \
    .agg(_sum("Hrs_RNDON").alias("Total_RN_Hours")) \
    .orderBy("WorkDate", "STATE")

print("💾 Writing Aggregation 1 to S3...")
hours_df.coalesce(1).write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_HOURS_PATH)

# ----------------------------------------------------
# AGGREGATION 2: Total Nurse-to-Patient Ratio Summary
# ----------------------------------------------------
print("📈 Step 3: Computing Nurse-to-Patient Ratio Table...")

ratio_base_df = silver_df.withColumn("Total_Nurse_Ratio", coalesce(col("TotalNursing_Hours"), lit(0) / col('MDScensus')) )

# Average Nurse to PAtient Ration by Hospital
ratio_by_hospital = ratio_base_df.groupBy('PROVNUM','PROVNAME').agg(avg('Total_Nurse_Ratio').alias('Avg_Nurse_Ratio_ByHospital'))

# Average Nurse to PAtient Ration by State
ratio_by_state = ratio_base_df.groupBy('STATE').agg(avg('Total_Nurse_Ratio').alias('Avg_Nurse_Ratio_ByState'))

# Average Nurse to PAtient Ration by Department(Reginstered Nurse and Certified Nurse)
ratio_by_department = ratio_base_df.groupBy('RN_Ratio','CNA_Ratio').agg(avg('Total_Nurse_Ratio').alias('Avg_Nurse_Ratio_ByDepartment'))

print("💾 Writing Aggregation 2 to S3...")
ratio_by_hospital.coalesce(1).write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_RATIO_PATH + "ratio_by_hospital/")

ratio_by_state.coalesce(1).write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_RATIO_PATH+ "ratio_by_state/")

ratio_by_department.coalesce(1).write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_RATIO_PATH+ "ratio_by_department/")

print("✅ Both Gold Summary Tables written successfully!")
job.commit()
