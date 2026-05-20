import streamlit as st  # Optional, but great if you want to see it visually
import pandas as pd
from deltalake import DeltaTable

# Direct S3 path to your fresh Delta Table
SILVER_S3_PATH = "s3://healthcare-etoe-proj-silver-bucket/master_staffing_delta/"

try:
    # 1. Read the Delta Table directly from S3
    dt = DeltaTable(SILVER_S3_PATH)
    df = dt.to_pandas()
    
    # 2. Print high-level diagnostics to your terminal
    print(f"Total rows in Silver Master: {len(df)}")
    print("\n--- Data Schema and Types ---")
    print(df.dtypes)
    
    # 3. Print the first 5 rows to check your transformations manually
    print("\n--- First 5 Records Preview ---")
    print(df[['PROVNUM', 'STATE', 'WorkDate','TotalNursing_Hours','TotalAdminNursing_Hours','RN_Ratio','CNA_Ratio','LPN_Ratio']].head(5000))
    
    # 4. Verify your transformation rules programmatically
    print("\n--- Running Quality Checks ---")
    assert df['PROVNUM'].isnull().sum() == 0, "❌ Error: Found null PROVNUM values!"
    assert (df['STATE'].str.strip() != df['STATE']).sum() == 0, "❌ Error: Found un-trimmed STATE values!"
    assert df.duplicated(subset=['PROVNUM', 'WorkDate']).sum() == 0, "❌ Error: Found duplicate rows!"
    
    print("🎉 All quality checks passed! Transformations applied perfectly.")
    
except Exception as e:
    print(f"❌ Failed to verify data: {e}")

