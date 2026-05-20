# Controller Glue Job Code
import boto3
import json
import io
from datetime import datetime, timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- IDs and S3 Config ---
MASTER_FILE_NAME = 'PBJ_Daily_Nurse_Staffing_Q2_2024.csv'
MASTER_FILE_ID = '1kZMZFGfTLdcwmdhjDPZh2-XE2_gOBRCz'
FOLDER_ID = '15KqJ1MZ7JcgAkOfqcaWcALWkG0dh3jpE'
RAW_BUCKET = 'healthcare-etoe-proj-raw-bucket'
TARGET_FOLDER_FILES = ['NH_ProviderInfo_Oct2024.csv',
                       'NH_QualityMsr_MDS.csv',
                       'NH_QualityMsr_MDS_Oct2024.csv', 
                       'NH_QualityMsr_Claims_Oct2024.csv', 
                       'FY_2024_SNF_VBP_Facility_Performance.csv', 
                       'Skilled_Nursing_Facility_Quality_Reporting_Program_Provider_Data_Oct2024.csv']


# --- AWS SSM Parameters for Bookmarking ---
PARAM_MASTER = "/glue/last_modified/master"
PARAM_FOLDER = "/glue/last_modified/folder"


def update_ssm_param(ssm_client, param_name, value):
    ssm_client.put_parameter(
        Name=param_name,
        Value=value,
        Type='String',
        Overwrite=True
    )

# For manually updating the ssm for testing and debugging to reload the files 
# ssm = boto3.client('ssm')
# update_ssm_param(ssm, PARAM_MASTER, '1900-01-01T00:00:00Z')
# update_ssm_param(ssm, PARAM_FOLDER, '1900-01-01T00:00:00Z')

def get_drive_service():
    secret_client = boto3.client("secretsmanager")
    secret = secret_client.get_secret_value(SecretId="google/drive/credentials")
    creds = service_account.Credentials.from_service_account_info(json.loads(secret['SecretString']))
    return build('drive', 'v3', credentials=creds)

def download_file(service, file_id, file_name, s3_key):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    
    boto3.client('s3').put_object(Bucket=RAW_BUCKET, Key=s3_key, Body=fh.getvalue())
    print(f"✅ Successfully ingested: {file_name}")

def check_and_ingest():
    service = get_drive_service()
    ssm = boto3.client('ssm')
    
    # 1. HANDLE MASTER FILE
    master_meta = service.files().get(fileId=MASTER_FILE_ID, fields='name, modifiedTime').execute()
    try:
        last_master_run = ssm.get_parameter(Name=PARAM_MASTER)['Parameter']['Value']
    except:
        last_master_run = "1900-01-01T00:00:00Z"

    if master_meta['modifiedTime'] > last_master_run:  
        download_file(service, MASTER_FILE_ID, master_meta['name'], f"raw/master/{master_meta['name']}")
        update_ssm_param(ssm, PARAM_MASTER, master_meta['modifiedTime'])
    else:
        print(f"⏭️ Master file {MASTER_FILE_NAME} has not changed.")


    # 2. HANDLE FOLDER (The 6 files)
    query = f"'{FOLDER_ID}' in parents and mimeType = 'text/csv' and trashed = false"
    results = service.files().list(q=query, fields="files(id, name, modifiedTime)").execute()
    all_folder_files = results.get('files', [])

     # Filter for only your target 6 files
    selected_files = [f for f in all_folder_files if f['name'] in TARGET_FOLDER_FILES]

    # Use the current time as the new high-water mark for the folder
    folder_run_start_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


    # Get SSM param for folder (last run time)
    try:
        last_folder_run = ssm.get_parameter(Name=PARAM_FOLDER)['Parameter']['Value']
    except:
        last_folder_run = "1900-01-01T00:00:00Z"

    for f in selected_files:
        if f['modifiedTime'] > last_folder_run:
            # Save into a specific prefix so your Silver job can read all 6 files at once
            download_file(service, f['id'], f['name'], f"raw/transactions/{f['name']}")
                # Update Folder SSM Bookmark with current timestamp
    update_ssm_param(ssm, PARAM_FOLDER, folder_run_start_time)
    print(f"✅ Processed {len(selected_files)} target files and updated bookmarks.")


check_and_ingest()
    
if __name__ == "__main__":
    check_and_ingest()

