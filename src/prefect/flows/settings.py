import os

BASE_DIR = os.getcwd()

GCP_PROJECT_ID = 'crafty-chiller-376916'
GCS_BUCKET_NAME = 'taxi-data-homework'

PREFECT_GCP_CREDENTIALS_BLOCK = 'gcp-credentials'
PREFECT_GCS_BUCKET_BLOCK = 'gcs-bucket'

DATASETS_LOCAL_DIR_PATH = '/tmp/datasets/'
