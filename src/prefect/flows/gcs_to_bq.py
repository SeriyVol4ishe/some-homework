from pathlib import Path

import pandas as pd
from prefect import (
    flow,
    task
)
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

from settings import DATASETS_LOCAL_DIR_PATH, GCP_PROJECT_ID


@flow(log_prints=True)
def gcs_to_bq(months: list[int], color="green", year=2020, remove_missing=True):
    """The main ETL function. Inserts data from """
    dataset_folder = f"{color}_taxi"
    total_row_count = 0
    for month in months:
        dataset_file_name = f"{color}_tripdata_{year}-{month:02}.parquet"

        local_dataset_filepath = extract_data_from_gcs(
            dataset_folder=dataset_folder,
            dataset_file_name=dataset_file_name
        )

        dataset = transform_data(
            local_dataset_filepath=local_dataset_filepath,
            remove_missing=remove_missing,
        )

        total_row_count += load_data(
            dataset_schema=dataset_folder,
            dataset=dataset
        )
    print(f'Total rows count: {total_row_count}')


@task()
def extract_data_from_gcs(dataset_folder: str, dataset_file_name: str) -> Path:
    """Download data from GCS"""

    dataset_filepath_gcs = Path(f"{dataset_folder}/{dataset_file_name}")
    dataset_dirpath_local = Path(f"{DATASETS_LOCAL_DIR_PATH}/")

    gcs_bucket_block = GcsBucket.load('gcs-bucket')
    gcs_bucket_block.get_directory(
        from_path=dataset_filepath_gcs,
        local_path=dataset_dirpath_local
    )

    print(f"File {dataset_filepath_gcs} successfully downloaded to {dataset_dirpath_local}!")

    return dataset_dirpath_local / dataset_folder / dataset_file_name


@task()
def transform_data(local_dataset_filepath: Path, remove_missing: bool) -> pd.DataFrame:
    dataset = pd.read_parquet(
        path=local_dataset_filepath
    )
    if remove_missing:
        print(f"Missing passenger count before transformation: {dataset['passenger_count'].isna().sum()}")
        dataset['passenger_count'] = dataset['passenger_count'].fillna(0)
        print(f"Missing passenger count after transformation: {dataset['passenger_count'].isna().sum()}")

    return dataset


@task()
def load_data(dataset_schema: str, dataset: pd.DataFrame) -> int:
    gcp_credentials_block = GcpCredentials.load('gcp-credentials')
    dataset.to_gbq(
        destination_table=f"trips_data_all.{dataset_schema}",
        project_id=GCP_PROJECT_ID,
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=100_000,
        if_exists='append'
    )
    return len(dataset)


if __name__ == '__main__':
    # gcs_to_bq(
    #     color='green',
    #     year=2020,
    #     month=1,
    # )
    gcs_to_bq(
        color='yellow',
        year=2019,
        months=[2, 3],
        remove_missing=False,
    )
