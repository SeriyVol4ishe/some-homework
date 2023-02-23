from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from settings import DATASETS_LOCAL_DIR_PATH


@flow(log_prints=True)
def web_to_gcs(color: str = "green", year: int = 2020, month: int = 1, remove_missing=True) -> None:
    dataset_file_name = f"{color}_tripdata_{year}-{month:02}"
    dataset_file_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/" \
                       f"{color}/{dataset_file_name}.csv.gz"

    raw_dataset = extract_data(
        dataset_file_url=dataset_file_url
    )
    dataset = transform_data(
        raw_dataset=raw_dataset,
        remove_missing=remove_missing,
    )
    load_data(
        dataset=dataset,
        dataset_folder=f"{color}_taxi",
        dataset_file_name=dataset_file_name
    )


@task()
def extract_data(dataset_file_url: str) -> pd.DataFrame():
    raw_dataset = pd.read_csv(
        filepath_or_buffer=dataset_file_url
    )
    print(f"Detected {len(raw_dataset)} records in dataset")

    return raw_dataset


@task()
def transform_data(raw_dataset: pd.DataFrame, remove_missing: bool) -> pd.DataFrame:
    for column_name in raw_dataset.columns:
        if 'datetime' in column_name:
            raw_dataset[column_name] = pd.to_datetime(raw_dataset[column_name])
    if remove_missing:
        raw_dataset['passenger_count'] = raw_dataset['passenger_count'].fillna(0)

    return raw_dataset


@task()
def load_data(dataset: pd.DataFrame, dataset_folder: str, dataset_file_name: str) -> None:
    dataset_filepath_local = Path(f"{DATASETS_LOCAL_DIR_PATH}/{dataset_folder}/{dataset_file_name}.parquet")
    dataset_filepath_gcs = Path(f"{dataset_folder}/{dataset_file_name}.parquet")

    dataset_filepath_local.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_parquet(
        dataset_filepath_local,
        compression='gzip'
    )

    gcs_bucket_block = GcsBucket.load('gcs-bucket', validate=False)

    gcs_bucket_block.upload_from_path(
        from_path=dataset_filepath_local,
        to_path=dataset_filepath_gcs,
    )


if __name__ == '__main__':
    for year in [2019, 2020]:
        for color in ['yellow', 'green', 'fhv']:
            if color == 'fhv' and year != 2019:
                break
            for month in range(1, 13):
                web_to_gcs(
                    color=color,
                    year=year,
                    month=month,
                    remove_missing=False,
                )
