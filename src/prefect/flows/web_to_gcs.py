from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from settings import DATASETS_LOCAL_DIR_PATH


@flow(log_prints=True)
def web_to_gcs(service: str = "green", year: int = 2020, month: int = 1, remove_missing=True) -> None:
    print(f'Start task for {service} taxi, {year} year, {month} month')
    dataset_file_name = f"{service}_tripdata_{year}-{month:02}"
    dataset_file_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/" \
                       f"{service}/{dataset_file_name}.csv.gz"

    raw_dataset = extract_data(
        dataset_file_url=dataset_file_url
    )
    dataset_filepath_local = Path(f"{DATASETS_LOCAL_DIR_PATH}/{service}_taxi/{dataset_file_name}.csv")
    dataset_filepath_local.parent.mkdir(parents=True, exist_ok=True)
    raw_dataset.to_csv(dataset_filepath_local, index=False)
    dataset = transform_data(df=raw_dataset, remove_missing=remove_missing)
    load_data(
        dataset=dataset,
        dataset_folder=f"{service}_taxi",
        dataset_file_name=dataset_file_name
    )


@task()
def extract_data(dataset_file_url: str) -> pd.DataFrame():
    raw_dataset = pd.read_csv(
        filepath_or_buffer=dataset_file_url,
        low_memory=False,
    )
    print(f"Detected {len(raw_dataset)} records in dataset")

    return raw_dataset


@task()
def transform_data(df: pd.DataFrame, remove_missing: bool) -> pd.DataFrame:
    df.columns = df.columns.str.lower()
    for column in df.columns:
        if 'datetime' in column:
            df[column] = pd.to_datetime(df[column])
        if column.endswith('id'):
            df[column] = df[column].astype(pd.Int64Dtype())
        if column.endswith('_type'):
            df[column] = df[column].astype(pd.Int64Dtype())
        if column.endswith('_flag'):
            applier = {
                'Y': True,
                'y': True,
                'yes': True,
                'Yes': True,
                'YES': True,
                'N': False,
                'n': False,
                'no': False,
                'No': False,
                'NO': False,
            }
            df[column] = df[column].map(applier).astype(pd.BooleanDtype())
        if column.endswith('_count'):
            df[column] = df[column].astype(pd.Int64Dtype())
    if remove_missing:
        df['passenger_count'] = df['passenger_count'].fillna(0)

    return df


@task()
def load_data(dataset: pd.DataFrame, dataset_folder: str, dataset_file_name: str) -> None:
    dataset_filepath_local = Path(f"{DATASETS_LOCAL_DIR_PATH}/{dataset_folder}/{dataset_file_name}.parquet")
    dataset_filepath_gcs = Path(f"{dataset_folder}/{dataset_file_name}.parquet")
    # dataset_filepath_local = Path(f"{DATASETS_LOCAL_DIR_PATH}/{dataset_folder}/{dataset_file_name}.csv")
    # dataset_filepath_gcs = Path(f"{dataset_folder}/{dataset_file_name}.csv")

    dataset_filepath_local.parent.mkdir(parents=True, exist_ok=True)
    # dataset.to_csv(
    #     dataset_filepath_local,
    #     index=False,
    # )
    dataset.to_parquet(
        dataset_filepath_local,
        engine='pyarrow',
        compression='gzip'
    )

    gcs_bucket_block = GcsBucket.load('gcs-bucket', validate=False)

    gcs_bucket_block.upload_from_path(
        from_path=dataset_filepath_local,
        to_path=dataset_filepath_gcs,
    )


if __name__ == '__main__':
    for month in range(1, 13):
        for service in ['yellow', 'green', 'fhv']:
            for year in [2019, 2020]:
                if service == 'fhv' and year != 2019:
                    break
                web_to_gcs(
                    service=service,
                    year=year,
                    month=month,
                    remove_missing=False,
                )
