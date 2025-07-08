import os
from utils import utils

"""
00 - Configuration
"""

dataset_zip_path = "datasets/run_6/snapshots_run_6/"
output_folder = "processed_datasets/snapshots_run_6/"

zip_files_list = utils.list_zip_files(dataset_zip_path)

print("List of zip files:")
for zip_file in zip_files_list:
    print(zip_file)


"""
01 - Process qos metrics from .zip files:

- Combine all .csv-files in the given .zip to a DataFrame
- Save the dataframe to output_path
"""

import zipfile
import time
import pandas as pd

failed_zips = []
successful_zips = []


def process_zip(path, output_folder, name="qos_metrics", starts_with=""):
    # Open as zip
    with zipfile.ZipFile(path, 'r') as zip_ref:

        # Get a list of all files in the zip
        items = zip_ref.namelist()

        # Get a list of all .csv-files in the zip
        csv_files = [x for x in items if x.endswith('.csv')]
        print(csv_files)
        csv_files = [x for x in csv_files if starts_with in x]  # Separate worker_n.csv from master_n.csv
        print(csv_files)
        csv_files.sort(reverse=True)  # NOTE: Sorting does not matter, but may be useful for debugging
        if len(csv_files) == 0:
            print(f"No .csv-files found in {path}")
            failed_zips.append(path)
            return

        count = 0
        start_time = time.time()

        # Iterate over all csv-files
        dataframes = []
        for path in csv_files:
            count += 1
            print(
                f"Progress {count}/{len(csv_files):6}, ({count / len(csv_files) * 100:5.3} %) (time_spent: {time.time() - start_time:.3} s  - avg: {(time.time() - start_time) / count} s)")
            with zip_ref.open(path) as csv_file:
                x = pd.read_csv(csv_file)
                dataframes.append(x)

        if len(dataframes) == 0:
            print(f"No dataframes found in {path} for {starts_with}")
            return pd.DataFrame()

        # Combine data to a single DataFrame
        df = pd.concat(dataframes)
        os.makedirs(output_folder, exist_ok=True)
        output_path = os.path.join(output_folder, f"{name}.feather")
        df.sort_index(inplace=True)
        if "timestamp" not in df.columns:
            # HPA-files already have timestamp, qos-files do not
            df.reset_index(drop=False, inplace=True, names=["timestamp"])
        df.to_feather(output_path)
        print(f"Saved to {output_path}")
        successful_zips.append(output_path)
        return df


dfs = []
titles = []
for application_type in ["master", "worker", "hpa", "yolo"]:
    filename_prefix = application_type + "_"
    if application_type == "yolo":
        filename_prefix = "" # Yolo-qos files have no prefix
    if dataset_zip_path.endswith(".zip"):
        # Process single zip
        print(f"Processing {dataset_zip_path}")
        df = process_zip(dataset_zip_path, output_folder, name=f"{application_type}_qos", starts_with=filename_prefix)
        dfs.append(df)
        zip_name = dataset_zip_path.split('/')[-1].replace(".zip", "")
        titles.append(zip_name)
    else:
        # Process all zips in path
        for zip_name_full in utils.list_zip_files(dataset_zip_path):
            print(zip_name_full)
            zip_name = zip_name_full.replace(".zip", "")
            full_output_path = f"{output_folder}/{zip_name}"
            df = process_zip(dataset_zip_path + zip_name_full, full_output_path, name=f"{application_type}_qos",
                             starts_with=filename_prefix)
            dfs.append(df)
            titles.append(zip_name)

print(f"Failed zips:")
for zip_name in failed_zips:
    print(zip_name)

print("")
print(f"Successful zips:")
for zip_name in successful_zips:
    print(zip_name)