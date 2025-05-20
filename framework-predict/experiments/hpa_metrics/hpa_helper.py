import common_utils
import os
import pandas as pd
from utils.header_cleaner import *
import difflib
import os

# Example usage
root_folder = '../../../data_warehouse/minimized_warehouse_7c'
filename = 'worker1.feather'

"""
Find all relevant files from subfolders
"""
subfolders = common_utils.find_subfolders_with_file(root_folder, filename)
print(subfolders)
prom_data_paths = {os.path.basename(x): x for x in subfolders}
hpa_data_paths = {key: os.path.join(val, "hpa_qos.feather") for key, val in prom_data_paths.items()}

"""
Load the HPA data to dataframes
cols = ['timestamp', 'cpu-avg-worker', 'cpu-avg-master', 'replicas-worker',
       'replicas-master']
"""
dfs = {}
for key in prom_data_paths.keys():
    hpa_df = pd.read_feather(hpa_data_paths[key])
    model_info = common_utils.path_to_workers_and_pcl_size(key)
    dfs[model_info.resolution] = hpa_df
    print(key)


def get_num_workers(timestamp):
    """
    Finds and returns the number of worker replicas ('replicas-worker')
    at the given timestamp or the closest timestamp match across all dataframes.

    Args:
        timestamp (str): Input timestamp string in ISO 8601 format.

    Returns:
        int: The 'replicas-worker' value for the closest timestamp match.
             Returns None if no match is found or if an error occurs.
    """
    try:
        # Step 1: Convert the input timestamp string to a pandas datetime object
        input_timestamp = pd.to_datetime(timestamp)

        closest_value = None
        closest_time_diff = pd.Timedelta.max  # Initialize to the largest possible timedelta

        # Step 2: Search through all dataframes in the `dfs` dictionary
        for resolution, df in dfs.items():
            if 'timestamp' not in df.columns or 'replicas-worker' not in df.columns:
                # Skip dataframes without the necessary columns
                continue

            # Step 3: Convert the timestamps in the dataframe to pandas datetime objects (if not already done)
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            # Compute the difference between the input timestamp and all timestamps in the dataframe
            time_diff = (df['timestamp'] - input_timestamp).abs()

            # Find the index of the closest timestamp
            closest_idx = time_diff.idxmin()

            # Check if this timestamp is closer than the previous closest
            if time_diff.iloc[closest_idx] < closest_time_diff:
                closest_time_diff = time_diff.iloc[closest_idx]
                # Get the 'replicas-worker' value for the closest match
                closest_value = df.loc[closest_idx, 'replicas-worker']

        # Step 4: Return the closest 'replicas-worker' value
        return closest_value

    except Exception as e:
        # Step 5: Handle errors and return None in case of issues
        print(f"Error occurred: {e}")
        return None
