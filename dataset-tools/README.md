# Scripts for processing datasets from our cluster

Running the example experiments on the cluster with the provided scripts will create a .zip-file.

The zip file contains:
- Raw Prometheus data as hundreds to thousands of separate json-files
- Raw QoS metrics as separate csv-files

The scripts in this folder are intended to process these zips into a more usable format.

## Usage:

1. Collect data from the cluster
2. Process the raw data into a dataset using tools in this folder
   3. Use `01_process_prometheus` to process the individual json-files inside the zip into Pandas DataFrames.
      4. As a result, there will be one .feather-file for each node in the cluster.
      5. The feather-files can be read with Python Pandas library.
   6. Use `02_process_qos` to process the QoS-files resulting from the example experiments.
      7. The QoS-files will also be stored as a Pandas DataFrame to a .feather-file.
      8. Other scripts in this repository expects that these QoS feather-files are stored in the same folder as the
      feathers from the `01_process_prometheus`-script.

