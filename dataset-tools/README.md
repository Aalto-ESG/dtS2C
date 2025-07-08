# Scripts for processing datasets from our cluster

Running the example experiments on the cluster with the provided scripts will create a .zip-file.

The zip file contains:
- Raw Prometheus data as hundreds to thousands of separate json-files
- Raw QoS metrics as separate csv-files

The scripts in this folder are intended to process these zips into a more usable format.

## Example data:
 - Example UTS / YOLO experiment (3.65 GB zip-file): https://drive.google.com/file/d/18rki8WgZj6EmSuRXUaAUEVBEU2pAQkbF/view?usp=sharing
 - Example ILW / Lidar-experiment (2.25 GB zip-file): https://drive.google.com/file/d/1Maz4yqi7xeD2gWSioLEqCwlpNkqPBP88/view?usp=sharing
 - You can use these example datasets to test the analysis tools:
   1. Download dataset
   2. Process dataset using tools from this folder
   3. Use the other tools in this repository to analyze the processed data


## Usage:

1. Collect data from the cluster
2. Process the raw data into a dataset using tools in this folder
   - Use `01_process_prometheus` to process the individual json-files inside the zip into Pandas DataFrames.
      - As a result, there will be one .feather-file for each node in the cluster.
      - The feather-files can be read with Python Pandas library.
   - Use `02_process_qos` to process the QoS-files resulting from the example experiments.
      - The QoS-files will also be stored as a Pandas DataFrame to a .feather-file.
      - Other scripts in this repository expects that these QoS feather-files are stored in the same folder as the
      feathers from the `01_process_prometheus`-script.

