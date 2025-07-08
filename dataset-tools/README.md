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
   1. Download a dataset.
   2. Unzip dataset to ``datasets/`` UTS dataset.
      3. You should now have a folder with multiple zips. You do not need to unzip the rest of the zips.
   2. Process dataset using tools from this folder following the instructions below.
   3. Use the other tools in this repository to analyze the processed data.


## Usage:

1. Install requirements
   2. ``pip install -r requirements.txt``
2. Collect data from the cluster
3. Use `01_process_prometheus.py` to process the individual json-files inside the zip into Pandas DataFrames.
   1. First change the input path to point to the dataset folder
   2. Then run the ``01_process_prometheus.py``
      - As a result, there will be one .feather-file for each node in the cluster.
      - The feather-files can be read with Python Pandas library.
4. Use `02_process_qos.py` to process the QoS-files resulting from the example experiments.
   1. First change the input path to point to the dataset folder
   2. Then run the ``02_process_qos.py``
      - The QoS-files will also be stored as a Pandas DataFrame to a .feather-file.
      - Other scripts in this repository expect that these QoS feather-files are stored in the same folder as the
      feathers from the `01_process_prometheus.py`-script.

