# Scripts for processing datasets from our cluster


Scripts for using AutoML to make predictions from cluster data.

## Usage:

1. Collect data from the cluster.
2. Process the raw data into a dataset using tools from `dataset-tools`.
3. Create a combined dataset for all cluster workers.
   4. Run `01_preprocess_workers` to preprocess data individually for each worker.
   5. Run `02_subgroup_workers` to split the data into different feature groups (as used in the publication).
   6. Run `03_preprocess_workers` to combine the data from individual workers to one dataset.
4. In the experiment-specific folders, there is an example for predicting energy usage.
   5. Run `04_create_training_dataset` to extract total cluster energy usage and use it as a target label for predictions.
   6. Run `05_train_models` to train models from the datasets.


## Customization

The pipeline included here is intended as an example. It is meant to be a starting point for using the cluster data
to train ML models for various purposes.