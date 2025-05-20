# dtS2C
Repository for replicating the experiments from our publication

# Usage:

This repository is split into four parts:

1. `Computing-cluster`
   - Includes everything needed for installing and setting up a Kubernetes cluster on top of Ubuntu machines.
   - Includes everything needed for running our example experiments on the cluster.
   - As an output, produces zip-files including raw metrics from the cluster and QoS metrics from the workload applications.
5. `Dataset-tools`
   - Contains tools for processing the raw metrics from the zip-files into Pandas DataFrames to make the data easier to handle.
7. `Analysis`
   - Contains examples for plotting the metrics from the experiment DataFrames in various formats.
9. `ML-predict`
   - Contains examples for processing the experiment DataFrames into datasets with input features and labels.
   - These datasets can then be used for training machine learning models.
