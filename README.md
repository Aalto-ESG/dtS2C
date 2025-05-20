# dtS2C
Repository for replicating the experiments from our publication

# Usage:

This repository is split into four parts:

1. `Computing-cluster`
   2. Includes everything needed for installing and setting up a Kubernetes cluster on top of Ubuntu machines.
   3. Includes everything needed for running our example experiments on the cluster.
   4. As an output, produces zip-files including raw metrics from the cluster and QoS metrics from the workload applications.
5. `Dataset-tools`
   6. Contains tools for processing the raw metrics from the zip-files into Pandas DataFrames to make the data easier to handle.
7. `Analysis`
   8. Contains examples for plotting the metrics from the experiment DataFrames in various formats.
9. `ML-predict`
   10. Contains examples for processing the experiment DataFrames into datasets with input features and labels suitable for training machine learning models.