# Collaborative warehouse AGVs

This is an example application where lidar-data is being generated 
from multiple autonomous guided vehicles (AGVs) that represent robots in a warehouse.

The lidar data is processed on a cluster to collaboratively create a real-time view to the state of the
warehouse in the form of a occupancy grid.

The application is split to multiple workers and one master. The worker nodes receive point clouds
that are used to compute a local occupancy grid. The occupancy grid is essentially a map of the warehouse,
showing which squares are currently occupied by some obstacle and which are empty. The local occupancy grid
is then send to the master, which constructs a global occupancy grid from the data of all workers.

This application represents a workload, where the individual applications running on the cluster must
communicate with each other to create a combined view of the world.

Data can be fed to the cluster over Kafka either from real vehicles, simulated vehicles or 
a previously collected dataset.

# Examples

These examples have configurable parameters, with default experiments running for 24 hours.

1. **Maximum Throughput Test**
   - Run `example_1_maximum_throughput_test.py` to benchmark maximum throughputs for each LiDAR resolution and worker 
amount on your cluster
   - Use these results to configure subsequent tests

2. **Linear Test**
   - Run `example_2_throughput_adjusted_linear_feed.py` to measure performance over time with linear feed-rate
   - **Important:** Adjust maximum data feed rates based on results from test #1
   - Note: Different cases process data at different rates; excessive data will oversaturate the cluster

2. **Day-Night Cycle Test**
   - Run `example_3_throughput_adjusted_day_night_cycle.py` to measure performance over time with feed-rate that varies over time
   - **Important:** Adjust maximum data feed rates based on results from test #1
   - Note: Different cases process data at different rates; excessive data will oversaturate the cluster

3. **Autoscaling**
   - Run `example_4_horizontal_pod_autoscale.py` for an experiment with Kubernetes Horizontal Pod Autoscaling (HPA) enabled.
   - The HPA algorithm automatically adjusts the number of workers. The aim is to analyze the effectiveness of the autoscaling.

**Outputs:** All experiments generate a zip file containing raw cluster metrics collected throughout the test.

# How to debug locally
- Optional: Launch kubernetes on Docker-Desktop (most of the scripts work without kubernetes)
- Launch `local_kube_kafka/` with `docker compose up` (This runs outside kubernetes, but works with the local kubernetes setup)
- Launch the application
- Feed data to the cluster over Kafka

# Update yolo consumer (in docker hub)
- `cd docker`
- `docker build -t debnera/warehouse_master:0.1 -f master.Dockerfile .`
- `docker push debnera/warehouse_master:0.1`

- `docker build -t debnera/warehouse_worker:0.1 -f worker.Dockerfile .`
- `docker push debnera/warehouse_worker:0.1`