# Run Example Experiments

These examples have configurable parameters, with default experiments running for 24 hours.

1. **Maximum Throughput Test**
   - Run `example_1_maximum_throughput_test.py` to benchmark maximum throughputs for each YOLO model on your cluster
   - Use these results to configure subsequent tests

2. **Day-Night Cycle Test**
   - Run `example_2_day_night_cycle.py` to measure performance over time with variable workloads
   - **Important:** Adjust maximum data feed rates based on results from test #1
   - Note: Different models process data at different rates; excessive data will oversaturate the cluster

3. **Scaled-Down Day-Night Cycle**
   - Run `example_3_day_night_cycle_scaled_down.py` for a similar test with reduced data feed rates

**Outputs:** All experiments generate a zip file containing raw cluster metrics collected throughout the test.

# Debug locally (without a cluster)

- Optional: Launch kubernetes on Docker-Desktop (most of the scripts work without kubernetes)
- Launch `local_kube_kafka/` with `docker compose up` (This runs outside kubernetes, but works with the local kubernetes setup)
- Launch

# Update yolo consumer (in docker hub)

`docker build -t debnera/ov_yolo:0.5 -f ov.Dockerfile .`

`docker push debnera/ov_yolo:0.5`

