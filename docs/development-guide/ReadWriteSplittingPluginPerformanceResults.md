# Read-Write Splitting Plugin Performance Results

### Read-Write Splitting Plugin Postgres Performance Results
| Benchmark                                                   | Average Test Run Overhead Time | Units       | Percentage (%) Increase of Average Test Run Overhead from Baseline |
|-------------------------------------------------------------|--------------------------------|-------------|--------------------------------------------------------------------|
| No Plugin Enabled                                           | 0 (baseline)                   | ms/test-run | 0                                                                  |
| Read-Write Splitting Plugin Enabled                         | +139.0                         | ms/test-run | +0.689                                                             |
| Read-Write Splitting Plugin with Connection Pooling Enabled | +2.8                           | ms/test-run | +0.014                                                             |
