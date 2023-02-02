# Read-Write Splitting Plugin Performance Results

### Read-Write Splitting Plugin Postgres Performance Results
| Benchmark                                                     | Average Test Run Overhead Time | Units       | Percentage (%) Increase of Average Test Run Overhead from Baseline |
|---------------------------------------------------------------|--------------------------------|-------------|--------------------------------------------------------------------|
| No Plugin Enabled                                             | 0 (baseline)                   | ms/test-run | 0                                                                  |
| Read-Write Splitting Plugin Enabled                           | +56                            | ms/test-run | +0.050                                                             |
| Read-Write Splitting Plugin and Reader Load Balancing Enabled | +186                           | ms/test-run | +0.169                                                             |
