# Read-Write Splitting Plugin Performance Results

### Read-Write Splitting Plugin Postgres Performance Results
| Benchmark                                                     | Percentage (%) Increase from baseline | Total Overhead Time | Units |
|---------------------------------------------------------------|---------------------------------------|---------------------|-------|
| No Plugin Enabled                                             | 0                                     | 0 (baseline)        | ms/op |
| Read-Write Splitting Plugin Enabled                           | +0.050                                | +56                 | ms/op |
| Read-Write Splitting Plugin and Reader Load Balancing Enabled | +0.169                                | +186                | ms/op |
