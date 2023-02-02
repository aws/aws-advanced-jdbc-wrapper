# Read-Write Splitting Plugin Performance Results

### Read-Write Splitting Plugin Postgres Performance Results
| Benchmark                                                     | Overhead Time | Units |
|---------------------------------------------------------------|---------------|-------|
| No Plugin Enabled                                             | 0 (baseline)  | ms/op |
| Read-Write Splitting Plugin Enabled                           | +56           | ms/op |
| Read-Write Splitting Plugin and Reader Load Balancing Enabled | +186          | ms/op |
