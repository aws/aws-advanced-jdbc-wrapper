# Read-Write Splitting Plugin Performance Test Results

## Benchmarks
| Benchmark                                                     | Score       | Units |
|---------------------------------------------------------------|-------------|-------|
| No Plugin Enabled                                             | 1,105,514.4 | ms/op |
| Read-Write Splitting Plugin Enabled                           | 1,106,074.0 | ms/op |
| Read-Write Splitting Plugin and Reader Load Balancing Enabled | 1,107,379.9 | ms/op |

## Performance Tests

### Switch Between Reader and Writer Connection Performance Test
|                                               | minOverheadTimeMillis | maxOverheadTimeMillis | avgOverheadTimeMillis |
|-----------------------------------------------|-----------------------|-----------------------|-----------------------|
| Switch to reader (open new connection)        | 773                   | 901                   | 804                   |
| Switch back to writer (use cached connection) | 137                   | 145                   | 141                   |


### Reader Load Balancing Performance Test
| readWriteSplittingPlugin | minAverageExecuteStatementTimeMillis | maxAverageExecuteStatementTimeMillis | avgExecuteStatementTimeMillis |
|--------------------------|--------------------------------------|--------------------------------------|-------------------------------|
| Enabled                  | 0.577                                | 60.7                                 | 1.716                         |
| Disabled                 | 0.0                                  | 1.58                                 | 0.001                         |
