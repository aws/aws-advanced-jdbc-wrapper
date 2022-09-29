# Performance Results

## Benchmarks
| Benchmark                                                         | Score    | Error   | Units |
|-------------------------------------------------------------------|----------|---------|-------|
| MySQL Connector/J                                                 | 858.705  | 142.615 | ms/op |
| JDBC Wrapper with MySQL with No Plugins                           | 853.966  | 45.878  | ms/op |
| JDBC Wrapper with MySQL with EFM                                  | 913.390  | 101.253 | ms/op |
| JDBC Wrapper with MySQL with Failover Connection Plugin           | 846.115  | 88.429  | ms/op |
| JDBC Wrapper with MySQL with EFM and Failover Connection Plugins  | 911.715  | 57.303  | ms/op |

| Benchmark                                                         | Score    | Error   | Units |
|-------------------------------------------------------------------|----------|---------|-------|
| pgJDBC                                                            | 634.043  | 44.318  | ms/op |
| JDBC Wrapper with pgJDBC with No Plugins                          | 633.108  | 34.338  | ms/op |
| JDBC Wrapper with pgJDBC with EFM                                 | 713.359  | 69.774  | ms/op |
| JDBC Wrapper with pgJDBC with Failover Connection Plugin          | 636.123  | 50.273  | ms/op |
| JDBC Wrapper with pgJDBC with EFM and Failover Connection Plugins | 710.346  | 86.632  | ms/op |

| Benchmark                                    | Score   | Error  | Units |
|----------------------------------------------|---------|--------|-------|
| connectWithNoPlugins                         | 133.329 | 19.982 | us/op |
| connectWithTenGenericPlugins                 | 198.782 | 37.724 | us/op |
| executeWithNoPlugins                         | 125.491 | 30.625 | us/op |
| executeTenGenericPlugins                     | 195.051 | 57.536 | us/op |
| initConnectionPluginManagerWithNoPlugins     | 4.703   | 1.993  | us/op |
| initConnectionPluginManagerTenGenericPlugins | 19.432  | 8.678  | us/op |
| initHostProvidersWithNoPlugins               | 12.158  | 4.820  | us/op |
| initHostProvidersTenGenericPlugins           | 29.134  | 6.694  | us/op |
| notifyConnectionChangedWithNoPlugins         | 15.977  | 6.057  | us/op |
| notifyConnectionChangedTenGenericPlugins     | 52.069  | 39.199 | us/op |
| releaseResourcesWithNoPlugins                | 8.030   | 3.776  | us/op |
| releaseResourcesTenGenericPlugins            | 15.243  | 4.906  | us/op |

| Benchmark                                                 | Score    | Error   | Units |
|-----------------------------------------------------------|----------|---------|-------|
| executeStatementBaseline                                  | 706.629  | 139.833 | us/op |
| executeStatementWithExecutionTimePlugin                   | 1037.951 | 187.424 | us/op |
| initAndReleaseBaseLine                                    | 0.667    | 0.140   | us/op |
| initAndReleaseWithAllPlugins                              | 668.783  | 118.127 | us/op |
| initAndReleaseWithAuroraHostListPlugin                    | 667.868  | 122.335 | us/op |
| initAndReleaseWithExecutionTimePlugin                     | 679.230  | 136.899 | us/op |

## Performance Tests

### Failover Performance with Different Socket Timeout Configuration

| SocketTimeout | NetworkOutageDelayMillis | MinFailureDetectionTimeMillis | MaxFailureDetectionTimeMillis | AvgFailureDetectionTimeMillis |
|---------------|--------------------------|-------------------------------|-------------------------------|-------------------------------|
| 30            | 5000                     | 65010                         | 65029                         | 65021                         |
| 30            | 10000                    | 60005                         | 60029                         | 60023                         |
| 30            | 15000                    | 55005                         | 55029                         | 55023                         |
| 30            | 25000                    | 45010                         | 45028                         | 45018                         |
| 30            | 20000                    | 50026                         | 50029                         | 50028                         |
| 30            | 30000                    | 40015                         | 40029                         | 40023                         |

### Enhanced Failure Monitoring Performance with Different Failure Detection Configuration

| FailureDetectionGraceTime | FailureDetectionInterval | FailureDetectionCount | NetworkOutageDelayMillis | MinFailureDetectionTimeMillis | MaxFailureDetectionTimeMillis | AvgFailureDetectionTimeMillis |
|---------------------------|--------------------------|-----------------------|--------------------------|-------------------------------|-------------------------------|-------------------------------|
| 30000                     | 5000                     | 3                     | 5000                     | 41107                         | 41114                         | 41111                         |
| 30000                     | 5000                     | 3                     | 10000                    | 36109                         | 36112                         | 36110                         |
| 30000                     | 5000                     | 3                     | 15000                    | 31107                         | 31111                         | 31109                         |
| 30000                     | 5000                     | 3                     | 20000                    | 26109                         | 26111                         | 26110                         |
| 30000                     | 5000                     | 3                     | 25000                    | 21109                         | 21110                         | 21109                         |
| 30000                     | 5000                     | 3                     | 30000                    | 16106                         | 16109                         | 16108                         |
| 30000                     | 5000                     | 3                     | 35000                    | 16108                         | 16111                         | 16109                         |
| 30000                     | 5000                     | 3                     | 40000                    | 16108                         | 16112                         | 16110                         |
| 30000                     | 5000                     | 3                     | 50000                    | 16112                         | 16112                         | 16112                         |
| 30000                     | 5000                     | 3                     | 60000                    | 16113                         | 16118                         | 16115                         |
| 6000                      | 1000                     | 1                     | 1000                     | 5108                          | 5130                          | 5113                          |
| 6000                      | 1000                     | 1                     | 2000                     | 4106                          | 4111                          | 4108                          |
| 6000                      | 1000                     | 1                     | 3000                     | 3105                          | 3108                          | 3107                          |
| 6000                      | 1000                     | 1                     | 4000                     | 2104                          | 2108                          | 2107                          |
| 6000                      | 1000                     | 1                     | 5000                     | 1103                          | 1107                          | 1105                          |
| 6000                      | 1000                     | 1                     | 6000                     | 1105                          | 1107                          | 1106                          |
| 6000                      | 1000                     | 1                     | 7000                     | 1106                          | 1108                          | 1107                          |
| 6000                      | 1000                     | 1                     | 8000                     | 1105                          | 1107                          | 1106                          |
| 6000                      | 1000                     | 1                     | 9000                     | 1106                          | 1110                          | 1108                          |
| 6000                      | 1000                     | 1                     | 10000                    | 1107                          | 1110                          | 1110                          |

### Failover Performance with Different Enhanced Failure Monitoring Configuration

| FailureDetectionGraceTime | FailureDetectionInterval | FailureDetectionCount | NetworkOutageDelayMillis | MinFailureDetectionTimeMillis | MaxFailureDetectionTimeMillis | AvgFailureDetectionTimeMillis |
|---------------------------|--------------------------|-----------------------|--------------------------|-------------------------------|-------------------------------|-------------------------------|
| 30000                     | 5000                     | 3                     | 5000                     | 36000                         | 36002                         | 36001                         |
| 30000                     | 5000                     | 3                     | 10000                    | 31000                         | 31001                         | 31000                         |
| 30000                     | 5000                     | 3                     | 15000                    | 26000                         | 26001                         | 26000                         |
| 30000                     | 5000                     | 3                     | 20000                    | 21000                         | 21002                         | 21001                         |
| 30000                     | 5000                     | 3                     | 25000                    | 15999                         | 16001                         | 16001                         |
| 30000                     | 5000                     | 3                     | 30000                    | 11000                         | 11001                         | 11001                         |
| 30000                     | 5000                     | 3                     | 35000                    | 5996                          | 6005                          | 6000                          |
| 30000                     | 5000                     | 3                     | 40000                    | 1000                          | 1001                          | 1000                          |
| 30000                     | 5000                     | 3                     | 50000                    | 4669937                       | 4835750                       | 4752841                       |
| 30000                     | 5000                     | 3                     | 60000                    | 4878341                       | 5044200                       | 4961261                       |
| 6000                      | 1000                     | 1                     | 1000                     | 39996                         | 40001                         | 39999                         |
| 6000                      | 1000                     | 1                     | 2000                     | 38999                         | 39001                         | 39000                         |
| 6000                      | 1000                     | 1                     | 3000                     | 37999                         | 38001                         | 38000                         |
| 6000                      | 1000                     | 1                     | 4000                     | 36999                         | 37001                         | 37000                         |
| 6000                      | 1000                     | 1                     | 5000                     | 36000                         | 36001                         | 36001                         |
| 6000                      | 1000                     | 1                     | 6000                     | 35000                         | 35002                         | 35001                         |
| 6000                      | 1000                     | 1                     | 7000                     | 34000                         | 34001                         | 34000                         |
| 6000                      | 1000                     | 1                     | 8000                     | 32999                         | 33002                         | 33000                         |
| 6000                      | 1000                     | 1                     | 9000                     | 32000                         | 32002                         | 32001                         |
| 6000                      | 1000                     | 1                     | 10000                    | 31001                         | 31001                         | 31001                         |
