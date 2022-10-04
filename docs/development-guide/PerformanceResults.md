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

### Failover Performance with 30 Seconds Socket Timeout Configuration

| SocketTimeout | NetworkOutageDelayMillis | MinFailureDetectionTimeMillis | MaxFailureDetectionTimeMillis | AvgFailureDetectionTimeMillis |
|---------------|--------------------------|-------------------------------|-------------------------------|-------------------------------|
| 30            | 5000                     | 25181                         | 25302                         | 25244                         |
| 30            | 10000                    | 20164                         | 20279                         | 20191                         |
| 30            | 15000                    | 15176                         | 15186                         | 15180                         |
| 30            | 20000                    | 10173                         | 10213                         | 10190                         |
| 30            | 25000                    | 5176                          | 5192                          | 5184                          |
| 30            | 30000                    | 166                           | 189                           | 176                           |

### Enhanced Failure Monitoring Performance with Different Failure Detection Configuration

| FailureDetectionGraceTime | FailureDetectionInterval | FailureDetectionCount | NetworkOutageDelayMillis | MinFailureDetectionTimeMillis | MaxFailureDetectionTimeMillis | AvgFailureDetectionTimeMillis |
|---------------------------|--------------------------|-----------------------|--------------------------|-------------------------------|-------------------------------|-------------------------------|
| 30000                     | 5000                     | 3                     | 5000                     | 41109                         | 41114                         | 41111                         |
| 30000                     | 5000                     | 3                     | 10000                    | 36108                         | 36112                         | 36110                         |
| 30000                     | 5000                     | 3                     | 15000                    | 31109                         | 31111                         | 31110                         |
| 30000                     | 5000                     | 3                     | 20000                    | 26108                         | 26111                         | 26109                         |
| 30000                     | 5000                     | 3                     | 25000                    | 21107                         | 21110                         | 21109                         |
| 30000                     | 5000                     | 3                     | 30000                    | 16108                         | 16111                         | 16109                         |
| 30000                     | 5000                     | 3                     | 35000                    | 16109                         | 16111                         | 16110                         |
| 30000                     | 5000                     | 3                     | 40000                    | 16110                         | 16114                         | 16112                         |
| 30000                     | 5000                     | 3                     | 50000                    | 16109                         | 16114                         | 16112                         |
| 30000                     | 5000                     | 3                     | 60000                    | 16115                         | 16119                         | 16117                         |
| 6000                      | 1000                     | 1                     | 1000                     | 5106                          | 5110                          | 5108                          |
| 6000                      | 1000                     | 1                     | 2000                     | 4108                          | 4111                          | 4109                          |
| 6000                      | 1000                     | 1                     | 3000                     | 3107                          | 3109                          | 3108                          |
| 6000                      | 1000                     | 1                     | 4000                     | 2106                          | 2111                          | 2107                          |
| 6000                      | 1000                     | 1                     | 5000                     | 1105                          | 1107                          | 1106                          |
| 6000                      | 1000                     | 1                     | 6000                     | 1101                          | 1107                          | 1105                          |
| 6000                      | 1000                     | 1                     | 7000                     | 1105                          | 1112                          | 1107                          |
| 6000                      | 1000                     | 1                     | 8000                     | 1107                          | 1109                          | 1108                          |
| 6000                      | 1000                     | 1                     | 9000                     | 1106                          | 1111                          | 1109                          |
| 6000                      | 1000                     | 1                     | 10000                    | 1109                          | 1114                          | 1111                          |

### Failover Performance with Different Enhanced Failure Monitoring Configuration

| FailureDetectionGraceTime | FailureDetectionInterval | FailureDetectionCount | NetworkOutageDelayMillis | MinFailureDetectionTimeMillis | MaxFailureDetectionTimeMillis | AvgFailureDetectionTimeMillis |
|---------------------------|--------------------------|-----------------------|--------------------------|-------------------------------|-------------------------------|-------------------------------|
| 30000                     | 5000                     | 3                     | 5000                     | 41280                         | 41315                         | 41292                         |
| 30000                     | 5000                     | 3                     | 10000                    | 36269                         | 36283                         | 36277                         |
| 30000                     | 5000                     | 3                     | 15000                    | 31200                         | 31292                         | 31261                         |
| 30000                     | 5000                     | 3                     | 20000                    | 26264                         | 26288                         | 26276                         |
| 30000                     | 5000                     | 3                     | 25000                    | 21273                         | 21311                         | 21288                         |
| 30000                     | 5000                     | 3                     | 30000                    | 16267                         | 16316                         | 16284                         |
| 30000                     | 5000                     | 3                     | 35000                    | 16273                         | 16284                         | 16279                         |
| 30000                     | 5000                     | 3                     | 40000                    | 16265                         | 16287                         | 16277                         |
| 30000                     | 5000                     | 3                     | 50000                    | 16275                         | 16312                         | 16284                         |
| 30000                     | 5000                     | 3                     | 60000                    | 16272                         | 16293                         | 16282                         |
| 6000                      | 1000                     | 1                     | 1000                     | 5261                          | 5301                          | 5276                          |
| 6000                      | 1000                     | 1                     | 2000                     | 4259                          | 4275                          | 4267                          |
| 6000                      | 1000                     | 1                     | 3000                     | 3263                          | 3280                          | 3271                          |
| 6000                      | 1000                     | 1                     | 4000                     | 2256                          | 2282                          | 2266                          |
| 6000                      | 1000                     | 1                     | 5000                     | 1256                          | 1275                          | 1263                          |
| 6000                      | 1000                     | 1                     | 6000                     | 1168                          | 1273                          | 1248                          |
| 6000                      | 1000                     | 1                     | 7000                     | 1259                          | 1277                          | 1267                          |
| 6000                      | 1000                     | 1                     | 8000                     | 1263                          | 1289                          | 1272                          |
| 6000                      | 1000                     | 1                     | 9000                     | 1264                          | 1281                          | 1270                          |
| 6000                      | 1000                     | 1                     | 10000                    | 1265                          | 1287                          | 1277                          |
