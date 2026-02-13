# Exception Context

## Driver troubleshooting complexity
Troubleshooting issues in modern JDBC drivers presents significant challenges due to the intermittent and transient nature of database connectivity problems, which are often network-bound and difficult to reproduce. The driver's reliance on rapidly changing database cluster metadata and state compounds these difficulties, while the connection context needed for diagnosis is typically scattered across application logs. Production environments rarely have debug or trace-level logging enabled, leaving teams without crucial diagnostic information when issues occur. The complexity intensifies in high-concurrency scenarios where problems may span multiple threads and transaction scopes, and where modern application architectures involve intricate interactions between frameworks, ORMs, connection pools, and distributed transaction managers. Additionally, most monitoring tools focus on exceptions rather than logs, creating a gap in observability. 

To address these challenges, the AWS Advanced JDBC Wrapper embeds connection and driver state snapshots directly into exceptions, includes a short history of critical events leading up to the error, leverages suppressed exceptions to attach additional diagnostic details, and operates independently of the application's logging configuration—ensuring that essential troubleshooting context is always available when exceptions occur.
## How to read exception context

An exception context is injected as a suppressed exception `SnapshotStateException`. It includes a connection's internal service state snapshot and the latest events:

```
software.amazon.jdbc.plugin.failover.FailoverFailedSQLException: Unable to establish SQL connection to the reader instance.
	(full stack trace is reduced for the sake of clarity)
	Suppressed: software.amazon.jdbc.exceptions.SnapshotStateException: 
State snapshot: 
	...
Latest events:
	...

	(full stack trace is reduced for the sake of clarity)
	... 11 more
```

The state snapshot represents the current state of internal connection services, loaded plugin states, internal caches, and monitoring threads. Although all sensitive data is masked and safe for sharing and analysis by third parties, **it's recommended to review it before sharing**. 

A sample of an exception with injected context is provided below. It's a real exception from one of the driver's integration test runs.

The latest events list includes timestamps for the UTC timezone.

```
software.amazon.jdbc.plugin.failover.FailoverFailedSQLException: Unable to establish SQL connection to the reader instance.
	at software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin.throwFailoverFailedException(FailoverConnectionPlugin.java:530)
	at software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin.failoverReader(FailoverConnectionPlugin.java:658)
	at software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin.failover(FailoverConnectionPlugin.java:614)
	at software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin.pickNewConnection(FailoverConnectionPlugin.java:894)
	at software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin.dealWithOriginalException(FailoverConnectionPlugin.java:562)
	at software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin.execute(FailoverConnectionPlugin.java:300)
	at software.amazon.jdbc.ConnectionPluginManager.lambda$execute$5(ConnectionPluginManager.java:315)
	at software.amazon.jdbc.ConnectionPluginManager.lambda$null$3(ConnectionPluginManager.java:254)
	at software.amazon.jdbc.ConnectionPluginManager.executeWithTelemetry(ConnectionPluginManager.java:218)
	at software.amazon.jdbc.ConnectionPluginManager.lambda$makePluginChainFunc$4(ConnectionPluginManager.java:253)
	at software.amazon.jdbc.ConnectionPluginManager.executeWithSubscribedPlugins(ConnectionPluginManager.java:203)
	at software.amazon.jdbc.ConnectionPluginManager.execute(ConnectionPluginManager.java:312)
	at software.amazon.jdbc.util.WrapperUtils.executeWithPlugins(WrapperUtils.java:332)
	at software.amazon.jdbc.wrapper.StatementWrapper.executeQuery(StatementWrapper.java:46)
	at com.zaxxer.hikari.pool.ProxyStatement.executeQuery(ProxyStatement.java:110)
	at com.zaxxer.hikari.pool.HikariProxyStatement.executeQuery(HikariProxyStatement.java)
	at integration.util.AuroraTestUtility.executeInstanceIdQuery(AuroraTestUtility.java:1879)
	at integration.util.AuroraTestUtility.queryInstanceId(AuroraTestUtility.java:1872)
	at integration.util.AuroraTestUtility.queryInstanceId(AuroraTestUtility.java:1862)
	at integration.container.tests.HikariTests.lambda$null$0(HikariTests.java:199)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
	at java.base/java.lang.Thread.run(Thread.java:829)
	Suppressed: software.amazon.jdbc.exceptions.SnapshotStateException: 
State snapshot: 
  pluginService: 
    properties: 
      failoverMode: reader-or-writer
      telemetryTracesBackend: xray
      tcpKeepAlive: false
      failureDetectionCount: 1
      telemetryMetricsBackend: otlp
      password: ***
      failoverTimeoutMs: 1
      database: test_database
      clusterInstanceHostPattern: ?.***.proxied:8666
      wrapperLogUnclosedConnections: true
      monitoring-socketTimeout: 3000
      enableTelemetry: true
      wrapperPlugins: failover
      connectTimeout: 10000
      socketTimeout: 1000
      failureDetectionInterval: 1000
      monitoring-connectTimeout: 3000
      user: test_user
      telemetrySubmitToplevel: true
      failureDetectionTime: 2000
    originalUrl: jdbc:mysql://test-mysql-j87kvt71h8-1.***.proxied:8666/test_database
    currentConnection: com.mysql.cj.jdbc.ConnectionImpl@2c35cdd1
    currentHostSpec: HostSpec@7acf610 [hostId=test-mysql-j87kvt71h8-1, host=test-mysql-j87kvt71h8-1.***.proxied, port=8666, WRITER, AVAILABLE, weight=100, null]
    isInTransaction: false
    dialect: software.amazon.jdbc.dialect.AuroraMysqlDialect
    pooledConnection: null
  pluginManager: 
    plugins: 
      software.amazon.jdbc.plugin.failover.FailoverConnectionPlugin: 
        properties: 
          failoverMode: reader-or-writer
          telemetryTracesBackend: xray
          tcpKeepAlive: false
          failureDetectionCount: 1
          telemetryMetricsBackend: otlp
          password: ***
          failoverTimeoutMs: 1
          database: test_database
          clusterInstanceHostPattern: ?.***.proxied:8666
          wrapperLogUnclosedConnections: true
          monitoring-socketTimeout: 3000
          enableTelemetry: true
          wrapperPlugins: failover
          connectTimeout: 10000
          socketTimeout: 1000
          failureDetectionInterval: 1000
          monitoring-connectTimeout: 3000
          user: test_user
          telemetrySubmitToplevel: true
          failureDetectionTime: 2000
        enableFailoverSetting: true
        enableConnectFailover: false
        failoverTimeoutMsSetting: 1
        failoverClusterTopologyRefreshRateMsSetting: 2000
        failoverWriterReconnectIntervalMsSetting: 2000
        failoverReaderConnectTimeoutMsSetting: 30000
        failoverMode: READER_OR_WRITER
        closedExplicitly: false
        closedReason: null
        isInTransaction: false
        rdsUrlType: RDS_INSTANCE
        skipFailoverOnInterruptedThread: false
      software.amazon.jdbc.plugin.DefaultConnectionPlugin: (blank)
    defaultConnProvider: 
      dataSourceClassName: com.mysql.cj.jdbc.MysqlDataSource
    effectiveConnProvider: null
  storageService: 
    software.amazon.jdbc.AllowedAndBlockedHosts: 
    software.amazon.jdbc.hostlistprovider.Topology: 
      1: 
        test-mysql-j87kvt71h8-3: HostSpec@7886b02f [hostId=test-mysql-j87kvt71h8-3, host=test-mysql-j87kvt71h8-3.***.proxied, port=8666, READER, AVAILABLE, weight=956, 2026-02-12 03:52:17.640777]
        test-mysql-j87kvt71h8-2: HostSpec@4893768e [hostId=test-mysql-j87kvt71h8-2, host=test-mysql-j87kvt71h8-2.***.proxied, port=8666, READER, AVAILABLE, weight=930, 2026-02-12 03:52:17.640877]
        test-mysql-j87kvt71h8-1: HostSpec@29aae4f6 [hostId=test-mysql-j87kvt71h8-1, host=test-mysql-j87kvt71h8-1.***.proxied, port=8666, WRITER, NOT_AVAILABLE, weight=43, 2026-02-12 03:52:16.737699]
  monitorService: 
    monitorCaches: 
      software.amazon.jdbc.plugin.strategy.fastestresponse.NodeResponseTimeMonitor: 
        monitors: 
      software.amazon.jdbc.hostlistprovider.ClusterTopologyMonitorImpl: 
        monitors: 
          1: 
            monitoringConnection: com.mysql.cj.jdbc.ConnectionImpl@e3cbb9
            writerHostSpec: HostSpec@7acf610 [hostId=test-mysql-j87kvt71h8-1, host=test-mysql-j87kvt71h8-1.***.proxied, port=8666, WRITER, AVAILABLE, weight=100, null]
            refreshRateNano: 30000000000
            highRefreshRateNano: 100000000
            monitoringProperties: 
              failoverMode: reader-or-writer
              telemetryTracesBackend: xray
              tcpKeepAlive: false
              failureDetectionCount: 1
              telemetryMetricsBackend: otlp
              password: ***
              failoverTimeoutMs: 1
              database: test_database
              clusterInstanceHostPattern: ?.***.proxied:8666
              wrapperLogUnclosedConnections: true
              monitoring-socketTimeout: 3000
              enableTelemetry: true
              wrapperPlugins: failover
              connectTimeout: 10000
              socketTimeout: 1000
              failureDetectionInterval: 1000
              monitoring-connectTimeout: 3000
              user: test_user
              telemetrySubmitToplevel: true
              failureDetectionTime: 2000
            initialHostSpec: HostSpec@7acf610 [hostId=test-mysql-j87kvt71h8-1, host=test-mysql-j87kvt71h8-1.***.proxied, port=8666, WRITER, AVAILABLE, weight=100, null]
            instanceTemplate: HostSpec@4d290dfb [hostId=?, host=?.***.proxied, port=8666, WRITER, AVAILABLE, weight=100, null]
            clusterId: 1
            isVerifiedWriterConnection: true
            isInPanicMode: false
      software.amazon.jdbc.plugin.customendpoint.CustomEndpointMonitorImpl: 
        monitors: 
      software.amazon.jdbc.plugin.efm2.HostMonitorImpl: 
        monitors: 
Latest events:
  [2026-02-12 03:52:18.446]: [Test worker] Current initial connection set to com.mysql.cj.jdbc.ConnectionImpl@2c35cdd1, HostSpec@7acf610 [hostId=test-mysql-j87kvt71h8-1, host=test-mysql-j87kvt71h8-1.***.proxied, port=8666, WRITER, AVAILABLE, weight=100, null]
  [2026-02-12 03:52:18.468]: [Test worker] Executing method: 'Connection.isValid'
  [2026-02-12 03:52:18.509]: [Test worker] Executing method: 'Connection.isValid'
  [2026-02-12 03:52:18.535]: [pool-358-thread-1] Executing method: 'Statement.executeQuery'
  [2026-02-12 03:52:20.539]: [pool-358-thread-1] Detected an exception while executing a command: Communications link failure

The last packet successfully received from the server was 2,010 milliseconds ago. The last packet sent successfully to the server was 2,030 milliseconds ago.
  [2026-02-12 03:52:20.540]: [pool-358-thread-1] Starting reader failover procedure.
  [2026-02-12 03:52:20.542]: [pool-358-thread-1] Reader failover elapsed in 2 ms.
  [2026-02-12 03:52:20.542]: [pool-358-thread-1] Current time / The exception raised time.

		at software.amazon.jdbc.util.WrapperUtils.injectAsSuppressedException(WrapperUtils.java:740)
		at software.amazon.jdbc.util.WrapperUtils.extendWithContext(WrapperUtils.java:665)
		at software.amazon.jdbc.util.WrapperUtils.executeWithPlugins(WrapperUtils.java:359)
		... 11 more

```


## Configuration

> [!WARNING]\
> Exception context injection is enabled by default. Users can opt out by explicitly disabling this feature.

These Java system properties can be set using `-D` flags when starting the JVM to configure global behavior of the AWS Advanced JDBC Wrapper.

| Property                                                 | Value                                             | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | Default Value              |
|----------------------------------------------------------|---------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------|
| `aws.jdbc.config.exception.context.enabled`              | boolean                                           | Enables injection of connection context into all exceptions.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `true` (enabled)           |
| `aws.jdbc.config.exception.context.injection.type`       | `ADD_SUPPRESSED_EXCEPTION` or `INJECT_TO_MESSAGE` | Specifies how context should be injected into the main exception. When `ADD_SUPPRESSED_EXCEPTION` is selected, a connection context is added as a suppressed exception of type software.amazon.jdbc.exceptions.SnapshotStateException to the main exception. The suppressed exception has a message that represents the connection state snapshot and a list of recent important events. When `INJECT_TO_MESSAGE` is selected, a connection context is added at the end of the main exception message. No suppressed exception is added to the main exception in this case. Verify with your application's exception reporting systems and tools which option is most suitable. | `ADD_SUPPRESSED_EXCEPTION` |
| `aws.jdbc.config.exception.context.driver.queue.ttl`     | long                                              | Time-to-live in milliseconds for collected important events at the driver level.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | `60000` (60 seconds)       |
| `aws.jdbc.config.exception.context.connection.queue.ttl` | long                                              | Time-to-live in milliseconds for collected important events at the connection level.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `60000` (60 seconds)       |




**Example:**
```bash
java -Daws.jdbc.config.exception.context.enabled=false -jar myapp.jar
```
 
