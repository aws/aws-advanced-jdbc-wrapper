# Custom Annotations Documentation

## Overview

The SpringHibernateBalancedReaderOneDataSourceExample demonstrates a **single datasource approach** for load-balanced read-write and read-only connections using the AWS Advanced JDBC Wrapper's built-in Read/Write Splitting Plugin. Unlike the TwoDataSourceExample, this approach does **not** use custom annotations for datasource routing.

## Key Difference from TwoDataSourceExample

| Aspect | OneDataSourceExample | TwoDataSourceExample |
|--------|---------------------|----------------------|
| **Datasources** | Single datasource | Two separate datasources (writer + reader) |
| **Routing Mechanism** | AWS Advanced JDBC Wrapper's Read/Write Splitting Plugin | Custom `@WithLoadBalancedReaderDataSource` annotation |
| **Configuration** | Connection string with `wrapperProfileName=F0` | Spring's `AbstractRoutingDataSource` + AOP |
| **Custom Annotations** | None (uses standard Spring annotations) | Custom annotation with AspectJ interception |
| **Complexity** | Simpler, driver-managed | More complex, application-managed |

## Standard Annotations Used

### @Transactional(readOnly = true)

The key to routing in this example is Spring's standard `@Transactional` annotation with the `readOnly` parameter:

```java
@Retryable(value = { ShouldRetryTransactionException.class }, maxAttempts = 3)
@Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
public int getNumOfBooksTransactional() {
    // AWS Advanced JDBC Wrapper automatically routes this to a reader instance
    return this.repository.findAll().stream().mapToInt(Book::getQuantityAvailable).sum();
}
```

**How it works:**
- The AWS Advanced JDBC Wrapper's Read/Write Splitting Plugin detects the `readOnly` flag on the transaction
- When `readOnly = true`, the driver automatically routes the connection to a reader instance
- When `readOnly = false` (or not specified), the driver routes to the writer instance

### @Transactional (Write Operations)

For write operations, use `@Transactional` without the `readOnly` flag:

```java
@Retryable(value = { ShouldRetryTransactionException.class, TransactionSystemException.class}, maxAttempts = 3)
@Transactional(propagation = Propagation.REQUIRES_NEW)
public void updateBookAvailabilityTransactional() {
    // AWS JDBC Driver automatically routes this to the writer instance
    final List<Book> allBooks = this.repository.findAll();
    this.repository.saveAll(allBooks);
}
```

### @Retryable

Used to handle transient failures during failover:

```java
@Retryable(value = { ShouldRetryTransactionException.class }, maxAttempts = 3)
```

**Purpose:**
- Automatically retries failed transactions up to 3 times
- Handles `ShouldRetryTransactionException` thrown during failover events
- Works with the custom `HibernateExceptionTranslator` to convert JDBC connection exceptions

### @EnableRetry

Enables Spring's retry mechanism at the application level:

```java
@SpringBootApplication
@EnableRetry
public class SpringHibernateBalancedReaderOneDataSourceExampleApplication {
    // ...
}
```

## Configuration

### Connection String Configuration

The datasource is configured with the AWS Advanced JDBC Wrapper's Read/Write Splitting Plugin:

```yaml
spring:
  datasource:
    load-balanced-writer-and-reader-datasource:
      url: jdbc:aws-wrapper:postgresql://cluster.XYZ.us-east-2.rds.amazonaws.com:5432/postgres?wrapperProfileName=F0&readerHostSelectorStrategy=roundRobin
      driver-class-name: software.amazon.jdbc.Driver
```

**Key Parameters:**
- `wrapperProfileName=F0`: Enables the Read/Write Splitting Plugin with internal connection pooling
- `readerHostSelectorStrategy=roundRobin`: Distributes read traffic evenly across reader instances

### Custom Exception Translation

The `Config` class includes a custom `HibernateExceptionTranslator` to handle failover exceptions:

```java
@Bean
public HibernateExceptionTranslator hibernateExceptionTranslator(){
  return new HibernateExceptionTranslator() {
    @Override
    public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
      // Convert JDBC connection exceptions to retryable exceptions
      if (ex.getCause() != null && ex.getCause() instanceof JDBCConnectionException) {
        return new ShouldRetryTransactionException(ex);
      }
      return super.translateExceptionIfPossible(ex);
    }
  };
}
```

**Purpose:**
- Converts `JDBCConnectionException` (including failover exceptions) to `ShouldRetryTransactionException`
- Enables `@Retryable` to automatically retry failed transactions
- Provides seamless failover handling

## Complete Flow

### Read Operation Flow

1. **Method Call**: Application calls a method with `@Transactional(readOnly = true)`
2. **Transaction Start**: Spring starts a read-only transaction
3. **Connection Request**: Hibernate requests a connection from the datasource
4. **Driver Detection**: AWS JDBC Driver detects the read-only transaction flag
5. **Reader Routing**: Driver routes the connection to a reader instance (using round-robin strategy)
6. **Query Execution**: Read queries execute on the reader instance
7. **Transaction Commit**: Spring commits the read-only transaction
8. **Connection Return**: Connection returns to the pool

### Write Operation Flow

1. **Method Call**: Application calls a method with `@Transactional` (no readOnly flag)
2. **Transaction Start**: Spring starts a read-write transaction
3. **Connection Request**: Hibernate requests a connection from the datasource
4. **Driver Detection**: AWS JDBC Driver detects the read-write transaction
5. **Writer Routing**: Driver routes the connection to the writer instance
6. **Query Execution**: Read and write queries execute on the writer instance
7. **Transaction Commit**: Spring commits the transaction
8. **Connection Return**: Connection returns to the pool

### Failover Handling Flow

1. **Failover Event**: Database failover occurs during a transaction
2. **Exception Thrown**: Driver throws `FailoverSQLException` or `JDBCConnectionException`
3. **Exception Translation**: `HibernateExceptionTranslator` converts to `ShouldRetryTransactionException`
4. **Retry Triggered**: `@Retryable` catches the exception and retries the transaction
5. **New Connection**: Driver establishes a connection to the new writer/reader
6. **Transaction Retry**: Transaction executes again on the new instance

## Benefits of Single Datasource Approach

1. **Simplicity**: No custom annotations or AOP configuration needed
2. **Driver-Managed**: AWS JDBC Driver handles all routing logic
3. **Automatic Detection**: Uses standard Spring `readOnly` flag
4. **Internal Pooling**: Driver manages connection pools for both writer and readers
5. **Failover Support**: Built-in failover handling with retry logic
6. **Less Code**: Fewer components to maintain

## Best Practices

1. **Always Use readOnly Flag**: Explicitly set `readOnly = true` for read operations
   ```java
   @Transactional(readOnly = true)
   ```

2. **Use Configuration Profiles**: Leverage predefined profiles (F0, D, E, F) for optimal settings
   ```
   wrapperProfileName=F0
   ```

3. **Configure Reader Selection**: Choose appropriate reader selection strategy
   ```
   readerHostSelectorStrategy=roundRobin  // or random, leastConnections
   ```

4. **Enable Retry Logic**: Always use `@Retryable` for failover resilience
   ```java
   @Retryable(value = { ShouldRetryTransactionException.class }, maxAttempts = 3)
   ```

5. **Custom Exception Translation**: Implement `HibernateExceptionTranslator` to convert failover exceptions

## Example: Complete Method Annotations

### Read-Only Method
```java
@Retryable(value = { ShouldRetryTransactionException.class }, maxAttempts = 3)
@Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
public int getNumOfBooksTransactional() {
    // Automatically routed to reader instance
    return this.repository.findAll().stream().mapToInt(Book::getQuantityAvailable).sum();
}
```

### Read-Write Method
```java
@Retryable(value = {ShouldRetryTransactionException.class, TransactionSystemException.class}, maxAttempts = 3)
@Transactional(propagation = Propagation.REQUIRES_NEW)
public void updateBookAvailabilityTransactional() {
    // Automatically routed to writer instance
    final List<Book> allBooks = this.repository.findAll();
    for (Book book : allBooks) {
        book.setQuantityAvailable(book.getQuantityAvailable() + 1);
    }
    this.repository.saveAll(allBooks);
}
```

## Comparison: When to Use Each Approach

### Performance Considerations

**OneDataSource Performance Trade-offs:**
- Uses a **single connection pool** where the driver switches the underlying physical connection between writer and reader instances
- **Connection switching overhead** occurs when transitioning between read and write operations
- **Pool contention** - writer and reader operations compete for connections from the same pool
- Less optimal for high-throughput applications with frequent read/write interleaving

**TwoDataSource Performance Advantages:**
- **Dedicated connection pools** for writer and readers eliminate switching overhead
- **No connection switching** - connections stay bound to their instance type
- **Better resource isolation** - read-heavy workloads don't impact write connection availability
- **Independent pool tuning** - optimize writer pool (smaller, longer-lived) and reader pool (larger, more aggressive) separately

### Use OneDataSourceExample (This Example) When:
- You want simplicity and less code
- You're comfortable with driver-managed routing
- You have low to moderate traffic
- Read/write operations are infrequent or not heavily interleaved
- You prefer configuration to code

### Use TwoDataSourceExample When:
- You need fine-grained control over datasource selection
- You have high-throughput, performance-critical workloads
- Your application frequently interleaves read and write operations
- You want to use external connection pools (e.g., HikariCP) for each datasource
- You need to optimize pool configurations separately for writer and readers
- You have complex routing requirements beyond read/write splitting
- You need to route based on custom business logic
- You're integrating with existing multi-datasource infrastructure

**Recommendation:** Start with OneDataSource for simplicity. If performance profiling reveals connection switching overhead or pool contention as bottlenecks, migrate to TwoDataSource for better performance at the cost of increased complexity.

## Configuration Profiles

The example uses `wrapperProfileName=F0`, which includes:
- Read/Write Splitting Plugin
- Failover Plugin
- Host Monitoring Plugin
- Internal Connection Pooling

Other available profiles:
- **D**: Basic read/write splitting without internal pooling
- **E**: Read/write splitting with enhanced monitoring
- **F**: Read/write splitting with failover support (no internal pooling)
- **F0**: Read/write splitting with failover and internal pooling (recommended)

See [Configuration Profiles](../../docs/using-the-jdbc-driver/UsingTheJdbcDriver.md#configuration-profiles) for details.

## Summary

The SpringHibernateBalancedReaderOneDataSourceExample demonstrates a **driver-managed approach** to read/write splitting using standard Spring annotations. The AWS JDBC Driver automatically routes connections based on the `@Transactional(readOnly)` flag, eliminating the need for custom annotations or AOP configuration. This approach is simpler, requires less code, and is ideal for most use cases.
