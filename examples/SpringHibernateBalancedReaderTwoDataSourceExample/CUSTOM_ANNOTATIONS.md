# Custom Annotations Documentation

## Overview

The SpringHibernateBalancedReaderTwoDataSourceExample demonstrates how to use custom annotations to route database operations between a writer datasource and a load-balanced reader datasource in a Spring application with Aurora clusters.

## Custom Annotation: @WithLoadBalancedReaderDataSource

### Purpose
Marks methods that should execute read operations against a load-balanced reader datasource instead of the default writer datasource.

### Definition
```java
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface WithLoadBalancedReaderDataSource {
}
```

### Key Characteristics
- **@Inherited**: Allows the annotation to be inherited by subclasses
- **@Retention(RetentionPolicy.RUNTIME)**: Makes the annotation available at runtime for reflection and AOP processing

### Usage Example
```java
@Retryable(value = { ShouldRetryTransactionException.class }, maxAttempts = 3)
@WithLoadBalancedReaderDataSource
@Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
public int getNumOfBooksTransactional() {
    // This method will use the load-balanced reader datasource
    return this.repository.findAll().stream().mapToInt(Book::getQuantityAvailable).sum();
}
```

## How It Works

### 1. Annotation Processing (WithLoadBalancedDataSourceInterception)

The `@WithLoadBalancedReaderDataSource` annotation is intercepted by an AspectJ aspect:

```java
@Aspect
@Component
public class WithLoadBalancedDataSourceInterception implements Ordered {

  @Around("@annotation(example.spring.WithLoadBalancedReaderDataSource)")
  public Object aroundMethod(ProceedingJoinPoint joinPoint) throws Throwable {
    try {
      LoadBalancedReaderDataSourceContext.enter();
      return joinPoint.proceed();
    } finally {
      LoadBalancedReaderDataSourceContext.exit();
    }
  }

  @Override
  public int getOrder() {
    // Higher priority than @Transactional to ensure datasource is set before transaction starts
    return Ordered.LOWEST_PRECEDENCE - 1;
  }
}
```

**Key Points:**
- Uses `@Around` advice to wrap annotated methods
- Sets the datasource context **before** the method executes
- Cleans up the context in the `finally` block
- Has higher priority than `@Transactional` (LOWEST_PRECEDENCE - 1) to ensure the datasource is selected before the transaction begins

### 2. Context Management (LoadBalancedReaderDataSourceContext)

Maintains thread-local state to track whether the current execution should use the reader datasource:

```java
public class LoadBalancedReaderDataSourceContext {

  private static final ThreadLocal<AtomicInteger> READER_DATASOURCE_LEVEL =
      ThreadLocal.withInitial(() -> new AtomicInteger(0));

  public static boolean isLoadBalancedReaderZone() {
    return READER_DATASOURCE_LEVEL.get().get() > 0;
  }

  public static void enter() {
    READER_DATASOURCE_LEVEL.get().incrementAndGet();
  }

  public static void exit() {
    READER_DATASOURCE_LEVEL.get().decrementAndGet();
  }
}
```

**Key Points:**
- Uses `ThreadLocal` to maintain per-thread state
- Uses `AtomicInteger` to support nested calls (increment/decrement pattern)
- Thread-safe for concurrent operations

### 3. Datasource Routing (RoutingDataSource)

Routes database connections based on the context:

```java
public class RoutingDataSource extends AbstractRoutingDataSource {

  private static final String WRITER = "writer";
  private static final String LOAD_BALANCED_READER = "load-balanced-reader";

  @Override
  protected Object determineCurrentLookupKey() {
    return LoadBalancedReaderDataSourceContext.isLoadBalancedReaderZone() 
        ? LOAD_BALANCED_READER 
        : WRITER;
  }
}
```

**Key Points:**
- Extends Spring's `AbstractRoutingDataSource`
- Checks the context to determine which datasource to use
- Returns "load-balanced-reader" when inside an annotated method, otherwise "writer"

## Complete Flow

1. **Method Call**: Application calls a method annotated with `@WithLoadBalancedReaderDataSource`
2. **Aspect Intercepts**: `WithLoadBalancedDataSourceInterception` intercepts the call
3. **Context Set**: `LoadBalancedReaderDataSourceContext.enter()` increments the counter
4. **Transaction Starts**: Spring's `@Transactional` starts a transaction (runs after the aspect due to ordering)
5. **Datasource Selection**: `RoutingDataSource.determineCurrentLookupKey()` checks the context and returns "load-balanced-reader"
6. **Connection Obtained**: Spring obtains a connection from the load-balanced reader datasource
7. **Method Executes**: The actual method logic runs using the reader connection
8. **Transaction Commits**: Spring commits the transaction
9. **Context Cleared**: `LoadBalancedReaderDataSourceContext.exit()` decrements the counter in the `finally` block

## Benefits

1. **Separation of Concerns**: Read operations can be routed to reader instances, reducing load on the writer
2. **Load Balancing**: Multiple reader instances can handle read traffic
3. **Declarative**: Simple annotation-based approach, no manual datasource switching
4. **Thread-Safe**: ThreadLocal ensures isolation between concurrent requests
5. **Nested Support**: Counter-based approach supports nested annotated method calls
6. **Failover Compatible**: Works with AWS JDBC Driver's failover capabilities

## Best Practices

1. **Use with @Transactional(readOnly = true)**: Always combine with read-only transactions
2. **Order Matters**: The aspect must run before `@Transactional` (configured via `getOrder()`)
3. **Avoid Writes**: Don't perform write operations in methods annotated with `@WithLoadBalancedReaderDataSource`
4. **Combine with @Retryable**: Handle transient failures during failover scenarios

## Example: Complete Method Annotation

```java
@Retryable(value = { ShouldRetryTransactionException.class }, maxAttempts = 3)
@WithLoadBalancedReaderDataSource
@Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
public int getNumOfBooksTransactional() {
    // Executes on load-balanced reader datasource
    // Retries up to 3 times on failure
    // Uses read-only transaction
    return this.repository.findAll().stream().mapToInt(Book::getQuantityAvailable).sum();
}
```

## Comparison: With vs Without Annotation

### Without @WithLoadBalancedReaderDataSource (Default)
```java
@Transactional(propagation = Propagation.REQUIRES_NEW)
public void updateBookAvailabilityTransactional() {
    // Uses writer datasource
    // Can perform both reads and writes
    final List<Book> allBooks = this.repository.findAll();
    this.repository.saveAll(allBooks);
}
```

### With @WithLoadBalancedReaderDataSource
```java
@WithLoadBalancedReaderDataSource
@Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
public int getNumOfBooksTransactional() {
    // Uses load-balanced reader datasource
    // Read-only operations
    return this.repository.findAll().stream().mapToInt(Book::getQuantityAvailable).sum();
}
```
