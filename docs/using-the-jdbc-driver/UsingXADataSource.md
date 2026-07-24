# Using the AWS Advanced JDBC Wrapper as an XADataSource

The AWS Advanced JDBC Wrapper can be used as a `javax.sql.XADataSource` so it can participate in
distributed (XA / two-phase commit) transactions coordinated by a JTA transaction manager such as
the one built into WildFly/JBoss (Narayana), or a standalone manager like Atomikos or Bitronix.

The XA entry point is `software.amazon.jdbc.ds.AwsWrapperXADataSource`. It wraps a **target
driver-specific `XADataSource`** (for example `org.postgresql.xa.PGXADataSource`,
`com.mysql.cj.jdbc.MysqlXADataSource`, or `org.mariadb.jdbc.MariaDbDataSource` — MariaDB
Connector/J uses a single unified class that also implements `XADataSource`) so the wrapper's
plugin pipeline applies to the
application-facing `java.sql.Connection`, while the XA control surface (`XAConnection` /
`XAResource`) is delegated to the target driver.

## Configuration

`AwsWrapperXADataSource` exposes the same configuration surface as `AwsWrapperDataSource`, except
that `targetDataSourceClassName` must name a `javax.sql.XADataSource` implementation.

| Property                       | Description                                                                 |
|--------------------------------|-----------------------------------------------------------------------------|
| `targetDataSourceClassName`    | Fully-qualified class name of the target `XADataSource` (required). PostgreSQL: `org.postgresql.xa.PGXADataSource`. MySQL: `com.mysql.cj.jdbc.MysqlXADataSource`. MariaDB: `org.mariadb.jdbc.MariaDbDataSource` (a single unified class that also implements `XADataSource`). |
| `jdbcUrl`                      | The JDBC URL to connect with (e.g. `jdbc:aws-wrapper:postgresql://host/db`). Wrapper properties may be supplied as query parameters here (e.g. `?wrapperPlugins=iam&wrapperDialect=aurora-pg`). |
| `serverName` / `serverPort` / `database` | Used to build the URL when `jdbcUrl` is not set.                  |
| `jdbcProtocol`                 | e.g. `jdbc:postgresql:` (required when `jdbcUrl` is not set).                |
| `user` / `password`            | Credentials.                                                                |
| `targetDataSourceProperties`   | Properties applied to the target `XADataSource` (see below).                |

### Passing properties to the wrapper and the target XADataSource

There are two ways to supply configuration, and you can use either or both:

1. **`jdbcUrl` query string** — append parameters to the URL, for example
   `jdbc:aws-wrapper:postgresql://host:5432/db?wrapperPlugins=iam&ssl=true`. Both **wrapper**
   parameters (`wrapperPlugins`, `wrapperDialect`, `iamRegion`, …) and **target-driver** parameters
   (`ssl`, `ApplicationName`, `socketTimeout`, …) can be mixed here. The wrapper consumes its own
   parameters and **strips them from the URL before handing it to the target driver**, so the target
   only ever sees target-driver parameters.
2. **`targetDataSourceProperties`** — a `Properties` object whose entries are applied to the target
   `XADataSource` via its bean setters. This mirrors how `AwsWrapperDataSource` configures a target
   `DataSource`. Wrapper-specific keys placed here are also stripped before reaching the target.

### WildFly `<xa-datasource-class>` example

Wrapper properties (for example `wrapperPlugins` and `wrapperDialect`) are configured as query
parameters on the `jdbcUrl`. In XML, separate them with `&amp;`. Only **connect-time** plugins are
meaningful on the XA path — see [Benefits and limitations](#benefits-and-limitations-on-the-xa-path);
do not configure connection-switching plugins such as `readWriteSplitting`, `failover`, or
`failover2` on the XA datasource.

```xml
<datasource-class name="aws-wrapper-xa">
    <driver>aws-advanced-jdbc-wrapper</driver>
</datasource-class>

<xa-datasource jndi-name="java:jboss/datasources/ExampleXADS" pool-name="ExampleXADS">
    <driver>aws-advanced-jdbc-wrapper</driver>
    <xa-datasource-class>software.amazon.jdbc.ds.AwsWrapperXADataSource</xa-datasource-class>
    <xa-datasource-property name="targetDataSourceClassName">org.postgresql.xa.PGXADataSource</xa-datasource-property>
    <!-- Wrapper properties are passed as jdbcUrl query parameters (XML-escape '&' as '&amp;').
         Here: confirm the Aurora PostgreSQL dialect up front and enable the host-monitoring (efm2)
         connect-time plugin. Add or remove connect-time plugins to taste. -->
    <xa-datasource-property name="jdbcUrl">jdbc:aws-wrapper:postgresql://db.cluster-xyz.us-east-1.rds.amazonaws.com:5432/mydb?wrapperDialect=aurora-pg&amp;wrapperPlugins=efm2</xa-datasource-property>
    <security>
        <user-name>myuser</user-name>
        <password>mypassword</password>
    </security>
</xa-datasource>
```

#### IAM authentication example

IAM database authentication works on the XA path: the IAM plugin generates a short-lived token at
connect time and the wrapper applies it to the target `XADataSource`. Enable the `iam` plugin, set
the region, use the IAM database user, and omit the password.

```xml
<xa-datasource jndi-name="java:jboss/datasources/IamXADS" pool-name="IamXADS">
    <driver>aws-advanced-jdbc-wrapper</driver>
    <xa-datasource-class>software.amazon.jdbc.ds.AwsWrapperXADataSource</xa-datasource-class>
    <xa-datasource-property name="targetDataSourceClassName">org.postgresql.xa.PGXADataSource</xa-datasource-property>
    <xa-datasource-property name="jdbcUrl">jdbc:aws-wrapper:postgresql://db.cluster-xyz.us-east-1.rds.amazonaws.com:5432/mydb?wrapperPlugins=iam&amp;iamRegion=us-east-1</xa-datasource-property>
    <security>
        <!-- IAM database user; no password (the IAM plugin supplies a generated token). -->
        <user-name>iam_user</user-name>
    </security>
</xa-datasource>
```

## Benefits and limitations on the XA path

On the XA datasource the wrapper's value is **primarily connect-time** rather than switch-time: no
connection-switching plugin may move an in-flight XA branch to a different session (the one runtime
exception is Blue/Green Deployment, which pauses and resumes rather than switching mid-transaction).
This is important to understand before adopting it.

**What the wrapper provides on the XA path:**

- **Authentication** — IAM authentication and AWS Secrets Manager / federated credentials work
  unchanged (they act while establishing the connection).
- **Topology-aware writer discovery** — connect to the current writer, handling cluster/custom
  endpoints and stale DNS.
- **Faster reconnection for the next transaction** — after an instance failover, when the
  transaction manager's pool discards the broken `XAConnection` and requests a new one, the wrapper
  routes it to the promoted writer via topology (faster than plain DNS).
- **Observability** — enhanced logging, exception context, and telemetry.
- **Blue/Green Deployment support** — not limited to connect time. The Blue/Green Deployment plugin
  also acts at runtime: it can hold execution while a switchover is in progress and resume against
  the promoted topology once the switchover completes.

**What is NOT supported on the XA path:**

- **Read/write splitting is not supported.** An XA transaction branch is pinned to one physical
  database session and the `XAResource` is bound to it, so routing a statement to a different
  (reader) session would run it outside the transaction. Read/write splitting therefore has no
  valid use on the XA datasource, and it is skipped during an XA transaction (a warning is logged if
  it is configured).
- **Transparent failover of an in-flight XA transaction is not possible.** This is inherent to XA:
  if the database session is lost mid-transaction, the branch cannot be moved to another node. The
  wrapper fails fast (SQLSTATE `08007`) so the transaction manager rolls the branch back. Recovery
  happens for the *next* transaction, not the current one.
- **The wrapper's internal connection pool is not applicable.** XA connection pooling is provided
  by the transaction manager / application server. Configuring the wrapper's internal connection
  pool (or a custom connection provider) together with the XA datasource fails fast.

If your only goal is transparent failover, note that the XA datasource offers little in that
regard — again, an inherent property of XA rather than a wrapper limitation.

## Recommended architecture: the two-datasource pattern

For applications that need both distributed transactions and the wrapper's failover / read-write
splitting features, use **two datasources**:

- an `AwsWrapperXADataSource` (no connection-switching plugins, no internal pool) for the
  **write / 2PC** path, and
- a non-XA `AwsWrapperDataSource` with failover + read/write splitting (and internal pooling) for
  the **read** path.

The read path keeps the headline switching features; the write path gets what XA allows.

```xml
<!-- Write / XA path: connect-time plugins only (no connection-switching plugins). Wrapper
     properties are jdbcUrl query parameters; XML-escape '&' as '&amp;'. -->
<xa-datasource jndi-name="java:jboss/datasources/WriterXADS" pool-name="WriterXADS">
    <driver>aws-advanced-jdbc-wrapper</driver>
    <xa-datasource-class>software.amazon.jdbc.ds.AwsWrapperXADataSource</xa-datasource-class>
    <xa-datasource-property name="targetDataSourceClassName">org.postgresql.xa.PGXADataSource</xa-datasource-property>
    <xa-datasource-property name="jdbcUrl">jdbc:aws-wrapper:postgresql://cluster-endpoint:5432/mydb?wrapperDialect=aurora-pg&amp;wrapperPlugins=efm2</xa-datasource-property>
</xa-datasource>

<!-- Read path (non-XA, with failover + read/write splitting). Here connection-switching plugins
     are appropriate because this datasource does not participate in XA transactions. -->
<datasource jndi-name="java:jboss/datasources/ReaderDS" pool-name="ReaderDS">
    <connection-url>jdbc:aws-wrapper:postgresql://reader-cluster-endpoint:5432/mydb?wrapperPlugins=readWriteSplitting,failover2,efm2</connection-url>
    <driver>aws-advanced-jdbc-wrapper</driver>
</datasource>
```

## Notes

- **PostgreSQL server configuration:** PostgreSQL disables prepared (two-phase) transactions by
  default (`max_prepared_transactions = 0`). To use XA / two-phase commit against PostgreSQL (or
  Aurora PostgreSQL), set `max_prepared_transactions` to a value greater than 0 (in the RDS/Aurora
  cluster parameter group for Aurora) and restart. Otherwise `XAResource.prepare` fails. MySQL/InnoDB
  supports XA by default.
- `AwsWrapperXADataSource` requires a target `XADataSource`; a plain `DataSource` class name is
  rejected at `getXAConnection()` time.
- Because each `XAConnection.getConnection()` returns a fresh logical connection over the same
  physical session, the wrapper builds a fresh plugin pipeline per logical connection to keep plugin
  state isolated between checkouts.
