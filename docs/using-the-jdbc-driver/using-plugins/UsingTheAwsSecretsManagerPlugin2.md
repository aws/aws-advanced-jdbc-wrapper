# AWS Secrets Manager Plugin v2 (Stale-While-Revalidate)

The AWS Secrets Manager Connection Plugin v2 (`awsSecretsManager2`) is an alternative implementation of the [AWS Secrets Manager Connection Plugin](./UsingTheAwsSecretsManagerPlugin.md) that uses a **Stale-While-Revalidate (SWR)** caching strategy. It is designed for applications that require low-latency connections and resilience during AWS Secrets Manager outages.

## Plugin Availability
The plugin is available since version 3.3.0.

## Why Use `awsSecretsManager2`?

The original `awsSecretsManager` plugin blocks connection requests when cached credentials have expired, waiting for a synchronous fetch from AWS Secrets Manager before proceeding. This can cause:
- **Connection latency spikes** during credential rotation when the cache expires.
- **Complete connection failures** if AWS Secrets Manager is temporarily unreachable.

The `awsSecretsManager2` plugin addresses both issues:

| Scenario                                        | `awsSecretsManager` (original)         | `awsSecretsManager2` (SWR)                                      |
|-------------------------------------------------|----------------------------------------|-----------------------------------------------------------------|
| Credentials cached and not expired              | Uses cached credentials                | Uses cached credentials                                         |
| Credentials cached but expired                  | **Blocks** while fetching fresh secret | Connects **immediately** with stale credentials; refreshes async |
| First connection (no cache)                     | Fetches synchronously                  | Fetches synchronously (same behavior)                           |
| Secrets Manager unreachable, cache exists       | **Throws exception**                   | Connects using stale cached credentials                         |
| Secrets Manager unreachable, no cache           | Throws exception                       | Throws exception (same behavior)                                |
| Multiple connections with expired cache (burst) | Each blocks on its own fetch           | All connect immediately; **one** background refresh triggered   |

## How It Works

### SWR Connection Flow

```
connect() called
    |
    v
Has cached secret? ----NO----> Fetch synchronously (blocking, first time only)
    |                                    |
   YES                            Cache result, connect
    |
    v
Secret expired? ----NO----> Use cached secret, connect immediately
    |
   YES
    |
    v
Use stale secret immediately (non-blocking)
    + trigger async background refresh
    |
    v
Connect with stale credentials
    |
    v
Login failure? ----NO----> Return connection
    |
   YES (credentials rotated)
    |
    v
Try synchronous refresh, retry connect
```

### Thundering Herd Prevention

When many connections are opened simultaneously with expired credentials, only **one** background refresh is triggered. All connections proceed immediately using the stale cached credentials. The `ConcurrentHashMap.compute()` pattern guarantees at most one in-flight refresh per `(secretId, region)` key.

### Cache Sharing

Both `awsSecretsManager` and `awsSecretsManager2` share the same credential cache. This means:
- Credentials cached by one plugin version are available to the other.
- You can switch between plugin versions without losing cached credentials.
- Both plugins should not be enabled simultaneously in the same plugin chain.

## Enabling the AWS Secrets Manager Connection Plugin v2

> [!WARNING]\
> To use this plugin, you must include the runtime dependencies [Jackson Databind](https://central.sonatype.com/artifact/com.fasterxml.jackson.core/jackson-databind) and [AWS Secrets Manager](https://central.sonatype.com/artifact/software.amazon.awssdk/secretsmanager) in your project. These are the same dependencies required by the original plugin.

> [!WARNING]\
> To use this plugin, you must provide valid AWS credentials. See the [AWS credentials documentation](../AwsCredentials.md) for details.

To enable the plugin, add the plugin code `awsSecretsManager2` to the [`wrapperPlugins`](../UsingTheJdbcDriver.md#connection-plugin-manager-parameters) value:

```java
properties.setProperty("wrapperPlugins", "awsSecretsManager2,failover2,efm2");
```

This plugin requires valid AWS credentials. See more details at [AWS Credentials Configuration](../custom-configuration/AwsCredentialsConfiguration.md).

Verify plugin compatibility within your driver configuration using the [compatibility guide](../Compatibility.md).

## Configuration Parameters

The `awsSecretsManager2` plugin uses the same configuration parameters as the original plugin:

| Parameter                              |  Value  |                         Required                         | Description                                                                                                                                                                                                                                           | Example                 | Default Value |
|----------------------------------------|:-------:|:--------------------------------------------------------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------------------------|---------------|
| `secretsManagerSecretId`               | String  |                           Yes                            | Set this value to be the secret name or the secret ARN.                                                                                                                                                                                               | `secretId`              | `null`        |
| `secretsManagerRegion`                 | String  | Yes unless the `secretsManagerSecretId` is a Secret ARN. | Set this value to be the region your secret is in.                                                                                                                                                                                                    | `us-east-2`             | `us-east-1`   |
| `secretsManagerEndpoint`               | String  |                            No                            | Set this value to be the endpoint override to retrieve your secret from. This parameter value should be in the form of a URL, with a valid protocol (ex. `http://`) and domain (ex. `localhost`). A port number is not required.                      | `http://localhost:1234` | `null`        |
| `secretsManagerExpirationTimeSec`      | Integer |                            No                            | This property sets the time in seconds that secrets are cached before they are considered stale. Stale secrets are still used for connections while a background refresh occurs. Minimum value: `300` (values below this are clamped with a warning). | `600`                   | `870`         |
| `secretsManagerSecretUsernameProperty` | String  |                            No                            | Set this value to be the key in the JSON secret that contains the username for database connection.                                                                                                                                                   | `writerUsername`        | `username`    |
| `secretsManagerSecretPasswordProperty` | String  |                            No                            | Set this value to be the key in the JSON secret that contains the password for database connection.                                                                                                                                                   | `readerPassword`        | `password`    |

*NOTE* A Secret ARN has the following format: `arn:aws:secretsmanager:<Region>:<AccountId>:secret:SecretName-6RandomCharacters`

## Security Considerations

Because the SWR pattern deliberately serves stale (expired) cached credentials, there is a window during which a connection may be established using credentials that have already been rotated in AWS Secrets Manager. This matters in two scenarios:

1. **Routine credential rotation:** After the secret is rotated in Secrets Manager, the plugin continues to use the old credentials until the background refresh completes. If the database still accepts the old password (as is typical during RDS managed rotation with multi-user strategies), connections using stale credentials will succeed during this window.

2. **Emergency credential rotation (security incident):** If credentials are rotated as an incident response measure, the plugin will attempt to connect using the revoked credentials. If the database has already invalidated them, the connection will fail with a login error and the plugin will automatically perform a synchronous re-fetch and retry — so the impact is limited to one failed attempt per connection. However, if the database briefly accepts both old and new passwords during the rotation, the stale credentials could succeed.

For environments with strict credential freshness requirements, consider using the original `awsSecretsManager` plugin, which always blocks until the latest credentials are fetched when the cache expires.

## Migrating from `awsSecretsManager` to `awsSecretsManager2`

Migration requires only changing the plugin code. All configuration parameters are identical.

**Before:**
```java
properties.setProperty("wrapperPlugins", "awsSecretsManager,failover2,efm2");
```

**After:**
```java
properties.setProperty("wrapperPlugins", "awsSecretsManager2,failover2,efm2");
```

### Telemetry behavior change

The `secretsManager.fetchCredentials.count` telemetry counter has different semantics between the two plugins:
- **`awsSecretsManager`** increments the counter on every connection attempt, including cache hits.
- **`awsSecretsManager2`** increments the counter only when an actual API call is made to AWS Secrets Manager.

If you have monitoring dashboards or alerts based on this counter, you will see a significant drop in its value after migrating. This is expected and reflects the reduced number of API calls, not a loss of functionality.

## When to Use Which Plugin

Use **`awsSecretsManager`** (original) if:
- You need strict credential freshness and prefer to block until the latest secret is available.
- Your application tolerates occasional connection delays during credential rotation.

Use **`awsSecretsManager2`** (SWR) if:
- You need consistently low connection latency, even during credential rotation windows.
- You want your application to remain operational during temporary AWS Secrets Manager outages.
- You experience connection bursts where many connections open simultaneously.

## Secret Data
The secret stored in the AWS Secrets Manager should be a JSON object containing the properties `username` and `password`, same as the original plugin. If the secret contains different key names, you can specify them with the `secretsManagerSecretUsernameProperty` and `secretsManagerSecretPasswordProperty` parameters.
