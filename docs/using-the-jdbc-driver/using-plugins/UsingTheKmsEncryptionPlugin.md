# Using the KMS Encryption Plugin

## Plugin Availability
The plugin is available since version 4.0.0.

The KMS Encryption Plugin provides transparent client-side encryption using AWS Key Management Service (KMS). This plugin automatically encrypts sensitive data before storing it in the database and decrypts it when retrieving data, based on metadata configuration.

## Features

- **Transparent Encryption**: Automatically encrypts and decrypts data without changing your application code
- **AWS KMS Integration**: Uses AWS KMS for secure key management and encryption operations
- **Metadata-Driven**: Configurable encryption based on table and column metadata
- **Audit Logging**: Optional audit logging for encryption operations
- **Minimal Performance Impact**: Efficient encryption with caching and optimized operations

## Prerequisites

- AWS KMS key with appropriate permissions
- Database table to store encryption metadata
- AWS credentials configured (via IAM roles, profiles, or environment variables)
- **JSqlParser 4.9 dependency** - Required for SQL parsing and analysis
- **The `sqlParser` plugin** - Must be loaded together with `kmsEncryption` (for example `wrapperPlugins=sqlParser,kmsEncryption`). It parses each statement and maps its parameters to columns; without it, `kmsEncryption` refuses to prepare statements

### Creating AWS KMS Master Key

1. **Create a KMS Key** in AWS Console or using AWS CLI:
```bash
aws kms create-key --description "Database encryption master key" --key-usage ENCRYPT_DECRYPT
```

2. **Note the Key ARN** from the response - you'll need this when setting up encryption metadata.

3. **Set Key Permissions** - Ensure your application has the following KMS permissions:
   - `kms:Encrypt`
   - `kms:Decrypt` 
   - `kms:GenerateDataKey`
   - `kms:DescribeKey`

### Data Key Management

The plugin automatically manages data keys:
- **Data keys are generated** automatically using the master key when encrypting new data
- **Data keys are cached** in memory for performance (configurable via `dataKeyCache*` properties)
- **Data keys are encrypted** with the master key and stored alongside encrypted data
- **No manual data key creation** is required

### Metadata Storage

Create the required metadata tables to store encryption configuration:

```sql
-- Key storage table (must be created first due to foreign key)
CREATE TABLE key_storage (
    id SERIAL PRIMARY KEY,
    key_id VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    master_key_arn VARCHAR(512) NOT NULL,
    encrypted_data_key TEXT NOT NULL,
    key_spec VARCHAR(50) DEFAULT 'AES_256',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Encryption metadata table
CREATE TABLE encryption_metadata (
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    encryption_algorithm VARCHAR(50) NOT NULL,
    key_id INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, column_name),
    FOREIGN KEY (key_id) REFERENCES key_storage(id)
);
```

### Setting Up Encryption Metadata

Use the KeyManagementUtility to properly configure encryption for your columns:

```java
// Initialize KeyManagementUtility
KeyManagementUtility keyManagementUtility = new KeyManagementUtility(
    keyManager, metadataManager, dataSource, kmsClient);

// Configure encryption for a column
String keyId = keyManagementUtility.initializeEncryptionForColumn(
    "users",           // table name
    "ssn",             // column name  
    masterKeyArn,      // KMS master key ARN
    "AES-256-GCM"      // encryption algorithm
);
```

**Alternative: Direct metadata insertion (not recommended for production):**
```sql
INSERT INTO encryption_metadata (table_name, column_name, encryption_algorithm, key_id) 
VALUES ('users', 'ssn', 'AES-256-GCM', 'your-generated-key-id');
```

### Adding JSqlParser Dependency

The KMS Encryption Plugin requires JSqlParser 4.9 for SQL statement analysis. Add this dependency to your project:

**Maven:**
```xml
<dependency>
    <groupId>com.github.jsqlparser</groupId>
    <artifactId>jsqlparser</artifactId>
    <version>4.9</version>
</dependency>
```

**Gradle:**
```gradle
implementation 'com.github.jsqlparser:jsqlparser:4.9'
```

## Configuration

### Connection Properties

| Property | Available Since Version | Description | Required | Default |
|----------|-------------------------|-------------|----------|---------|
| `kms.region` | 4.0.0 | AWS KMS region for encryption operations | Yes | None |
| `encryptionMetadataSchema` | 4.0.0 | Schema name for encryption metadata tables | No | `encrypt` |
| `encryptionFailOnUnmappedParameter` | 4.6.0 | When `true` (default), the plugin rejects a statement on a table with encrypted columns with an `SQLException` at prepare time if it cannot reliably map the statement's parameters to columns (see [Limitations](#limitations)). When `false`, the plugin only logs a WARNING and prepares the statement; values bound to encrypted columns may then be written unencrypted. | No | `true` |
| `metadataCacheEnabled` | 4.0.0 | Enable/disable metadata caching | No | `true` |
| `metadataCacheExpirationMinutes` | 4.0.0 | Metadata cache expiration time in minutes | No | `60` |
| `metadataCacheRefreshIntervalMs` | 4.0.0 | Metadata cache refresh interval in milliseconds | No | `300000` |
| `keyManagementMaxRetries` | 4.0.0 | Maximum number of retries for key management operations | No | `3` |
| `keyManagementRetryBackoffBaseMs` | 4.0.0 | Base backoff time in milliseconds for key management retries | No | `100` |
| `auditLoggingEnabled` | 4.0.0 | Enable/disable audit logging | No | `false` |
| `dataKeyCacheEnabled` | 4.0.0 | Enable/disable data key caching | No | `true` |
| `dataKeyCacheMaxSize` | 4.0.0 | Maximum size of data key cache | No | `1000` |
| `dataKeyCacheExpirationMs` | 4.0.0 | Data key cache expiration in milliseconds | No | `3600000` |

### Example Connection String

```java
String url = "jdbc:aws-wrapper:postgresql://your-cluster.cluster-xyz.us-east-1.rds.amazonaws.com:5432/mydb";
Properties props = new Properties();
props.setProperty("user", "username");
props.setProperty("password", "password");
props.setProperty("wrapperPlugins", "sqlParser,kmsEncryption");
props.setProperty("kms.region", "us-east-1");
props.setProperty("auditLoggingEnabled", "true");

Connection conn = DriverManager.getConnection(url, props);
```

The `sqlParser` plugin is required and must run before `kmsEncryption`. List it first; with `autoSortWrapperPluginOrder` enabled (the default) the driver also sorts it ahead of `kmsEncryption`. If `sqlParser` is missing, preparing a statement fails with `SQL parse results not found in call context`.

## Setup

### 1. Create Encryption Metadata Table

First, create the required tables to store encryption metadata and keys:

```sql
-- Key storage table (must be created first due to foreign key)
CREATE TABLE key_storage (
    id SERIAL PRIMARY KEY,
    key_id VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    master_key_arn VARCHAR(512) NOT NULL,
    encrypted_data_key TEXT NOT NULL,
    key_spec VARCHAR(50) DEFAULT 'AES_256',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Encryption metadata table
CREATE TABLE encryption_metadata (
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    encryption_algorithm VARCHAR(50) NOT NULL,
    key_id INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, column_name),
    FOREIGN KEY (key_id) REFERENCES key_storage(id)
);
```

### 2. Configure Column Encryption

**Recommended: Use KeyManagementUtility for proper key management:**

```java
KeyManagementUtility keyManagementUtility = new KeyManagementUtility(
    keyManager, metadataManager, dataSource, kmsClient);

// Configure encryption for sensitive columns
keyManagementUtility.initializeEncryptionForColumn("customers", "ssn", masterKeyArn);
keyManagementUtility.initializeEncryptionForColumn("customers", "credit_card", masterKeyArn);
keyManagementUtility.initializeEncryptionForColumn("customers", "phone", masterKeyArn);
keyManagementUtility.initializeEncryptionForColumn("customers", "address", masterKeyArn);
```

**Alternative: Direct SQL insertion (for testing only):**
```sql
-- Configure encryption for sensitive columns in the customers table
INSERT INTO encryption_metadata (table_name, column_name, encryption_algorithm, key_id)
VALUES 
    ('customers', 'ssn', 'AES-256-GCM', 'generated-key-id-1'),
    ('customers', 'credit_card', 'AES-256-GCM', 'generated-key-id-2'),
    ('customers', 'phone', 'AES-256-GCM', 'generated-key-id-3'),
    ('customers', 'address', 'AES-256-GCM', 'generated-key-id-4');
```

### 3. Create Your Application Tables

Create your application tables normally:

```sql
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone BYTEA,              -- Will be encrypted
    ssn BYTEA,                -- Will be encrypted
    credit_card BYTEA,        -- Will be encrypted
    address BYTEA,            -- Will be encrypted
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Usage

Once configured, the plugin works transparently:

```java
// Insert data - sensitive fields are automatically encrypted
String sql = "INSERT INTO customers (first_name, last_name, email, phone, ssn, credit_card, address) VALUES (?, ?, ?, ?, ?, ?, ?)";
try (PreparedStatement stmt = connection.prepareStatement(sql)) {
    stmt.setString(1, "John");
    stmt.setString(2, "Doe");
    stmt.setString(3, "john.doe@example.com");
    stmt.setString(4, "555-123-4567");     // Automatically encrypted
    stmt.setString(5, "123-45-6789");      // Automatically encrypted
    stmt.setString(6, "4111-1111-1111-1111"); // Automatically encrypted
    stmt.setString(7, "123 Main St, City, ST 12345"); // Automatically encrypted
    stmt.executeUpdate();
}

// Query data - encrypted fields are automatically decrypted
String query = "SELECT * FROM customers WHERE customer_id = ?";
try (PreparedStatement stmt = connection.prepareStatement(query)) {
    stmt.setInt(1, customerId);
    try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
            String phone = rs.getString("phone");         // Automatically decrypted
            String ssn = rs.getString("ssn");             // Automatically decrypted
            String creditCard = rs.getString("credit_card"); // Automatically decrypted
            String address = rs.getString("address");     // Automatically decrypted
            
            // Use the decrypted data normally
            System.out.println("Phone: " + phone);
        }
    }
}
```

## SQL Annotation Syntax

In addition to metadata-based encryption, you can use SQL comment annotations to explicitly mark parameters for encryption. This provides explicit control over which parameters are encrypted, regardless of column position or metadata configuration.

### Annotation Format

Use SQL comments with the format `/*@encrypt:table.column*/` immediately before the parameter placeholder:

```java
// Basic annotation usage
String sql = "INSERT INTO customers (name, ssn, email) VALUES (?, /*@encrypt:customers.ssn*/ ?, ?)";
try (PreparedStatement stmt = connection.prepareStatement(sql)) {
    stmt.setString(1, "John Doe");
    stmt.setString(2, "123-45-6789");  // Encrypted via annotation
    stmt.setString(3, "john@example.com");
    stmt.executeUpdate();
}

// Update with annotation
String updateSql = "UPDATE customers SET ssn = /*@encrypt:customers.ssn*/ ? WHERE customer_id = ?";
try (PreparedStatement stmt = connection.prepareStatement(updateSql)) {
    stmt.setString(1, "987-65-4321");  // Encrypted
    stmt.setInt(2, customerId);
    stmt.executeUpdate();
}

// Different column order - annotation ensures correct encryption
String sql2 = "INSERT INTO customers (name, email, ssn) VALUES (?, ?, /*@encrypt:customers.ssn*/ ?)";
try (PreparedStatement stmt = connection.prepareStatement(sql2)) {
    stmt.setString(1, "Jane Smith");
    stmt.setString(2, "jane@example.com");
    stmt.setString(3, "555-66-7777");  // Encrypted even though it's the 3rd parameter
    stmt.executeUpdate();
}
```

### When to Use Annotations

**Use annotations when:**
- You need explicit control over which parameters are encrypted
- Column order varies in different queries
- Working with dynamic SQL where column positions might change
- You want to encrypt specific parameters without modifying metadata

**Use metadata (recommended) when:**
- You want centralized encryption configuration
- You don't want to modify SQL statements
- You need automatic encryption based on column names
- You want easier maintenance and consistency

**Note:** Both approaches can be used together. When an annotation is present, it takes precedence over metadata configuration for that specific parameter. If the annotation names a different column than the one the statement assigns the parameter to, the plugin uses the annotation and logs a WARNING.

### Annotation Syntax Rules

1. The annotation must be a SQL comment: `/*@encrypt:table.column*/`
2. It must immediately precede the `?` parameter placeholder. It applies to the next real placeholder that follows it
3. The format is: `table.column` (both table and column names required)
4. Whitespace between the annotation and `?` is allowed but not required
5. Only an annotation that starts its own block comment outside string literals and other comments counts. Annotation-like text inside a string literal, a line comment (`-- ...`, MySQL `# ...`) or another block comment is ignored
6. A `?` inside a string literal, a quoted identifier, a comment or a PostgreSQL dollar-quoted string is not a parameter placeholder, and neither is the PostgreSQL `??` operator. Such characters do not shift the parameter numbering
7. An annotation that is not followed by a placeholder is ignored, and the plugin logs a WARNING
8. Annotations do not bypass the fail-closed check described in [Limitations](#limitations). A statement that the plugin cannot map reliably on a table with encrypted columns is rejected even if it contains annotations

> [!WARNING]\
> **The annotation is sent to the database as part of the statement.** It is a valid SQL comment, so it does not affect how the statement executes, but it is not removed from the SQL text: it appears in server-side statement logs, in `pg_stat_statements`, in slow-query logs, and in any query-capture tooling. Since the annotation names the table and column being encrypted, using it discloses which columns hold sensitive data to anyone who can read those logs. If that matters in your environment, mark columns through the metadata table instead of through annotations.

**Valid examples:**
```sql
/*@encrypt:users.ssn*/ ?
/*@encrypt:customers.credit_card*/ ?
/*@encrypt:orders.payment_info*/    ?
```

**Invalid examples:**
```sql
? /*@encrypt:users.ssn*/           -- Annotation must come BEFORE the ?
/*@encrypt:ssn*/ ?                 -- Missing table name
/* @encrypt:users.ssn */ ?         -- Space after /* not allowed
```

## Security Considerations

### KMS Key Permissions

Ensure your application has the necessary KMS permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kms:Encrypt",
                "kms:Decrypt",
                "kms:GenerateDataKey"
            ],
            "Resource": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
        }
    ]
}
```

### Data Protection

- Encrypted data is stored as binary data in the database
- The original data never leaves your application - encryption/decryption happens locally using data keys from KMS
- Only encryption keys are managed by AWS KMS, not the actual data
- Consider using different KMS keys for different environments (dev, staging, prod)

### Performance Considerations

- KMS calls are only needed for data key generation/decryption, not for each data encryption/decryption
- Data key caching significantly reduces KMS API calls for repeated operations
- Consider the impact on performance for high-throughput applications during key rotation
- KMS has rate limits that may affect very high-volume key operations
- The plugin caches both metadata and data keys to minimize external calls

## Troubleshooting

### Common Issues

1. **Missing KMS Permissions**: Ensure your AWS credentials have the necessary KMS permissions
2. **Metadata Table Not Found**: Verify the encryption metadata table exists and is accessible
3. **Region Mismatch**: Ensure the KMS region matches where your key is located
4. **Invalid Key ID**: Verify the KMS key ID or ARN is correct and accessible
5. **`Refusing to prepare a statement on table '...': it has encrypted columns, but the statement's parameters cannot be reliably mapped to columns (...)`**: The statement writes to a table with encrypted columns, but the plugin cannot tell which parameter belongs to which column, so it cannot guarantee that the encrypted column values get encrypted. The text in parentheses names the reason, for example `INSERT without a column list` or `column ssn receives parameter(s) 2 inside an expression`. The message never contains the SQL text or parameter values. To fix it, rewrite the statement in one of the supported shapes listed in [Limitations](#limitations):
   - list the columns explicitly: `INSERT INTO users (name, ssn) VALUES (?, ?)`;
   - bind each encrypted column value with its own `?`, for example `SET ssn = ?` instead of `SET ssn = lower(?)` or `SET ssn = (SELECT ...)`;
   - do not write literals or expressions into encrypted columns;
   - use `INSERT` or `UPDATE` instead of `REPLACE` or `MERGE`, and plain `?` placeholders instead of numbered ones (`?1`).

   Only if you have verified that the statement never writes an unencrypted value to an encrypted column, set `encryptionFailOnUnmappedParameter=false`. The plugin then logs a WARNING and prepares the statement.
6. **`... is not supported for encrypted columns. Use setString, setNString, setBytes or setObject.`**: A stream or LOB setter (`setCharacterStream`, `setBinaryStream`, `setClob`, `setBlob`, and similar) was called for a parameter that maps to an encrypted column. Pass the value with one of the listed setters.

### Debugging

Enable audit logging to track encryption operations:

```java
props.setProperty("auditLoggingEnabled", "true");
```

Check the application logs for encryption-related messages.

## Limitations

- Currently supports string data types for encryption
- Requires metadata configuration for each encrypted column
- Performance impact mainly during data key operations, mitigated by caching

### Statements the plugin maps automatically

Automatic encryption works for `PreparedStatement` parameters in these statement shapes:

- `INSERT` with a column list: single-row and multi-row `VALUES`, `INSERT ... SELECT` (including `UNION`), and MySQL `INSERT ... SET`
- `UPDATE ... SET`, including row assignments such as `SET (name, ssn) = (?, ?)`
- PostgreSQL `INSERT ... ON CONFLICT ... DO UPDATE SET` and MySQL `INSERT ... ON DUPLICATE KEY UPDATE`

A value is encrypted only when it is bound by its own `?` placeholder directly to the encrypted column. A cast around the placeholder and parentheses are allowed, for example `CAST(? AS bytea)`, `?::bytea` or `(?)`. String literals and other expressions are never encrypted: in `SET ssn = '123-45-6789'` or `SET ssn = lower(?)` the plugin cannot encrypt the value. Literals and expressions in other columns of the same statement are fine and do not shift the parameter numbering.

### Statements rejected by default

On a table with encrypted columns, the plugin rejects the following statements with an `SQLException` when they are prepared, so that no value is written unencrypted (controlled by `encryptionFailOnUnmappedParameter`, default `true`):

- `INSERT` without a column list
- parameters inside an expression (function call, arithmetic, concatenation, `CASE`, sub-select) that is assigned to an encrypted column
- parameters whose target column cannot be determined, for example a sub-select or an unknown target table
- `REPLACE` and `MERGE` statements
- statements the SQL parser cannot read, for example PostgreSQL `E'...'` strings, MySQL backslash escapes inside string literals (`'it\'s'`), or the PostgreSQL JSONB `??` operator
- numbered placeholders such as `?1`
- statements where the number of parameters found by the parser differs from the number of placeholders the driver sends

Statements on tables without encrypted columns are never rejected. Versions before 4.6.0 did not reject these statements and could write values bound to encrypted columns unencrypted without an error or a log entry. See the [Troubleshooting](#troubleshooting) section for how to fix a rejected statement.

### Other limitations

- Stream and LOB setters (`setCharacterStream`, `setNCharacterStream`, `setAsciiStream`, `setBinaryStream`, `setUnicodeStream`, `setClob`, `setNClob`, `setBlob`) throw an `SQLException` when the parameter maps to an encrypted column. Use `setString`, `setNString`, `setBytes` or `setObject`.
- `CallableStatement` setters are not intercepted; values passed to stored procedures are not encrypted.
- Values that come from a CTE or from another column (`INSERT INTO users (ssn) SELECT ssn FROM staging`) are not encrypted by the plugin.
- Table names are matched against the metadata case-sensitively. Use the same spelling in statements as in `encryption_metadata`.

## Best Practices

1. **Use IAM Roles**: Use IAM roles instead of hardcoded credentials when possible
2. **Separate Keys**: Use different KMS keys for different environments
3. **Monitor Usage**: Monitor KMS usage and costs
4. **Test Performance**: Test the performance impact in your specific use case
5. **Backup Metadata**: Ensure the encryption metadata table is included in backups
6. **Key Rotation**: Implement a strategy for KMS key rotation

## Example Application

See [KmsEncryptionIntegrationTest.java](../../../wrapper/src/test/java/integration/container/tests/KmsEncryptionIntegrationTest.java) for a complete working example.
