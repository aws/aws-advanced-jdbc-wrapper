# Using the KMS Encryption Plugin

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
- **JSqlParser 4.5.x dependency** - Required for SQL parsing and analysis

### Creating AWS KMS Master Key

1. **Create a KMS Key** in AWS Console or using AWS CLI:
```bash
aws kms create-key --description "Database encryption master key" --key-usage ENCRYPT_DECRYPT
```

2. **Note the Key ARN** from the response - you'll need this for the `kms.MasterKeyArn` property.

3. **Set Key Permissions** - Ensure your application has the following KMS permissions:
   - `kms:Encrypt`
   - `kms:Decrypt` 
   - `kms:GenerateDataKey`
   - `kms:DescribeKey`

### Data Key Management

The plugin automatically manages data keys:
- **Data keys are generated** automatically using the master key when encrypting new data
- **Data keys are cached** in memory for performance (configurable via `dataKeyCache.*` properties)
- **Data keys are encrypted** with the master key and stored alongside encrypted data
- **No manual data key creation** is required

### Metadata Storage

Create a metadata table to store encryption configuration:

```sql
CREATE TABLE encryption_metadata (
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    key_arn VARCHAR(512) NOT NULL,
    algorithm VARCHAR(50) DEFAULT 'AES_256_GCM',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, column_name)
);
```

Insert encryption metadata for columns that should be encrypted:
```sql
INSERT INTO encryption_metadata (table_name, column_name, key_arn) 
VALUES ('users', 'ssn', 'arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012');
```

### Adding JSqlParser Dependency

The KMS Encryption Plugin requires JSqlParser 4.5.x for SQL statement analysis. Add this dependency to your project:

**Maven:**
```xml
<dependency>
    <groupId>com.github.jsqlparser</groupId>
    <artifactId>jsqlparser</artifactId>
    <version>4.5</version>
</dependency>
```

**Gradle:**
```gradle
implementation 'com.github.jsqlparser:jsqlparser:4.5'
```

## Configuration

### Connection Properties

| Property | Description | Required | Default |
|----------|-------------|----------|---------|
| `kms.region` | AWS KMS region for encryption operations | Yes | None |
| `kms.MasterKeyArn` | Master key ARN for encryption | Yes | None |
| `key.rotationDays` | Number of days for key rotation | No | `30` |
| `metadataCache.enabled` | Enable/disable metadata caching | No | `true` |
| `metadataCache.expirationMinutes` | Metadata cache expiration time in minutes | No | `60` |
| `metadataCache.refreshIntervalMs` | Metadata cache refresh interval in milliseconds | No | `300000` |
| `keyManagement.maxRetries` | Maximum number of retries for key management operations | No | `3` |
| `keyManagement.retryBackoffBaseMs` | Base backoff time in milliseconds for key management retries | No | `100` |
| `audit.loggingEnabled` | Enable/disable audit logging | No | `false` |
| `kms.connectionTimeoutMs` | KMS connection timeout in milliseconds | No | `5000` |
| `dataKeyCache.enabled` | Enable/disable data key caching | No | `true` |
| `dataKeyCache.maxSize` | Maximum size of data key cache | No | `1000` |
| `dataKeyCache.expirationMs` | Data key cache expiration in milliseconds | No | `3600000` |

### Example Connection String

```java
String url = "jdbc:aws-wrapper:postgresql://your-cluster.cluster-xyz.us-east-1.rds.amazonaws.com:5432/mydb";
Properties props = new Properties();
props.setProperty("user", "username");
props.setProperty("password", "password");
props.setProperty("wrapperPlugins", "kmsEncryption");
props.setProperty("kms.MasterKeyArn", "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012");
props.setProperty("kms.region", "us-east-1");
props.setProperty("audit.loggingEnabled", "true");

Connection conn = DriverManager.getConnection(url, props);
```

## Setup

### 1. Create Encryption Metadata Table

First, create a table to store encryption metadata:

```sql
CREATE TABLE encryption_metadata (
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    encryption_type VARCHAR(50) NOT NULL DEFAULT 'AES',
    PRIMARY KEY (table_name, column_name)
);
```

### 2. Configure Column Encryption

Define which columns should be encrypted by inserting metadata:

```sql
-- Configure encryption for sensitive columns in the customers table
INSERT INTO encryption_metadata (table_name, column_name, encryption_type)
VALUES 
    ('customers', 'ssn', 'AES'),
    ('customers', 'credit_card', 'AES'),
    ('customers', 'phone', 'AES'),
    ('customers', 'address', 'AES');
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

### Debugging

Enable audit logging to track encryption operations:

```java
props.setProperty("enableAuditLogging", "true");
```

Check the application logs for encryption-related messages.

## Limitations

- Currently supports string data types for encryption
- Requires metadata configuration for each encrypted column
- Performance impact mainly during data key operations, mitigated by caching
- Limited to INSERT and UPDATE operations for automatic encryption

## Best Practices

1. **Use IAM Roles**: Use IAM roles instead of hardcoded credentials when possible
2. **Separate Keys**: Use different KMS keys for different environments
3. **Monitor Usage**: Monitor KMS usage and costs
4. **Test Performance**: Test the performance impact in your specific use case
5. **Backup Metadata**: Ensure the encryption metadata table is included in backups
6. **Key Rotation**: Implement a strategy for KMS key rotation

## Example Application

See the [KmsEncryptionExample.java](../../../examples/AWSDriverExample/src/main/java/software/amazon/KmsEncryptionExample.java) for a complete working example.
