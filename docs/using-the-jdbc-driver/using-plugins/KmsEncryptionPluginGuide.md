# KMS Encryption Plugin - Complete Setup and Usage Guide

This guide provides comprehensive instructions for setting up and using the KMS Encryption Plugin with the AWS Advanced JDBC Wrapper.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Database Setup](#database-setup)
4. [KMS Key Setup](#kms-key-setup)
5. [Plugin Configuration](#plugin-configuration)
6. [Basic Usage](#basic-usage)
7. [Key Rotation](#key-rotation)
8. [Advanced Features](#advanced-features)
9. [Troubleshooting](#troubleshooting)

## Overview

The KMS Encryption Plugin provides transparent column-level encryption using AWS KMS (Key Management Service). It uses envelope encryption where:
- AWS KMS generates and encrypts data keys
- Data keys encrypt your actual data
- HMAC verification ensures data integrity
- Encryption/decryption happens automatically via JDBC

**Key Features:**
- Transparent encryption/decryption through PreparedStatement and ResultSet
- Automatic SQL parsing to detect encrypted columns
- Manual annotation support for explicit column specification
- HMAC-based integrity verification
- Support for PostgreSQL and MySQL
- Key rotation capabilities

## Prerequisites

### AWS Requirements
- AWS account with KMS access
- IAM permissions for KMS operations:
  - `kms:GenerateDataKey`
  - `kms:Decrypt`
  - `kms:CreateKey` (for key creation)
  - `kms:DescribeKey`

### Database Requirements
- **PostgreSQL**: Version 12+ with `pgcrypto` extension
- **MySQL**: Version 8.0+

### Java Requirements
- Java 8 or higher
- AWS SDK for Java 2.x

## Database Setup

### Step 1: Create Encryption Metadata Schema

The plugin requires a dedicated schema to store encryption metadata and keys.

**PostgreSQL:**
```sql
-- Create metadata schema
CREATE SCHEMA encrypt;

-- Install pgcrypto extension (required for HMAC functions)
CREATE EXTENSION IF NOT EXISTS pgcrypto;
```

**MySQL:**
```sql
-- Create metadata schema
CREATE SCHEMA encrypt;
```

### Step 2: Create Key Storage Table

This table stores encrypted data keys and HMAC keys.

**PostgreSQL:**
```sql
CREATE TABLE encrypt.key_storage (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    master_key_arn VARCHAR(512) NOT NULL,
    encrypted_data_key TEXT NOT NULL,
    hmac_key BYTEA NOT NULL,
    key_spec VARCHAR(50) DEFAULT 'AES_256',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
```

**MySQL:**
```sql
CREATE TABLE encrypt.key_storage (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    master_key_arn VARCHAR(512) NOT NULL,
    encrypted_data_key TEXT NOT NULL,
    hmac_key VARBINARY(32) NOT NULL,
    key_spec VARCHAR(50) DEFAULT 'AES_256',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Step 3: Create Encryption Metadata Table

This table maps columns to their encryption keys.

**PostgreSQL:**
```sql
CREATE TABLE encrypt.encryption_metadata (
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    encryption_algorithm VARCHAR(50) NOT NULL,
    key_id INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, column_name),
    FOREIGN KEY (key_id) REFERENCES encrypt.key_storage(id)
);
```

**MySQL:**
```sql
CREATE TABLE encrypt.encryption_metadata (
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    encryption_algorithm VARCHAR(50) NOT NULL,
    key_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, column_name),
    FOREIGN KEY (key_id) REFERENCES encrypt.key_storage(id)
);
```

### Step 4: Install Encrypted Data Type (PostgreSQL Only)

PostgreSQL supports a custom `encrypted_data` domain type with HMAC verification.

```java
import software.amazon.jdbc.plugin.encryption.schema.EncryptedDataTypeInstaller;

// Install the encrypted_data type and validation functions
EncryptedDataTypeInstaller.installEncryptedDataType(connection, "encrypt");
```

This creates:
- `encrypted_data` domain type (base type: BYTEA)
- `has_valid_hmac_structure()` function for structure validation
- `validate_encrypted_data_hmac()` trigger function for HMAC verification

### Step 5: Create Application Tables

**PostgreSQL (with encrypted_data type):**
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    ssn encrypted_data,  -- Custom type with HMAC validation
    email VARCHAR(100)
);

-- Add HMAC validation trigger
CREATE TRIGGER validate_ssn_hmac
    BEFORE INSERT OR UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION validate_encrypted_data_hmac('ssn');
```

**MySQL:**
```sql
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    ssn VARBINARY(256),  -- Must be VARBINARY to store encrypted data
    email VARCHAR(100)
);
```

## KMS Key Setup

### Step 1: Create KMS Master Key

**Using AWS Console:**
1. Navigate to AWS KMS Console
2. Click "Create key"
3. Select "Symmetric" key type
4. Choose "Encrypt and decrypt" usage
5. Add alias (e.g., `alias/jdbc-encryption-key`)
6. Configure key policy with required permissions
7. Note the Key ARN (e.g., `arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012`)

**Using AWS CLI:**
```bash
# Create master key
aws kms create-key \
    --description "JDBC Encryption Master Key" \
    --key-usage ENCRYPT_DECRYPT \
    --key-spec SYMMETRIC_DEFAULT

# Create alias
aws kms create-alias \
    --alias-name alias/jdbc-encryption-key \
    --target-key-id <KEY_ID>

# Enable automatic key rotation
aws kms enable-key-rotation --key-id <KEY_ID>
```

### Step 2: Generate Data Key

**Using Java SDK:**
```java
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.*;
import java.util.Base64;

KmsClient kmsClient = KmsClient.builder()
    .region(Region.US_EAST_1)
    .build();

// Generate data key
GenerateDataKeyRequest request = GenerateDataKeyRequest.builder()
    .keyId("arn:aws:kms:us-east-1:123456789012:key/...")
    .keySpec(DataKeySpec.AES_256)
    .build();

GenerateDataKeyResponse response = kmsClient.generateDataKey(request);

// Get encrypted data key (store this in database)
String encryptedDataKey = Base64.getEncoder()
    .encodeToString(response.ciphertextBlob().asByteArray());

// Plaintext key is used by plugin (not stored)
byte[] plaintextKey = response.plaintext().asByteArray();
```

### Step 3: Generate HMAC Key

```java
import java.security.SecureRandom;

// Generate 256-bit HMAC key
byte[] hmacKey = new byte[32];
new SecureRandom().nextBytes(hmacKey);
```

### Step 4: Store Keys in Database

**PostgreSQL:**
```sql
INSERT INTO encrypt.key_storage 
    (name, master_key_arn, encrypted_data_key, hmac_key, key_spec)
VALUES 
    ('users-ssn-key', 
     'arn:aws:kms:us-east-1:123456789012:key/...', 
     '<base64-encoded-encrypted-key>',
     '<binary-hmac-key>',
     'AES_256')
RETURNING id;
```

**MySQL:**
```sql
INSERT INTO encrypt.key_storage 
    (name, master_key_arn, encrypted_data_key, hmac_key, key_spec)
VALUES 
    ('users-ssn-key', 
     'arn:aws:kms:us-east-1:123456789012:key/...', 
     '<base64-encoded-encrypted-key>',
     '<binary-hmac-key>',
     'AES_256');

SELECT LAST_INSERT_ID();
```

### Step 5: Register Column Encryption

```sql
INSERT INTO encrypt.encryption_metadata 
    (table_name, column_name, encryption_algorithm, key_id)
VALUES 
    ('users', 'ssn', 'AES-256-GCM', <key_id_from_step_4>);
```

## Plugin Configuration

### Connection Properties

```java
Properties props = new Properties();

// Enable KMS encryption plugin
props.setProperty("wrapperPlugins", "kmsEncryption");

// Configure KMS master key
props.setProperty("kmsKeyArn", "arn:aws:kms:us-east-1:123456789012:key/...");
props.setProperty("kmsRegion", "us-east-1");

// Optional: Configure metadata schema (default: "encrypt")
props.setProperty("encryptionMetadataSchema", "encrypt");

// Optional: Configure data key cache (default: 300 seconds)
props.setProperty("dataKeyCacheTTL", "300");

// Get connection
String url = "jdbc:aws-wrapper:postgresql://localhost:5432/mydb";
Connection conn = DriverManager.getConnection(url, props);
```

### Connection URL Format

```
jdbc:aws-wrapper:postgresql://host:port/database?wrapperPlugins=kmsEncryption&kmsKeyArn=arn:aws:kms:...&kmsRegion=us-east-1
```

### AWS Credentials

The plugin uses the default AWS credential provider chain:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. System properties
3. AWS credentials file (`~/.aws/credentials`)
4. IAM instance profile (for EC2)
5. IAM role (for ECS/Lambda)

## Basic Usage

### Automatic Encryption (via SQL Parsing)

The plugin automatically detects encrypted columns by parsing SQL statements.

```java
// INSERT - automatic encryption
String sql = "INSERT INTO users (name, ssn, email) VALUES (?, ?, ?)";
try (PreparedStatement stmt = conn.prepareStatement(sql)) {
    stmt.setString(1, "Alice");
    stmt.setString(2, "123-45-6789");  // Automatically encrypted
    stmt.setString(3, "alice@example.com");
    stmt.executeUpdate();
}

// SELECT - automatic decryption
String sql = "SELECT name, ssn, email FROM users WHERE name = ?";
try (PreparedStatement stmt = conn.prepareStatement(sql)) {
    stmt.setString(1, "Alice");
    ResultSet rs = stmt.executeQuery();
    if (rs.next()) {
        String ssn = rs.getString("ssn");  // Automatically decrypted
        System.out.println("SSN: " + ssn);  // Prints: 123-45-6789
    }
}

// UPDATE - automatic encryption
String sql = "UPDATE users SET ssn = ? WHERE name = ?";
try (PreparedStatement stmt = conn.prepareStatement(sql)) {
    stmt.setString(1, "987-65-4321");  // Automatically encrypted
    stmt.setString(2, "Alice");
    stmt.executeUpdate();
}
```

### Manual Encryption (via Annotations)

Use SQL comments to explicitly mark encrypted columns when automatic parsing fails.

```java
// INSERT with annotation
String sql = "INSERT INTO users (name, ssn, email) VALUES (?, /*@encrypt:users.ssn*/ ?, ?)";
try (PreparedStatement stmt = conn.prepareStatement(sql)) {
    stmt.setString(1, "Bob");
    stmt.setString(2, "111-22-3333");  // Encrypted via annotation
    stmt.setString(3, "bob@example.com");
    stmt.executeUpdate();
}

// UPDATE with annotation
String sql = "UPDATE users SET ssn = /*@encrypt:users.ssn*/ ? WHERE name = ?";
try (PreparedStatement stmt = conn.prepareStatement(sql)) {
    stmt.setString(1, "444-55-6666");  // Encrypted via annotation
    stmt.setString(2, "Bob");
    stmt.executeUpdate();
}

// Annotation format: /*@encrypt:table_name.column_name*/
```

### Verifying Encryption

To verify data is actually encrypted in storage, connect without the plugin:

```java
// Connection WITHOUT encryption plugin
Properties plainProps = new Properties();
plainProps.setProperty("user", "postgres");
plainProps.setProperty("password", "password");

String plainUrl = "jdbc:postgresql://localhost:5432/mydb";
Connection plainConn = DriverManager.getConnection(plainUrl, plainProps);

// Query encrypted column
String sql = "SELECT name, ssn FROM users WHERE name = ?";
try (PreparedStatement stmt = plainConn.prepareStatement(sql)) {
    stmt.setString(1, "Alice");
    ResultSet rs = stmt.executeQuery();
    if (rs.next()) {
        byte[] encryptedSsn = rs.getBytes("ssn");
        // This will be encrypted binary data, not plaintext
        System.out.println("Encrypted (base64): " + 
            Base64.getEncoder().encodeToString(encryptedSsn));
    }
}
```

## Key Rotation

### Using KeyManagementUtility

The `KeyManagementUtility` class provides methods for key rotation.

```java
import software.amazon.jdbc.plugin.encryption.key.*;
import software.amazon.jdbc.plugin.encryption.metadata.*;
import software.amazon.jdbc.plugin.encryption.model.*;

// Create utility components
EncryptionConfig config = EncryptionConfig.builder()
    .kmsRegion("us-east-1")
    .defaultMasterKeyArn("arn:aws:kms:...")
    .encryptionMetadataSchema("encrypt")
    .build();

KmsClient kmsClient = KmsClient.builder()
    .region(Region.US_EAST_1)
    .build();

KeyManager keyManager = new KeyManager(kmsClient, connection, true, config);
MetadataManager metadataManager = new MetadataManager(connection, config);

KeyManagementUtility utility = new KeyManagementUtility(
    keyManager, metadataManager, connection, kmsClient, config);

// Rotate key (uses same master key)
utility.rotateDataKey("users", "ssn", null);

// Rotate key with new master key
utility.rotateDataKey("users", "ssn", "arn:aws:kms:us-east-1:123456789012:key/new-key");
```

### Manual Key Rotation Steps

1. **Generate new data key:**
```java
GenerateDataKeyRequest request = GenerateDataKeyRequest.builder()
    .keyId(masterKeyArn)
    .keySpec(DataKeySpec.AES_256)
    .build();
GenerateDataKeyResponse response = kmsClient.generateDataKey(request);
```

2. **Generate new HMAC key:**
```java
byte[] newHmacKey = new byte[32];
new SecureRandom().nextBytes(newHmacKey);
```

3. **Store new key:**
```sql
INSERT INTO encrypt.key_storage 
    (name, master_key_arn, encrypted_data_key, hmac_key, key_spec)
VALUES 
    ('users-ssn-key-v2', 
     'arn:aws:kms:...',
     '<new-encrypted-key>',
     '<new-hmac-key>',
     'AES_256')
RETURNING id;
```

4. **Update metadata:**
```sql
UPDATE encrypt.encryption_metadata 
SET key_id = <new_key_id>, 
    updated_at = CURRENT_TIMESTAMP
WHERE table_name = 'users' AND column_name = 'ssn';
```

5. **Re-encrypt existing data:**
```java
// Read with old key, write with new key
String selectSql = "SELECT id, ssn FROM users";
String updateSql = "UPDATE users SET ssn = ? WHERE id = ?";

try (PreparedStatement selectStmt = conn.prepareStatement(selectSql);
     PreparedStatement updateStmt = conn.prepareStatement(updateSql)) {
    
    ResultSet rs = selectStmt.executeQuery();
    while (rs.next()) {
        int id = rs.getInt("id");
        String ssn = rs.getString("ssn");  // Decrypted with old key
        
        updateStmt.setString(1, ssn);  // Re-encrypted with new key
        updateStmt.setInt(2, id);
        updateStmt.executeUpdate();
    }
}
```

## Advanced Features

### HMAC Verification (PostgreSQL)

The `encrypted_data` type includes HMAC verification to detect tampering.

```sql
-- Check HMAC structure
SELECT name, has_valid_hmac_structure(ssn) as valid_structure 
FROM users;

-- The trigger automatically validates HMAC on INSERT/UPDATE
-- Invalid HMAC will raise an error
```

### Data Key Caching

The plugin caches decrypted data keys to reduce KMS API calls.

```java
// Configure cache TTL (default: 300 seconds)
props.setProperty("dataKeyCacheTTL", "600");  // 10 minutes

// Cache is automatically managed
// Keys are evicted after TTL expires
```

### Multiple Encrypted Columns

```sql
-- Register multiple columns
INSERT INTO encrypt.encryption_metadata 
    (table_name, column_name, encryption_algorithm, key_id)
VALUES 
    ('users', 'ssn', 'AES-256-GCM', 1),
    ('users', 'credit_card', 'AES-256-GCM', 2),
    ('medical_records', 'diagnosis', 'AES-256-GCM', 3);
```

```java
// All encrypted columns are handled automatically
String sql = "INSERT INTO users (name, ssn, credit_card) VALUES (?, ?, ?)";
try (PreparedStatement stmt = conn.prepareStatement(sql)) {
    stmt.setString(1, "Alice");
    stmt.setString(2, "123-45-6789");      // Encrypted
    stmt.setString(3, "4111-1111-1111-1111");  // Encrypted
    stmt.executeUpdate();
}
```

### Complex Queries

The plugin handles complex SQL including:
- JOINs
- Subqueries
- CTEs (Common Table Expressions)
- WHERE clauses with encrypted columns

```java
// JOIN with encrypted columns
String sql = """
    SELECT u.name, u.ssn, o.order_id
    FROM users u
    JOIN orders o ON u.id = o.user_id
    WHERE u.ssn = ?
    """;

try (PreparedStatement stmt = conn.prepareStatement(sql)) {
    stmt.setString(1, "123-45-6789");  // Encrypted for comparison
    ResultSet rs = stmt.executeQuery();
    // Results are automatically decrypted
}
```

## Troubleshooting

### Common Issues

**1. "No encryption configuration found for column"**
- Verify metadata exists in `encrypt.encryption_metadata`
- Check table and column names match exactly (case-sensitive)
- Ensure key_id references valid key in `encrypt.key_storage`

**2. "KMS decrypt failed"**
- Verify IAM permissions include `kms:Decrypt`
- Check KMS key ARN is correct
- Ensure AWS credentials are configured
- Verify key is in "Enabled" state

**3. "HMAC verification failed" (PostgreSQL)**
- Data may have been tampered with
- HMAC key may have changed
- Check `encrypt.key_storage.hmac_key` matches key used for encryption

**4. "Cannot insert plain text into encrypted column"**
- This is expected behavior with PostgreSQL triggers
- Use the plugin connection for all operations
- Or use annotation: `/*@encrypt:table.column*/`

### Debugging

Enable detailed logging:

```java
// Set log level
System.setProperty("java.util.logging.config.file", "logging.properties");

// logging.properties:
// software.amazon.jdbc.plugin.encryption.level=FINEST
```

Check metadata:

```sql
-- Verify encryption metadata
SELECT * FROM encrypt.encryption_metadata 
WHERE table_name = 'users';

-- Verify key storage
SELECT id, name, master_key_arn, key_spec, created_at 
FROM encrypt.key_storage;

-- Check key relationships
SELECT em.table_name, em.column_name, ks.name as key_name, ks.master_key_arn
FROM encrypt.encryption_metadata em
JOIN encrypt.key_storage ks ON em.key_id = ks.id;
```

### Performance Considerations

- **Data key caching**: Reduces KMS API calls (default: 5 minutes)
- **Connection pooling**: Reuse connections to avoid repeated initialization
- **Batch operations**: Use batch inserts/updates when possible
- **Index encrypted columns**: Not recommended (encrypted data is random)
- **Query encrypted columns**: WHERE clauses on encrypted columns require full table scan

## Best Practices

1. **Key Management**
   - Rotate keys regularly (e.g., annually)
   - Use separate keys for different sensitivity levels
   - Enable automatic KMS key rotation
   - Monitor key usage via CloudTrail

2. **Security**
   - Use IAM roles instead of access keys
   - Apply least-privilege IAM policies
   - Enable CloudTrail logging for KMS operations
   - Regularly audit encryption metadata

3. **Performance**
   - Use connection pooling
   - Configure appropriate cache TTL
   - Minimize encrypted columns (only sensitive data)
   - Consider read replicas for read-heavy workloads

4. **Operations**
   - Test key rotation in non-production first
   - Backup encryption metadata regularly
   - Document which columns are encrypted
   - Monitor KMS API usage and costs

## Example: Complete Setup

See the complete working example in:
`wrapper/src/test/java/integration/container/tests/KmsEncryptionIntegrationTest.java`

This test demonstrates:
- Database schema setup
- KMS key generation
- Metadata configuration
- Basic encryption/decryption
- Update operations
- HMAC verification
- Annotation usage
- Key rotation

## Additional Resources

- [AWS KMS Documentation](https://docs.aws.amazon.com/kms/)
- [Envelope Encryption](https://docs.aws.amazon.com/kms/latest/developerguide/concepts.html#enveloping)
- [AWS JDBC Wrapper Documentation](../../Documentation.md)
- [JSQLParser Documentation](https://github.com/JSQLParser/JSqlParser)
