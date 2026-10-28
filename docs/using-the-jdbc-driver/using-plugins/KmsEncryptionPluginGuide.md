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
- HMAC-based integrity verification (see [Two-Key Encryption Architecture](../../DatabaseEncryptionSetup.md#encrypted-data-format))
- Support for PostgreSQL and MySQL
- KMS key rotation, and data key rotation with re-encryption (see [Key Rotation](#key-rotation))

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

Automatic KMS key rotation is the recommended way to rotate keys. It is transparent to the plugin and needs no database changes. See [Key Rotation](#key-rotation).

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

The system uses a two-key architecture for enhanced security (see [Two-Key Encryption Architecture](../../DatabaseEncryptionSetup.md#encrypted-data-format) for details):
- **Data Encryption Key**: Encrypted by KMS, used for data encryption/decryption
- **HMAC Key**: Stored in plaintext, used for integrity verification

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

// Enable KMS encryption plugin. It requires the sqlParser plugin, which analyzes each SQL statement.
props.setProperty("wrapperPlugins", "sqlParser,kmsEncryption");

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
jdbc:aws-wrapper:postgresql://host:port/database?wrapperPlugins=sqlParser,kmsEncryption&kmsKeyArn=arn:aws:kms:...&kmsRegion=us-east-1
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

The plugin uses two levels of keys. A KMS key (master key) wraps each data key. Each encrypted column has one data key and one HMAC key, stored in `encrypt.key_storage`. Encrypted values do not record which data key encrypted them, so the plugin can only read a column whose values are all encrypted under the column's current data key.

### Rotating the KMS key (recommended)

Enable automatic rotation on the KMS key:

```bash
aws kms enable-key-rotation --key-id <KEY_ID>
```

KMS keeps the previous key material and uses it to decrypt data keys wrapped before the rotation. The rotation is transparent to the plugin and needs no database changes. It does not change the data keys themselves.

### Rotating a column's data key

Rotate a column's data key only when you need a new data key, for example after a suspected data key exposure or to move a column to a different KMS key. Use `KeyManagementUtility.rotateDataKeyAndReencrypt()`. It must run on a connection that does not use the `kmsEncryption` plugin: either a plain target driver connection, or an AWS Advanced JDBC Wrapper connection whose `wrapperPlugins` do not include `kmsEncryption`. The utility checks this and throws an error if the plugin is present.

```java
import software.amazon.jdbc.plugin.encryption.key.*;
import software.amazon.jdbc.plugin.encryption.metadata.*;
import software.amazon.jdbc.plugin.encryption.model.*;

EncryptionConfig config = EncryptionConfig.builder()
    .kmsRegion("us-east-1")
    .encryptionMetadataSchema("encrypt")
    .build();

KmsClient kmsClient = KmsClient.builder()
    .region(Region.US_EAST_1)
    .build();

// Connection without the kmsEncryption plugin (see the procedure below for a wrapper connection)
Connection connection = DriverManager.getConnection(
    "jdbc:postgresql://localhost:5432/mydb", "user", "password");

KeyManager keyManager = new KeyManager(kmsClient, connection, true, config); // true = PostgreSQL
MetadataManager metadataManager = new MetadataManager(connection, config);
KeyManagementUtility utility = new KeyManagementUtility(
    keyManager, metadataManager, connection, kmsClient, config);

// "id" is the primary key column of users; null keeps the current KMS key
DataKeyReencryptionResult result = utility.rotateDataKeyAndReencrypt(
    "users", "ssn", "id", null, KeyManagementUtility.DEFAULT_REENCRYPTION_BATCH_SIZE);
System.out.println(result); // new key id, old key id, re-encrypted and unmatched row counts
```

Pass a KMS key ARN instead of `null` to wrap the new data key under a different KMS key.

`rotateDataKeyAndReencrypt()` does the following:
1. Checks that every value of the column verifies against the current HMAC key. If any value does not, it stops with an error and changes nothing.
2. Creates a new data key and HMAC key and stores them in `encrypt.key_storage`. If the rotation fails before the switch in step 3 is committed, it deletes this new key row again; if that is not possible, the error message names the new key id.
3. Switches `encrypt.encryption_metadata.key_id` to the new key and commits.
4. Re-encrypts the rows in primary key order, `batchSize` rows per transaction. Values are decrypted with the old key and encrypted with the new key; the stored type of each value is kept.

It never deletes the old `key_storage` row and never changes the old KMS key.

Requirements:
- The primary key column must be a single, unique, `NOT NULL` column. Rows with a `NULL` value in it are not processed.
- Table and column names may contain only letters, digits and underscores, at most 64 characters. They are matched against `encrypt.encryption_metadata` as stored there, with the bare table name, the way the plugin stores it. On PostgreSQL the match is case-sensitive, and unquoted names are stored in lower case (`users`, not `Users`). The SQL against the data table uses the table and column spelling stored in the metadata.
- A table name may have one schema prefix (`app.users`). The prefix is used only in the SQL against the data table; the metadata lookup and the HMAC trigger cache use the bare table name (`users`).
- Before it creates a key, the utility checks that the table, the encrypted column and the primary key column exist. A wrong name fails with nothing changed.
- With the HMAC trigger, the utility sets the trigger's cached HMAC key for its own connection once, before the first batch: `hmac_key.<table>.<column>` with `set_config` on PostgreSQL, the user variable `@hmac_key_<table>_<column>` on MySQL.

> [!WARNING]
> `KeyManagementUtility.rotateDataKey()` does **not** re-encrypt existing rows. It only stores a new data key and points the column's metadata at it. After the call:
> - Every row encrypted under the previous data key fails to decrypt through the plugin with `HMAC verification failed`.
> - Running applications keep writing new rows under the previous key until their metadata cache refreshes (`metadataCacheRefreshIntervalMs`, 5 minutes by default).
> - The data is recoverable only while the previous `key_storage` row and its KMS key exist. Do not delete them.
>
> Use `rotateDataKey()` only for a column that holds no data. If you already called it on a column with data, see [Finishing or repairing a re-encryption](#finishing-or-repairing-a-re-encryption).

### Finishing or repairing a re-encryption

`KeyManagementUtility.reencryptRemainingRows(table, column, primaryKeyColumn, oldKeyIds, batchSize)` re-encrypts values that are still under one of the given old key ids into the column's current key. It uses the same batching and HMAC check as the rotation and is safe to run again: values already under the current key are skipped. Use it:
- To resume a failed rotation. Committed batches are kept; the exception message names the old key id to pass.
- After the rotation, once `metadataCacheRefreshIntervalMs` has passed, to convert rows that running applications wrote under the old key.
- To recover a column after `rotateDataKey()` was called on it. Find the previous key id:

```sql
-- Data keys created for the column, oldest first
SELECT id, name, created_at FROM encrypt.key_storage
WHERE name LIKE 'key-users-ssn-%' ORDER BY created_at;

-- The key the column uses now
SELECT key_id FROM encrypt.encryption_metadata
WHERE table_name = 'users' AND column_name = 'ssn';
```

```java
DataKeyReencryptionResult repair = utility.reencryptRemainingRows(
    "users", "ssn", "id", Collections.singletonList(previousKeyId),
    KeyManagementUtility.DEFAULT_REENCRYPTION_BATCH_SIZE);
if (!repair.isComplete()) {
    // Rows that match neither the current key nor the given old keys are left unchanged
    System.out.println(repair.getUnmatchedCount() + " unmatched rows, for example "
        + repair.getUnmatchedPrimaryKeys());
}
```

### Data key rotation procedure

1. **Plan a maintenance window.** Stop or pause the applications that write to the column if you can. While the batches run, reads through the plugin fail with `HMAC verification failed` for rows that are not converted yet.
2. **Back up** the table and the `encrypt` schema.
3. **Run `rotateDataKeyAndReencrypt()`** on a connection without the `kmsEncryption` plugin. Record the old and new key ids from the log or from `result.getSourceKeyIds()` and `result.getCurrentKeyId()`.
4. **If it fails**, fix the cause and run `reencryptRemainingRows()` with the old key id. Committed batches are kept.
5. **Wait at least `metadataCacheRefreshIntervalMs`** (default 300000 ms) or restart the applications, then run `reencryptRemainingRows()` again with the old key id. This converts rows that applications wrote under the old key before their metadata cache refreshed.
6. **With the PostgreSQL HMAC trigger**, each database session caches the column's HMAC key. The trigger function `validate_encrypted_data_hmac()` from this version reloads the key from the metadata when a value fails the check against the cached key, so pooled sessions pick up the new key on their own. If your `validate_encrypted_data_hmac()` was created by an earlier version, re-create it: run only its `CREATE OR REPLACE FUNCTION` statement from [`encrypted_data_type.sql`](../../../wrapper/src/test/resources/sql/encrypted_data_type.sql), with `SCHEMA_NAME` replaced by your metadata schema. Until then, recycle pooled connections (for example, restart the applications), because an old function rejects values under the new key until the session ends. Do not re-run `EncryptedDataTypeInstaller.installEncryptedDataType()` on a database with data: its script starts with `DROP DOMAIN IF EXISTS encrypted_data CASCADE`, which drops every column of that type.
7. **Keep the old `key_storage` row and its KMS key** until every backup and snapshot that holds data encrypted under the old key has expired.

The following example runs steps 3 and 4 for the `ssn` column of the `users` table, and step 5 as a separate, later job. It uses an AWS Advanced JDBC Wrapper connection with the `failover2` and `efm2` plugins and without `kmsEncryption`. If a failover happens during a batch, that batch is rolled back and the call fails; run `reencryptRemainingRows()` to resume.

```java
import java.sql.*;
import java.util.Collections;
import java.util.Properties;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.jdbc.plugin.encryption.key.*;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;

EncryptionConfig config = EncryptionConfig.builder()
    .kmsRegion("us-east-1")
    .encryptionMetadataSchema("encrypt")
    .build();
KmsClient kmsClient = KmsClient.builder().region(Region.US_EAST_1).build();

Properties props = new Properties();
props.setProperty("user", "user");
props.setProperty("password", "password");
props.setProperty("wrapperPlugins", "failover2,efm2"); // any plugins except kmsEncryption

try (Connection connection = DriverManager.getConnection(
    "jdbc:aws-wrapper:postgresql://my-cluster.cluster-xyz.us-east-1.rds.amazonaws.com:5432/mydb",
    props)) {

  KeyManager keyManager = new KeyManager(kmsClient, connection, true, config); // true = PostgreSQL
  MetadataManager metadataManager = new MetadataManager(connection, config);
  KeyManagementUtility utility = new KeyManagementUtility(
      keyManager, metadataManager, connection, kmsClient, config);
  int batchSize = KeyManagementUtility.DEFAULT_REENCRYPTION_BATCH_SIZE;

  // Record the key the column uses before the rotation. Steps 4 and 5 need it.
  int oldKeyId;
  try (PreparedStatement stmt = connection.prepareStatement(
      "SELECT key_id FROM encrypt.encryption_metadata WHERE table_name = ? AND column_name = ?")) {
    stmt.setString(1, "users");
    stmt.setString(2, "ssn");
    try (ResultSet rs = stmt.executeQuery()) {
      rs.next();
      oldKeyId = rs.getInt(1);
    }
  }

  // Step 3: switch to a new data key and re-encrypt the rows in batches
  try {
    DataKeyReencryptionResult result =
        utility.rotateDataKeyAndReencrypt("users", "ssn", "id", null, batchSize);
    System.out.println("Rotated from key " + oldKeyId + " to key " + result.getCurrentKeyId()
        + ": " + result);
  } catch (KeyManagementException e) {
    // Step 4: committed batches are kept. Fix the cause, then resume with
    // reencryptRemainingRows(..., Collections.singletonList(oldKeyId), ...), as in step 5.
    // If the column still uses oldKeyId (the error says the rotation was aborted before any
    // change, or the new key was not activated), run rotateDataKeyAndReencrypt() again instead.
    System.err.println(e.getMessage());
    throw e;
  }
  // Keep oldKeyId for step 5. It is also written to the log by rotateDataKeyAndReencrypt().
  System.out.println("Old key id for step 5: " + oldKeyId);
}
```

Step 5 is a separate job. Run it after `metadataCacheRefreshIntervalMs` has passed (5 minutes by default), or after restarting the applications, so that it converts the rows they wrote under the old key in the meantime. Running it right after step 3 misses those rows. Set up the connection, `config`, `kmsClient` and `utility` as in the first block, and pass the old key id recorded there:

```java
// Step 5, a separate job run after metadataCacheRefreshIntervalMs: convert late rows
int oldKeyId = 42; // the value recorded in step 3
DataKeyReencryptionResult repair = utility.reencryptRemainingRows(
    "users", "ssn", "id", Collections.singletonList(oldKeyId),
    KeyManagementUtility.DEFAULT_REENCRYPTION_BATCH_SIZE);
System.out.println("Converted " + repair.getReencryptedCount() + " late rows");
if (!repair.isComplete()) {
  // These rows match neither the current key nor the old key. They are left unchanged.
  System.err.println(repair.getUnmatchedCount() + " unmatched rows, for example "
      + repair.getUnmatchedPrimaryKeys());
}
```

## Advanced Features

### HMAC Verification (PostgreSQL)

The `encrypted_data` type includes HMAC verification to detect tampering. The system uses a two-key architecture where the HMAC key is separate from the encryption key, allowing database-level verification without exposing encrypted data. See [Two-Key Encryption Architecture](../../DatabaseEncryptionSetup.md#encrypted-data-format) for the security rationale.

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
- The column's data key was rotated without re-encrypting existing rows (for example with `rotateDataKey()`), or a re-encryption has not finished. See [Finishing or repairing a re-encryption](#finishing-or-repairing-a-re-encryption)
- With the PostgreSQL HMAC trigger, a pooled session may still cache the previous HMAC key after a rotation. The current trigger function reloads the key on a mismatch. With a function created by an earlier version, re-create the function (see step 6 of the [Data key rotation procedure](#data-key-rotation-procedure)) or recycle the connection

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
   - Enable automatic rotation on the KMS key (yearly by default). It is transparent to the plugin and needs no data changes
   - Rotate a column's data key only when required, with `rotateDataKeyAndReencrypt()` (see [Key Rotation](#key-rotation))
   - Use separate keys for different sensitivity levels
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
   - Rehearse data key rotation, including the repair pass and a restore from backup, in non-production first
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

## Additional Resources

- [AWS KMS Documentation](https://docs.aws.amazon.com/kms/)
- [Envelope Encryption](https://docs.aws.amazon.com/kms/latest/developerguide/concepts.html#enveloping)
- [AWS JDBC Wrapper Documentation](../../Documentation.md)
- [JSQLParser Documentation](https://github.com/JSQLParser/JSqlParser)
