# Using the KMS Encryption Plugin

## Plugin Availability
The plugin is available since version 4.0.0.

The KMS Encryption Plugin provides transparent client-side, column-level encryption using AWS Key Management Service (KMS). It encrypts values bound to encrypted columns before they are sent to the database and decrypts them when they are read, based on metadata stored in the database.

> [!WARNING]\
> **A database-side HMAC validation trigger is required on every encrypted column.** On its own, the plugin cannot guarantee that an encrypted column never holds a plaintext value: it only encrypts `PreparedStatement` parameters sent over a connection with the plugin enabled, and it maps parameters to columns by best-effort SQL parsing. The trigger, not the SQL parser, is what guarantees an encrypted column never holds plaintext. See [Add HMAC validation triggers](#7-add-hmac-validation-triggers).

## Contents

- [Features](#features)
- [How Encryption Works](#how-encryption-works)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Configuration](#configuration)
- [Usage](#usage)
- [SQL Annotation Syntax](#sql-annotation-syntax)
- [Limitations](#limitations)
- [Key Rotation](#key-rotation)
- [Security Considerations](#security-considerations)
- [Performance Considerations](#performance-considerations)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)
- [Database Differences Summary](#database-differences-summary)
- [Example Application](#example-application)
- [Additional Resources](#additional-resources)

## Features

- **Transparent Encryption**: Encrypts `PreparedStatement` parameters and decrypts `ResultSet` values
- **AWS KMS Integration**: Uses envelope encryption with data keys wrapped by an AWS KMS key
- **Metadata-Driven**: The encrypted columns and their keys are recorded in metadata tables in the database
- **Annotations**: SQL comment annotations can mark a parameter for encryption explicitly
- **Integrity Verification**: Every value carries an HMAC that the driver, and the database triggers, verify
- **PostgreSQL and MySQL**: Supports PostgreSQL and MySQL, including Aurora and RDS
- **Key Rotation**: KMS key rotation, and data key rotation with re-encryption (see [Key Rotation](#key-rotation))
- **Audit Logging**: Optional audit logging for encryption operations

## How Encryption Works

The plugin uses envelope encryption:
- An AWS KMS key (the master key) wraps each data key. Only the KMS-encrypted copy of a data key is stored in the database.
- The data key encrypts your data with AES-256-GCM, on the client.
- An HMAC tag protects the integrity of every encrypted value.

### Two-Key Architecture

Each encrypted column has two keys, stored together in a row of `key_storage`:

1. **Data Encryption Key (DEK)** - Used for encrypting and decrypting the data. Stored encrypted by AWS KMS; only decrypted on the client side when needed.
2. **HMAC Verification Key** - Used for verifying data integrity without decryption. Stored in plaintext in the metadata table so database triggers can validate data without accessing the encryption key.

Even if the HMAC key is compromised, the encrypted data remains secure.

### Encrypted Data Format

```
[HMAC:32 bytes][Type:1 byte][IV:12 bytes][Ciphertext:variable]
```

- **HMAC (32 bytes)**: HMAC-SHA256 tag computed over `[Type][IV][Ciphertext]` using the HMAC key
- **Type (1 byte)**: Data type indicator (0x01 = String, 0x02 = Integer, etc.)
- **IV (12 bytes)**: Initialization vector for AES-GCM, random for every value
- **Ciphertext**: AES-256-GCM encrypted data

Minimum length: 61 bytes (32 + 1 + 12 + 16-byte GCM auth tag, for an empty plaintext). The driver rejects shorter values, and the HMAC trigger functions on both PostgreSQL and MySQL reject values shorter than 61 bytes as well, and so do the PostgreSQL `encrypted_data` domain and the `has_valid_hmac_structure()` helper. Earlier versions of the PostgreSQL script used a 45-byte minimum for the domain and the helper; running the current script upgrades both (see [step 3 of Setup](#3-install-the-encrypted-data-type-and-hmac-functions)).

Because the IV is random, encrypting the same value twice gives different bytes. See [Limitations](#other-limitations) for what this means for queries.

## Prerequisites

### AWS Requirements

- An AWS KMS symmetric encryption key (see [Create the KMS master key](#1-create-the-kms-master-key))
- AWS credentials configured (via IAM roles, profiles, or environment variables)
- IAM permissions on the KMS key:
  - The plugin: `kms:Decrypt`. It decrypts each column's stored data key and encrypts and decrypts values locally
  - `KeyManagementUtility`: `kms:GenerateDataKey` and `kms:Decrypt`, plus `kms:DescribeKey` for `validateMasterKey()`, and `kms:CreateKey` and `kms:CreateAlias` for `createMasterKeyWithPermissions()`

### Database Requirements

- **PostgreSQL**: Version 12+ (Aurora PostgreSQL or RDS PostgreSQL) with the `pgcrypto` extension available
- **MySQL**: Version 8.0+ (Aurora MySQL or RDS MySQL)

### Java Requirements

- Java 8 or higher
- AWS SDK for Java 2.x (KMS)
- **JSqlParser 4.9 dependency** - Required for SQL parsing and analysis
- **The `sqlParser` plugin** - Must be loaded together with `kmsEncryption` (for example `wrapperPlugins=sqlParser,kmsEncryption`). It parses each statement and maps its parameters to columns; without it, `kmsEncryption` refuses to prepare statements and to run SQL passed to `execute`, `executeQuery`, `executeUpdate` or `addBatch`

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

## Setup

The examples use the default metadata schema `encrypt`. To use a different schema, set the `encryptionMetadataSchema` connection property and replace `encrypt` throughout.

### 1. Create the KMS Master Key

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

Note the key ARN. The plugin's `encryptionAllowedMasterKeyArns` property must list it in the same form (ARN, alias, or key id) that you use to set up the columns in step 5. Automatic KMS key rotation is the recommended way to rotate keys. It is transparent to the plugin and needs no database changes. See [Key Rotation](#key-rotation).

### 2. Create the Metadata Schema

Create a dedicated schema to store encryption metadata:

```sql
CREATE SCHEMA encrypt;
```

**PostgreSQL only** - install `pgcrypto`, which the HMAC verification functions require. Install it into `public` so the HMAC trigger can call it as `public.hmac` under its locked `search_path`:

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;
```

MySQL does not require any extensions. The HMAC validation triggers (step 7) use a pure SQL HMAC-SHA256 implementation from the script [`encrypted_data_type_mysql.sql`](../../../wrapper/src/test/resources/sql/encrypted_data_type_mysql.sql).

### 3. Install the Encrypted Data Type and HMAC Functions

The database objects are defined in SQL scripts in the driver's source repository. The released driver jar does not contain an installer for them. Take the scripts from the release tag that matches your driver version:

- PostgreSQL: [`wrapper/src/test/resources/sql/encrypted_data_type.sql`](../../../wrapper/src/test/resources/sql/encrypted_data_type.sql)
- MySQL: [`wrapper/src/test/resources/sql/encrypted_data_type_mysql.sql`](../../../wrapper/src/test/resources/sql/encrypted_data_type_mysql.sql)

Replace every `SCHEMA_NAME` in the script with your metadata schema (default `encrypt`), then run it against the application database.

**PostgreSQL** creates the `encrypted_data` DOMAIN type (a `BYTEA` with a minimum length check), the `verify_encrypted_data_hmac()` and `has_valid_hmac_structure()` functions and the `validate_encrypted_data_hmac()` trigger function. Run it in the schema where your application tables live, so that `encrypted_data` resolves in step 6:

```bash
sed 's/SCHEMA_NAME/encrypt/g' encrypted_data_type.sql | psql -h host -U dbuser -d database -v ON_ERROR_STOP=1
```

The PostgreSQL script is safe to run again on a database that already holds encrypted data. It creates the `encrypted_data` domain only when it does not exist yet, and replaces the functions with `CREATE OR REPLACE`. On a database set up with an earlier version of the script, it replaces the domain's 45-byte length check with the 61-byte one. The new check is added `NOT VALID`, so existing values are not re-checked; new values are. The driver never writes a value shorter than 61 bytes, so you can then check the existing values too with `ALTER DOMAIN encrypted_data VALIDATE CONSTRAINT encrypted_data_check`, which fails if any column of the type holds a shorter value.

> [!WARNING]\
> Scripts from earlier driver versions start with `DROP DOMAIN IF EXISTS encrypted_data CASCADE`, which drops every column of type `encrypted_data` and the data in it. Do not re-run an earlier version of the script on a database with data.

**MySQL** creates the HMAC functions and the `validate_encrypted_data_hmac_before_insert` and `validate_encrypted_data_hmac_before_update` procedures used by the triggers in step 7. The procedures run from triggers, so they cannot use dynamic SQL; this is why the metadata schema is written into the script. The script uses `DELIMITER`, so run it with the `mysql` command-line client rather than through JDBC, in the database that holds your tables:

```bash
sed 's/SCHEMA_NAME/encrypt/g' encrypted_data_type_mysql.sql | mysql -h host -u dbuser -p database
```

With binary logging enabled, creating the routines and triggers needs the `SUPER` privilege or `log_bin_trust_function_creators=1`.

MySQL does not support DOMAINs. Encrypted columns use `VARBINARY(256)` instead (see step 6), so MySQL has no type-level check that a column holds encrypted data.

### 4. Create the Metadata Tables

`key_storage` holds each column's KMS-encrypted data key and its HMAC key. `encryption_metadata` maps each encrypted column to its row in `key_storage`. Create `key_storage` first, because `encryption_metadata` references it.

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

`encryption_metadata.key_id` is the integer `id` of the column's row in `key_storage`. `table_name` is the bare table name, without a schema prefix.

### 5. Configure Column Encryption

For a complete program, see [KeyManagementExample.java](../../../examples/EncryptionExample/src/main/java/software/amazon/KeyManagementExample.java).

**Recommended: use `KeyManagementUtility`.** `initializeEncryptionForColumn()` generates a data key under the given KMS key and a random HMAC key, stores them in `key_storage`, and registers the column in `encryption_metadata`:

```java
import java.sql.Connection;
import java.sql.DriverManager;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.jdbc.plugin.encryption.key.KeyManagementUtility;
import software.amazon.jdbc.plugin.encryption.key.KeyManager;
import software.amazon.jdbc.plugin.encryption.metadata.MetadataManager;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;

String masterKeyArn = "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012";

EncryptionConfig config = EncryptionConfig.builder()
    .kmsRegion("us-east-1")
    .encryptionMetadataSchema("encrypt")
    .allowedMasterKeyArns(masterKeyArn)
    .build();
KmsClient kmsClient = KmsClient.builder().region(Region.US_EAST_1).build();

try (Connection connection = DriverManager.getConnection(
    "jdbc:postgresql://my-cluster.cluster-xyz.us-east-1.rds.amazonaws.com:5432/mydb", "user", "password")) {

  KeyManager keyManager = new KeyManager(kmsClient, connection, true, config); // true = PostgreSQL
  MetadataManager metadataManager = new MetadataManager(connection, config);
  KeyManagementUtility utility = new KeyManagementUtility(
      keyManager, metadataManager, connection, kmsClient, config);

  // Uses AES-256-GCM.
  utility.initializeEncryptionForColumn("users", "ssn", masterKeyArn);
  utility.initializeEncryptionForColumn("users", "credit_card", masterKeyArn);
}
```

`KeyManagementUtility` also has a constructor that takes a `DataSource` instead of a `Connection`. The KMS key you pass is stored in `key_storage.master_key_arn` as given, and the plugin's `encryptionAllowedMasterKeyArns` must list it in exactly that form.

An `EncryptionConfig` built with `EncryptionConfig.builder()` requires `kmsRegion`. Its defaults differ from the connection property defaults in two places: `maxRetries` is 5 (the `keyManagementMaxRetries` property defaults to 3) and the data key cache expiration is 30 minutes (the `dataKeyCacheExpirationMs` property defaults to 1 hour).

**Alternative: create the keys yourself.** Generate a data key from KMS and a separate random HMAC key, store both in `key_storage`, then register the column with the returned `id`:

```java
import java.security.SecureRandom;
import java.sql.*;
import java.util.Base64;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.*;

// Generate data key from KMS. Only the encrypted copy is stored.
KmsClient kmsClient = KmsClient.builder().region(Region.US_EAST_1).build();
GenerateDataKeyResponse response = kmsClient.generateDataKey(
    GenerateDataKeyRequest.builder()
        .keyId(masterKeyArn)
        .keySpec(DataKeySpec.AES_256)
        .build()
);

String encryptedDataKey = Base64.getEncoder()
    .encodeToString(response.ciphertextBlob().asByteArray());

// Generate separate 256-bit HMAC key
byte[] hmacKey = new byte[32];
new SecureRandom().nextBytes(hmacKey);

int keyId;
try (PreparedStatement stmt = conn.prepareStatement(
    "INSERT INTO encrypt.key_storage "
        + "(name, master_key_arn, encrypted_data_key, hmac_key, key_spec) "
        + "VALUES (?, ?, ?, ?, ?)",
    Statement.RETURN_GENERATED_KEYS)) {
  stmt.setString(1, "key-users-ssn-1");
  stmt.setString(2, masterKeyArn);
  stmt.setString(3, encryptedDataKey);
  stmt.setBytes(4, hmacKey);
  stmt.setString(5, "AES_256");
  stmt.executeUpdate();
  try (ResultSet rs = stmt.getGeneratedKeys()) {
    rs.next();
    keyId = rs.getInt(1);
  }
}

// Register the encrypted column
try (PreparedStatement stmt = conn.prepareStatement(
    "INSERT INTO encrypt.encryption_metadata "
        + "(table_name, column_name, encryption_algorithm, key_id) VALUES (?, ?, ?, ?)")) {
  stmt.setString(1, "users");
  stmt.setString(2, "ssn");
  stmt.setString(3, "AES-256-GCM");
  stmt.setInt(4, keyId);
  stmt.executeUpdate();
}
```

`KeyManagementUtility` names its keys `key-<table>-<column>-<timestamp>`; the example follows that pattern so the queries in [Finishing or repairing a re-encryption](#finishing-or-repairing-a-re-encryption) find the key.

### 6. Create Application Tables

**PostgreSQL** - use the `encrypted_data` custom type:
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    ssn encrypted_data,
    credit_card encrypted_data,
    email VARCHAR(100)
);
```

**MySQL** - use `VARBINARY` for encrypted columns:
```sql
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    ssn VARBINARY(256),
    credit_card VARBINARY(256),
    email VARCHAR(100)
);
```

### 7. Add HMAC Validation Triggers

A database-side HMAC validation trigger is required on every encrypted column. The trigger checks on every insert and update that the value written to the column carries a valid HMAC for the column's key, and rejects plaintext and tampered values. The plugin alone cannot guarantee that an encrypted column never holds plaintext: it does not encrypt a value written by a client that does not use the plugin, through a plain `Statement` or a `CallableStatement`, or by a statement the plugin was configured not to reject (`encryptionFailOnUnmappedParameter=false`). It also finds encrypted columns by best-effort SQL parsing, so a value it does not map to an encrypted column, such as a value copied from another column (see [Limitations](#limitations)), is not encrypted. The trigger is what provides that guarantee. The driver also verifies the HMAC when it reads and decrypts a value, but only the trigger stops an unencrypted value from being stored.

On PostgreSQL the `encrypted_data` domain only checks the minimum length. On MySQL there is no domain-level type check at all, so the trigger is the only database-side guard.

**PostgreSQL:**
```sql
CREATE TRIGGER validate_ssn_hmac
    BEFORE INSERT OR UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION validate_encrypted_data_hmac('ssn');
```

**MySQL:** install the routines first (step 3). Then create one trigger per event and column:
```sql
CREATE TRIGGER users_ssn_hmac_ins BEFORE INSERT ON users FOR EACH ROW
  CALL validate_encrypted_data_hmac_before_insert('users', 'ssn', NEW.ssn);
CREATE TRIGGER users_ssn_hmac_upd BEFORE UPDATE ON users FOR EACH ROW
  CALL validate_encrypted_data_hmac_before_update('users', 'ssn', NEW.ssn, OLD.ssn);
```

The INSERT trigger calls `validate_encrypted_data_hmac_before_insert`; the UPDATE trigger calls `validate_encrypted_data_hmac_before_update`, which skips a value the UPDATE leaves unchanged.

The two procedures take different arguments than the single `validate_encrypted_data_hmac` procedure of an earlier version. Re-run the script (it drops the old procedure) and drop and re-create the triggers as shown above. Until the triggers are re-created, they still call the dropped procedure, so every INSERT and UPDATE on those tables fails. Run both steps together, in a maintenance window if the tables take live writes. To check which version is installed, query the routine comments; both procedures of this version return `aws-advanced-jdbc-wrapper kms_encryption trigger v1`, and an earlier install returns an empty string:

```sql
SELECT ROUTINE_NAME, ROUTINE_COMMENT FROM information_schema.ROUTINES
 WHERE ROUTINE_SCHEMA = DATABASE()
   AND ROUTINE_NAME IN ('validate_encrypted_data_hmac_before_insert',
                        'validate_encrypted_data_hmac_before_update');
```

| Feature | PostgreSQL | MySQL |
|---------|------------|-------|
| Native HMAC | `public.hmac()` via pgcrypto | Pure SQL implementation |
| Trigger Syntax | Single trigger for INSERT/UPDATE | Separate INSERT and UPDATE triggers |

On both databases the trigger looks up the column's HMAC key from the metadata for every row and caches nothing, so a data key rotation takes effect immediately with no reload logic or connection recycling. An UPDATE that leaves the encrypted value unchanged is not re-checked, so rows written under a previous key keep working while a rotation is in progress. See the [Data key rotation procedure](#data-key-rotation-procedure).

To check the structure of stored values on PostgreSQL:

```sql
SELECT name, has_valid_hmac_structure(ssn) AS valid_structure
FROM users;
```

## Configuration

### Connection Properties

| Property | Available Since Version | Description | Required | Default |
|----------|-------------------------|-------------|----------|---------|
| `kms.region` | 4.0.0 | AWS KMS region for encryption operations | No | `us-east-1` |
| `encryptionAllowedMasterKeyArns` | 4.7.0 | Comma-separated list of the KMS master keys the plugin may use. The plugin refuses to start without it. Each entry must match a `key_storage.master_key_arn` value exactly (the same ARN, alias, or key id that was used to set up the column). A data key that names any other master key, or none, is refused before KMS is called. See [Restricting the KMS keys the application can use](#restricting-the-kms-keys-the-application-can-use). | Yes | None |
| `encryptionMetadataSchema` | 4.0.0 | Schema name for encryption metadata tables | No | `encrypt` |
| `encryptionFailOnUnmappedParameter` | 4.6.0 | When `true` (default), the plugin rejects a statement on a table with encrypted columns with a `SQLException` at prepare time if it cannot reliably map the statement's parameters to columns (see [Limitations](#limitations)). When `false`, the plugin only logs a WARNING and prepares the statement; values bound to encrypted columns may then be written unencrypted, and only the HMAC trigger rejects them. While the encryption metadata can be read, it does not affect the writes in [Statements always refused](#statements-always-refused). | No | `true` |
| `metadataCacheEnabled` | 4.0.0 | Enable/disable metadata caching. Do not set this to `false` in production: the plugin then opens a short-lived metadata connection for every statement that touches an encrypted column. | No | `true` |
| `metadataCacheExpirationMinutes` | 4.0.0 | Metadata cache expiration time in minutes | No | `60` |
| `metadataCacheRefreshIntervalMs` | 4.0.0 | Metadata cache refresh interval in milliseconds | No | `300000` |
| `keyManagementMaxRetries` | 4.0.0 | Maximum number of retries for key management operations | No | `3` |
| `keyManagementRetryBackoffBaseMs` | 4.0.0 | Base backoff time in milliseconds for key management retries | No | `100` |
| `auditLoggingEnabled` | 4.0.0 | Enable/disable audit logging | No | `false` |
| `dataKeyCacheEnabled` | 4.0.0 | Enable/disable data key caching. Do not set this to `false` in production: every statement that touches an encrypted column then makes a KMS `Decrypt` call. | No | `true` |
| `dataKeyCacheMaxSize` | 4.0.0 | Maximum size of data key cache | No | `1000` |
| `dataKeyCacheExpirationMs` | 4.0.0 | Data key cache expiration in milliseconds | No | `3600000` |

No connection property selects which KMS master key encrypts a column: each column's master key is chosen when encryption is set up for that column, and recorded in `key_storage`. `encryptionAllowedMasterKeyArns` only restricts which of those recorded keys the plugin may use.

### Example Connection

The connection configuration is the same for PostgreSQL and MySQL:

```java
Properties props = new Properties();
props.setProperty("user", "username");
props.setProperty("password", "password");
props.setProperty("wrapperPlugins", "sqlParser,kmsEncryption"); // kmsEncryption requires sqlParser
props.setProperty("kms.region", "us-east-1"); // Defaults to us-east-1 if not set
// Required: the KMS master keys the plugin may use, as recorded in key_storage.master_key_arn
props.setProperty("encryptionAllowedMasterKeyArns",
    "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012");
props.setProperty("encryptionMetadataSchema", "encrypt"); // Optional, defaults to "encrypt"

// PostgreSQL
String pgUrl = "jdbc:aws-wrapper:postgresql://my-cluster.cluster-xyz.us-east-1.rds.amazonaws.com:5432/mydb";

// MySQL
String mysqlUrl = "jdbc:aws-wrapper:mysql://my-cluster.cluster-xyz.us-east-1.rds.amazonaws.com:3306/mydb";

Connection conn = DriverManager.getConnection(pgUrl, props); // or mysqlUrl
```

The same configuration as a connection URL:

```
jdbc:aws-wrapper:postgresql://host:port/database?wrapperPlugins=sqlParser,kmsEncryption&kms.region=us-east-1&encryptionAllowedMasterKeyArns=arn%3Aaws%3Akms%3Aus-east-1%3A123456789012%3Akey%2F12345678-1234-1234-1234-123456789012
```

The `sqlParser` plugin is required and must run before `kmsEncryption`. List it first; with `autoSortWrapperPluginOrder` enabled (the default) the driver also sorts it ahead of `kmsEncryption`. If `sqlParser` is missing, preparing a statement, or running SQL through a `Statement`, fails with `SQL parse results not found in call context`.

### AWS Credentials

The plugin calls KMS with the same AWS credentials provider as the driver's other AWS plugins. By default this is the AWS SDK default credentials provider chain; set `awsProfile` to use a named profile (see [AWS Credentials Configuration](../custom-configuration/AwsCredentialsConfiguration.md)). See [AWS Credentials](../AwsCredentials.md) for keeping credentials valid.

For the IAM permissions the credentials need, see [KMS Key Permissions](#kms-key-permissions).

## Usage

Once configured, the plugin works transparently. Values bound to encrypted columns with `PreparedStatement` setters are encrypted, and values read from encrypted columns are decrypted:

```java
// INSERT - automatic encryption
String sql = "INSERT INTO users (name, ssn, credit_card, email) VALUES (?, ?, ?, ?)";
try (PreparedStatement stmt = conn.prepareStatement(sql)) {
    stmt.setString(1, "Alice");
    stmt.setString(2, "123-45-6789");          // Automatically encrypted
    stmt.setString(3, "4111-1111-1111-1111");  // Automatically encrypted
    stmt.setString(4, "alice@example.com");
    stmt.executeUpdate();
}

// SELECT - automatic decryption
String query = "SELECT name, ssn, credit_card FROM users WHERE id = ?";
try (PreparedStatement stmt = conn.prepareStatement(query)) {
    stmt.setInt(1, userId);
    try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
            String ssn = rs.getString("ssn");                // Automatically decrypted
            String creditCard = rs.getString("credit_card"); // Automatically decrypted
        }
    }
}

// UPDATE - automatic encryption
String update = "UPDATE users SET ssn = ? WHERE id = ?";
try (PreparedStatement stmt = conn.prepareStatement(update)) {
    stmt.setString(1, "987-65-4321");  // Automatically encrypted
    stmt.setInt(2, userId);
    stmt.executeUpdate();
}
```

Filter and join on columns that are not encrypted. A condition such as `WHERE ssn = ?` on an encrypted column never matches; see [Other limitations](#other-limitations).

### Verifying Encryption

To verify data is actually encrypted in storage, connect without the plugin:

```java
// Connection WITHOUT encryption plugin
Connection plainConn = DriverManager.getConnection(
    "jdbc:postgresql://my-cluster.cluster-xyz.us-east-1.rds.amazonaws.com:5432/mydb", "user", "password");

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
3. The format is `table.column`. A bare `column` is also accepted and refers to the table the statement writes to; prefer `table.column`, which does not depend on the parser identifying that table
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
/* @encrypt:users.ssn */ ?         -- Space after /* not allowed
```

## Limitations

Every encrypted column must be registered in the metadata tables (see [Setup](#setup)).

This section lists what the plugin does not support, which statements it rejects, and what it does not protect. Statements that work are in [Statements the plugin maps automatically](#statements-the-plugin-maps-automatically).

### Statements the plugin maps automatically

Automatic encryption works for `PreparedStatement` parameters in these statement shapes:

- `INSERT` with a column list: single-row and multi-row `VALUES`, `INSERT ... SELECT` (including `UNION`), and MySQL `INSERT ... SET`
- `UPDATE ... SET`, including row assignments such as `SET (name, ssn) = (?, ?)`
- PostgreSQL `INSERT ... ON CONFLICT ... DO UPDATE SET` and MySQL `INSERT ... ON DUPLICATE KEY UPDATE`

A value is encrypted only when it is bound by its own `?` placeholder directly to the encrypted column. A cast around the placeholder and parentheses are allowed, for example `CAST(? AS bytea)`, `?::bytea` or `(?)`. String literals and other expressions are never encrypted: a literal written to an encrypted column, as in `SET ssn = '123-45-6789'`, is refused (see [Statements always refused](#statements-always-refused)), and a parameter inside an expression, as in `SET ssn = lower(?)`, is rejected by default. Literals and expressions in other columns of the same statement are fine and do not shift the parameter numbering.

### Statements always refused

On a table with encrypted columns, the plugin refuses with a `SQLException` a statement that writes an encrypted column with a value that is not a bind parameter: a literal, as in `SET ssn = '123-45-6789'`, or an expression that reads no column and holds no subquery. Such a value is certain to be stored as plaintext. A prepared statement is refused when it is prepared. SQL passed directly to `execute`, `executeQuery`, `executeUpdate` or `addBatch(String)` is refused by that call, before it is sent. This includes `addBatch(String)` on a prepared statement, which some drivers, such as MySQL Connector/J, accept. As long as the encryption metadata can be read, `encryptionFailOnUnmappedParameter` does not change this.

The check covers `INSERT` with a column list (including `INSERT ... SELECT` and MySQL `INSERT ... SET`), `UPDATE ... SET`, and the assignments of PostgreSQL `ON CONFLICT ... DO UPDATE SET` and MySQL `ON DUPLICATE KEY UPDATE`. It does not cover `REPLACE`, `MERGE`, `INSERT` without a column list, a `VALUES` row whose number of values differs from the column list, data-modifying CTEs, a string with several statements, or SQL the parser cannot read. Those are left to the HMAC trigger; a prepared statement of one of these shapes that has parameters is also subject to [Statements rejected by default](#statements-rejected-by-default). A value read from another column or a subquery, a MySQL user variable, `DEFAULT` and `NULL` are not refused either: they may be ciphertext the database already holds, and the HMAC trigger checks every value except `NULL`.

### Statements rejected by default

On a table with encrypted columns, the plugin rejects the following statements with a `SQLException` when they are prepared, so that no value is written unencrypted (controlled by `encryptionFailOnUnmappedParameter`, default `true`):

- `INSERT` without a column list
- parameters inside an expression (function call, arithmetic, concatenation, `CASE`, sub-select) that is assigned to an encrypted column
- parameters whose target column cannot be determined, for example a sub-select or an unknown target table
- `REPLACE` and `MERGE` statements
- statements the SQL parser cannot read, for example PostgreSQL `E'...'` strings, MySQL backslash escapes inside string literals (`'it\'s'`), or the PostgreSQL JSONB `??` operator
- numbered placeholders such as `?1`
- statements where the number of parameters found by the parser differs from the number of placeholders the driver sends

This check applies to prepared statements only: SQL passed directly to `execute`, `executeQuery`, `executeUpdate` or `addBatch` has no bound values, and a `?` in it is not a parameter. When the encryption metadata cannot be read, the plugin cannot tell which columns are encrypted, so `encryptionFailOnUnmappedParameter` decides whether every check, including the one in [Statements always refused](#statements-always-refused), refuses the statement or only logs a WARNING.

Statements on tables without encrypted columns are never rejected. If you encounter a `SQLException` because your statement falls into one of the categories above, see the [Troubleshooting](#troubleshooting) section for how to fix it.

### Other limitations

- Encrypted columns cannot be searched or compared. Each value is encrypted with a random IV, so the same plaintext is stored as different bytes every time: a condition such as `WHERE ssn = ?`, or a join on an encrypted column, never matches. Filter and join on columns that are not encrypted.
- Other operations on an encrypted column are equally meaningless on the stored bytes: `ORDER BY`, `GROUP BY`, `DISTINCT`, `LIKE`, range comparisons, aggregates such as `MIN` or `MAX`, and `UNIQUE` constraints (two rows with the same plaintext are stored as different bytes, so a duplicate is never detected). Do these in the application after decrypting, or on an unencrypted column. `IS NULL` and `IS NOT NULL` still work.
- Values are encrypted from these `PreparedStatement` setters: `setString`, `setNString`, `setInt`, `setLong`, `setShort`, `setByte`, `setDouble`, `setFloat`, `setBoolean`, `setBigDecimal`, `setDate`, `setTime`, `setTimestamp`, `setBytes` and `setObject`. The type byte of the [encrypted data format](#encrypted-data-format) records the value's Java type.
- Stream and LOB setters (`setCharacterStream`, `setNCharacterStream`, `setAsciiStream`, `setBinaryStream`, `setUnicodeStream`, `setClob`, `setNClob`, `setBlob`), and `setURL`, `setArray`, `setRef`, `setRowId` and `setSQLXML`, throw a `SQLException` when the parameter maps to an encrypted column. Use `setString`, `setNString`, `setBytes` or `setObject`. `setObject` also fails on a URL, `Array`, `Ref`, `RowId` or `SQLXML` value, so convert it to a supported type first, for example with `setString(index, url.toString())`.
- Values are decrypted by these `ResultSet` getters: `getString`, `getInt`, `getLong`, `getShort`, `getByte`, `getDouble`, `getFloat`, `getBoolean`, `getBigDecimal`, `getDate`, `getTime`, `getTimestamp`, `getBytes` and `getObject`. `getObject` returns the Java type that was written, including `LocalDate`, `LocalTime` and `LocalDateTime`, which have no dedicated getter. Read each value with the getter that matches the type it was written with:
  - Numeric getters convert between numeric types (for example `getLong` on an `int`), as in a plain JDBC read.
  - `getString` returns the text form of any written type except `byte[]`, whose text form is not meaningful.
  - `getBytes` returns the bytes that were written with `setBytes`. On a column written with `setString` it returns the string's UTF-8 bytes. (Earlier versions treated the text as base64 and failed with a type conversion error, or returned the decoded bytes if the text happened to be valid base64. If you relied on that, decode the string from `getString` yourself.)
  - A getter from a different family than the written type, such as `getTimestamp` on a `LocalDateTime`, fails with a type conversion error.
  - `Date`, `Time` and `Timestamp` values are stored as epoch milliseconds, so a `Timestamp` keeps millisecond precision only.
- Other getters (`getNString`, `getAsciiStream`, `getBinaryStream`, `getCharacterStream`, `getClob`, `getBlob`, `getArray` and similar) are not intercepted and are not tested. On an encrypted column they return the stored ciphertext or fail, not the decrypted value. Use one of the getters listed above.
- SQL run through a plain `Statement` is not encrypted. The plugin checks it only for the writes described in [Statements always refused](#statements-always-refused); any other value is sent as written.
- `CallableStatement` setters are not intercepted; values passed to stored procedures are not encrypted.
- Values that come from a CTE or from another column (`INSERT INTO users (ssn) SELECT ssn FROM staging`) are not encrypted by the plugin.
- Table names are matched against the metadata case-sensitively. Use the same spelling in statements as in `encryption_metadata`.

### What the plugin does not protect

The plugin encrypts the values of the columns you register, in the application. It does not provide:

- **Protection from a user who can use the application's credentials.** Anyone who can run code with the runtime role's `kms:Decrypt` and its database access can read every encrypted column the plugin can. The plugin is not an access control inside the application.
- **Protection of plaintext before and after the plugin.** Values are plaintext in application memory, in your logs and caches, in query strings sent through a plain `Statement` or `CallableStatement`, and in any column that is not registered.
- **Protection of other columns, indexes, and metadata.** Column names, table names, row counts, value lengths (ciphertext length follows plaintext length), null versus non-null, and the types written are visible to anyone who can read the table. Unencrypted columns of the same row are visible.
- **Searching or ordering on encrypted columns** (see [Other limitations](#other-limitations)).
- **Row binding or replay protection** (see [Encrypted values are not tied to their row](#encrypted-values-are-not-tied-to-their-row)).
- **A guarantee that no plaintext is stored.** The plugin finds encrypted columns by parsing SQL, so a statement it cannot map is rejected or left to the trigger (see [Limitations](#limitations)), and the trigger can be bypassed by the roles listed in [Who can disable or skip the HMAC trigger](#who-can-disable-or-skip-the-hmac-trigger).
- **Protection from a compromised database administrator with metadata access**, who can read the HMAC keys and write the metadata (see [Why write access to `key_storage` is dangerous](#why-write-access-to-key_storage-is-dangerous)). The data itself stays protected as long as they cannot call KMS with a role that may decrypt it.
- **Protection of existing plaintext.** Registering a column does not encrypt values already in it. Existing rows must be re-written through the plugin; until then a read of a plaintext value fails, because it carries no valid HMAC.
- **Encryption in transit.** Use SSL/TLS for the database connection.

## Key Rotation

The plugin uses two levels of keys. A KMS key (master key) wraps each data key. Each encrypted column has one data key and one HMAC key, stored in `encrypt.key_storage`. Encrypted values do not record which data key encrypted them, so the plugin can only read a column whose values are all encrypted under the column's current data key.

For a complete program that rotates data keys with `KeyManagementUtility`, see [KeyManagementExample.java](../../../examples/EncryptionExample/src/main/java/software/amazon/KeyManagementExample.java).

To change which KMS key a column uses, see [Moving a column to a new master key](#moving-a-column-to-a-new-master-key).

### Rotating the KMS key (recommended)

Enable automatic rotation on the KMS key:

```bash
aws kms enable-key-rotation --key-id <KEY_ID>
```

KMS keeps the previous key material and uses it to decrypt data keys wrapped before the rotation. The rotation is transparent to the plugin and needs no database changes. It does not change the data keys themselves.

Rotate with KMS key rotation, not by pointing an alias at a different KMS key. The plugin decrypts each data key with the key named in `key_storage.master_key_arn`. If that is an alias and the alias is moved to another key, KMS refuses to decrypt the data keys wrapped under the previous key, and the columns that use them can be neither read nor written until the alias points back. To move a column to a different KMS key, use [`rotateDataKeyAndReencrypt()`](#rotating-a-columns-data-key).

### Moving a column to a new master key

To move a column to a new master key:

1. Create the new master key and grant the runtime role `kms:Decrypt` on it, and the admin role the actions in [Separating the runtime role from the admin role](#separating-the-runtime-role-from-the-admin-role).
2. Add the new key to `encryptionAllowedMasterKeyArns` for the application and for `KeyManagementUtility`, in the same form (ARN, alias or key id) that `key_storage` will hold. Keep the old key listed.
3. Follow the [Data key rotation procedure](#data-key-rotation-procedure), passing the new master key to `rotateDataKeyAndReencrypt()`. Reads of rows that are not converted yet fail while it runs, so plan for it as described there.
4. Wait until every application instance has refreshed its metadata (`metadataCacheRefreshIntervalMs`) or restarted, and the final `reencryptRemainingRows()` check has passed.
5. Only then remove the old key from the allow-list and from the runtime and admin roles' IAM policies.

### Rotating a column's data key

Rotate a column's data key only when you need a new data key, for example after a suspected data key exposure or to move a column to a different KMS key. Use `KeyManagementUtility.rotateDataKeyAndReencrypt()`. It must run on a connection that does not use the `kmsEncryption` plugin: either a plain target driver connection, or an AWS Advanced JDBC Wrapper connection whose `wrapperPlugins` do not include `kmsEncryption`. The utility checks this and throws an error if the plugin is present.

```java
import java.sql.Connection;
import java.sql.DriverManager;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.jdbc.plugin.encryption.key.*;
import software.amazon.jdbc.plugin.encryption.metadata.*;
import software.amazon.jdbc.plugin.encryption.model.*;

EncryptionConfig config = EncryptionConfig.builder()
    .kmsRegion("us-east-1")
    .encryptionMetadataSchema("encrypt")
    // Checks the KMS key recorded in key_storage; required to pass null below
    .allowedMasterKeyArns("arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012")
    .build();

KmsClient kmsClient = KmsClient.builder()
    .region(Region.US_EAST_1)
    .build();

// Connection without the kmsEncryption plugin (see the procedure below for a wrapper connection)
Connection connection = DriverManager.getConnection(
    "jdbc:postgresql://my-cluster.cluster-xyz.us-east-1.rds.amazonaws.com:5432/mydb", "user", "password");

KeyManager keyManager = new KeyManager(kmsClient, connection, true, config); // true = PostgreSQL
MetadataManager metadataManager = new MetadataManager(connection, config);
KeyManagementUtility utility = new KeyManagementUtility(
    keyManager, metadataManager, connection, kmsClient, config);

// "id" is the primary key column of users; null keeps the current KMS key
DataKeyReencryptionResult result = utility.rotateDataKeyAndReencrypt(
    "users", "ssn", "id", null, KeyManagementUtility.DEFAULT_REENCRYPTION_BATCH_SIZE);
System.out.println(result); // new key id, old key id, re-encrypted and unmatched row counts
```

Pass a KMS key ARN instead of `null` to wrap the new data key under a different KMS key. Passing `null` keeps the KMS key recorded in `key_storage`, so it is only accepted when the `EncryptionConfig` has `allowedMasterKeyArns` to check that key against; without it, the method throws an `IllegalArgumentException`. When the allow-list is set, it must contain both the current KMS key, which the re-encryption uses to decrypt the old data key, and the new one; the method refuses an unlisted key with SQL state `KEY07` before it changes anything. The application's `encryptionAllowedMasterKeyArns` must list the new KMS key too, or the plugin will refuse the column after the rotation. See [Restricting the KMS keys the application can use](#restricting-the-kms-keys-the-application-can-use).

`rotateDataKeyAndReencrypt()` does the following:
1. Checks that every value of the column verifies against the current HMAC key. If any value does not, it stops with an error and changes nothing.
2. Creates a new data key and HMAC key and stores them in `encrypt.key_storage`. If the rotation fails before the switch in step 3 is committed, it deletes this new key row again; if that is not possible, the error message names the new key id.
3. Switches `encrypt.encryption_metadata.key_id` to the new key and commits.
4. Re-encrypts the rows in primary key order, `batchSize` rows per transaction. Values are decrypted with the old key and encrypted with the new key; the stored type of each value is kept.

It never deletes the old `key_storage` row and never changes the old KMS key.

Requirements:
- The primary key column must be a single, unique, `NOT NULL` column. Rows with a `NULL` value in it are not processed.
- On PostgreSQL, the primary key column must have a built-in type, such as `integer`, `bigint`, `uuid` or `text`, or a domain over one. A column of another type, such as `citext` or an enum, is refused with nothing changed. This applies to `rotateDataKeyAndReencrypt()` and `reencryptRemainingRows()`.
- Table and column names may contain only letters, digits and underscores, at most 64 characters. They are matched against `encrypt.encryption_metadata` as stored there, with the bare table name, the way the plugin stores it. On PostgreSQL the match is case-sensitive, and unquoted names are stored in lower case (`users`, not `Users`). The SQL against the data table uses the table and column spelling stored in the metadata.
- A table name may have one schema prefix (`app.users`). The prefix is used only in the SQL against the data table; the metadata lookup and the HMAC trigger use the bare table name (`users`).
- Before it creates a key, the utility checks that the table, the encrypted column and the primary key column exist. A wrong name fails with nothing changed.
- The HMAC triggers on both PostgreSQL and MySQL look up the key from the metadata for every row and ignore any session setting, so nothing needs priming on either database.

> [!WARNING]
> `KeyManagementUtility.rotateDataKey()` does **not** re-encrypt existing rows. It only stores a new data key and points the column's metadata at it, so it is only for a column that holds no data. Before it changes anything it runs `SELECT 1 FROM <table> WHERE <column> IS NOT NULL LIMIT 1`. If a row comes back, it throws a `KeyManagementException` that points to `rotateDataKeyAndReencrypt()`, and the column keeps its key. On a column with no non-null value it works as before.
>
> A column that a release without this check already rotated has the following symptoms:
> - Every row encrypted under the previous data key fails to decrypt through the plugin with `HMAC verification failed`.
> - Running applications keep using the previous key until their metadata cache refreshes (`metadataCacheRefreshIntervalMs`, 5 minutes by default). With the HMAC trigger, which every encrypted column requires, their writes to the column are rejected until then.
> - The data is recoverable only while the previous `key_storage` row and its KMS key exist. Do not delete them.
>
> To recover such a column, see [Finishing or repairing a re-encryption](#finishing-or-repairing-a-re-encryption).

### Finishing or repairing a re-encryption

`KeyManagementUtility.reencryptRemainingRows(table, column, primaryKeyColumn, oldKeyIds, batchSize)` re-encrypts values that are still under one of the given old key ids into the column's current key. It uses the same batching and HMAC check as the rotation and is safe to run again: values already under the current key are skipped. It requires `allowedMasterKeyArns` on the `EncryptionConfig` and throws an `IllegalArgumentException` without it: the values are re-encrypted into the column's current data key, and the master key of that key is read from `key_storage`, so it must be checked against the list. Use it:

- To resume a failed rotation. Committed batches are kept; the exception message names the old key id to pass.
- To recover a column that an earlier release's `rotateDataKey()` was called on while it held data. Find the previous key id:

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

1. **Plan a maintenance window.** Stop or pause the applications that write to the column if you can. While the batches run, reads through the plugin fail with `HMAC verification failed` for rows that are not converted yet. Application instances that have not refreshed their metadata yet (`metadataCacheRefreshIntervalMs`) cannot write the column either, because the HMAC trigger checks every value against the new key. Restart the applications right after step 3 if you cannot pause them.
2. **Back up** the table and the `encrypt` schema.
3. **Run `rotateDataKeyAndReencrypt()`** on a connection without the `kmsEncryption` plugin. Record the old and new key ids from the log or from `result.getSourceKeyIds()` and `result.getCurrentKeyId()`.
4. **If it fails**, fix the cause and run `reencryptRemainingRows()` with the old key id. Committed batches are kept.
5. **Wait at least `metadataCacheRefreshIntervalMs`** (default 300000 ms) or restart the applications, so that every instance uses the new key, then run `reencryptRemainingRows()` again with the old key id as a final check. It converts any row that is still under the old key and reports rows that match neither key.
6. **With the PostgreSQL HMAC trigger**, the trigger function `validate_encrypted_data_hmac()` from this version looks up the column's HMAC key from the metadata for every row and caches nothing, so pooled sessions pick up the new key on their own with no reload logic or connection recycling. If your `validate_encrypted_data_hmac()` was created by an earlier (caching) version — `SELECT obj_description('validate_encrypted_data_hmac'::regproc, 'pg_proc')` returns NULL rather than the `...trigger v1` marker — re-create it by running [`encrypted_data_type.sql`](../../../wrapper/src/test/resources/sql/encrypted_data_type.sql) from this version again, with `SCHEMA_NAME` replaced by your metadata schema. The script is safe to re-run on a database with data (see [step 3 of Setup](#3-install-the-encrypted-data-type-and-hmac-functions)), and it also sets the function comment that the check above looks for. First make sure pgcrypto is in the `public` schema, because this version calls it as `public.hmac` under a locked `search_path`: an earlier install with a bare `CREATE EXTENSION pgcrypto` may have put it in another schema, and `CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public` is a no-op when the extension already exists elsewhere. Move it with `ALTER EXTENSION pgcrypto SET SCHEMA public` (check with `SELECT extnamespace::regnamespace FROM pg_extension WHERE extname = 'pgcrypto'`); otherwise the next write fails with `function public.hmac(bytea, bytea, text) does not exist`.
   **With the MySQL HMAC trigger**, the procedures `validate_encrypted_data_hmac_before_insert` and `validate_encrypted_data_hmac_before_update` from this version also look up the column's HMAC key from the metadata for every row and cache nothing, so pooled sessions pick up the new key on their own with no reload logic or connection recycling. These two procedures take different arguments than the single `validate_encrypted_data_hmac` procedure of an earlier version, so you must re-install the script and drop and re-create the triggers as described in [step 7 of Setup](#7-add-hmac-validation-triggers). Until the triggers are re-created, they still call the dropped procedure, so every INSERT and UPDATE on those tables fails; run both steps together, in a maintenance window if the tables take live writes. The MySQL script only replaces routines and does not touch your tables.
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
    // Checks the KMS key recorded in key_storage; required to pass null below
    .allowedMasterKeyArns("arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012")
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

Step 5 is a separate job. Run it after `metadataCacheRefreshIntervalMs` has passed (5 minutes by default), or after restarting the applications, so that every instance uses the new key. It converts any row that is still under the old key. Set up the connection, `config`, `kmsClient` and `utility` as in the first block, and pass the old key id recorded there:

```java
// Step 5, a separate job run after metadataCacheRefreshIntervalMs: final check
int oldKeyId = 42; // the value recorded in step 3
DataKeyReencryptionResult repair = utility.reencryptRemainingRows(
    "users", "ssn", "id", Collections.singletonList(oldKeyId),
    KeyManagementUtility.DEFAULT_REENCRYPTION_BATCH_SIZE);
System.out.println("Converted " + repair.getReencryptedCount() + " remaining rows");
if (!repair.isComplete()) {
  // These rows match neither the current key nor the old key. They are left unchanged.
  System.err.println(repair.getUnmatchedCount() + " unmatched rows, for example "
      + repair.getUnmatchedPrimaryKeys());
}
```

## Security Considerations

### Data Protection

- Encrypted data is stored as binary data in the database
- The original data never leaves your application - encryption/decryption happens locally using data keys from KMS
- Only encryption keys are managed by AWS KMS, not the actual data
- Use different KMS keys for different environments (dev, staging, prod)
- Restrict access to the tables in the metadata schema. The HMAC keys in `key_storage` are stored in plaintext so that the triggers can read them. A user that can both read `key_storage` and write an encrypted column can compute a valid HMAC for any value, so the trigger stops plaintext that reaches the column by mistake, not a user who deliberately misuses that access. The application's database user needs `SELECT` on `key_storage` and `encryption_metadata`. On PostgreSQL the trigger function runs with the privileges of the user whose statement fires it, so every user that writes an encrypted column needs `SELECT` on both tables as well. On MySQL the trigger runs as its definer, so writers need no access to the metadata tables. Only the user that runs `KeyManagementUtility` needs to write to them (see [Separating the runtime role from the admin role](#separating-the-runtime-role-from-the-admin-role))
- Always use SSL/TLS for database connections
- Enable CloudTrail logging for KMS operations

### Restricting the KMS keys the application can use

The plugin does not take the master key for a column from its own configuration: it reads it from the column's `key_storage` row and asks KMS to decrypt the data key under it. Two independent controls keep a changed row from pointing the plugin at a key it should not use, such as a key in another AWS account.

**List the permitted master keys in `encryptionAllowedMasterKeyArns`.** This property is required; the plugin refuses to start without it. Before every KMS call, including when the data key is already cached, the plugin checks the master key named by `key_storage` against the list. A key that is not listed, or a row that names no key, is refused with a `KeyManagementException` whose SQL state is `KEY07`, and KMS is never called. The refusal applies to reads and writes alike.

The comparison is exact, so list each key in the same form (ARN, alias, or key id) that was passed to `KeyManagementUtility` when the column was set up. Add a new master key to the list before rotating a column onto it, or the application will refuse that column until the list is updated. Keep the old master key listed, and allowed `kms:Decrypt` in the application's IAM policy, until every application instance has picked up the new key (after `metadataCacheRefreshIntervalMs`, or a restart): until then, instances still using their cached metadata keep decrypting the column's values with the old data key, which is wrapped under the old master key. The re-encryption itself also decrypts the old data key, so the old master key must stay on the allow-list given to `KeyManagementUtility`, and allowed in its IAM policy, until the re-encryption is complete.

`KeyManagementUtility` does not require the list, so a program can create a master key and use it straight away, but it enforces the list when the `EncryptionConfig` passed to its `KeyManager` has one (`EncryptionConfig.builder().allowedMasterKeyArns(...)`). Without the list, `rotateDataKey()` and `rotateDataKeyAndReencrypt()` must be passed the master key to rotate onto, and throw an `IllegalArgumentException` when given `null`: keeping the current key would mean trusting whatever ARN `key_storage` holds. For the same reason, `reencryptRemainingRows()` throws an `IllegalArgumentException` without the list, because it re-encrypts into the column's current key as recorded in `key_storage`.

**Scope the application's IAM permissions to the same keys.** Grant the credentials the plugin runs under only the KMS actions they need, on the specific master key ARN(s) the columns use - never `"Resource": "*"`. Use a distinct key per environment. This also blocks a key in another AWS account: using one requires permission in your own account's policy as well as in that key's policy.

### KMS Key Permissions

The plugin needs only `kms:Decrypt` on the master keys its columns use:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kms:Decrypt"
            ],
            "Resource": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
        }
    ]
}
```

Grant the actions `KeyManagementUtility` uses (see [AWS Requirements](#aws-requirements)) only to the credentials that set up and rotate the columns.

### KMS key policy and lifecycle

- **Do not use the `kms:ViaService` condition key.** It allows a call only when it is made on your behalf by an AWS service, such as RDS or S3. The plugin calls KMS directly from your application, so a key policy or IAM policy that requires `kms:ViaService` denies every call. Restrict access with the principal, the key ARN, and conditions that apply to direct calls instead, such as `aws:PrincipalArn` or `aws:SourceVpce`. The plugin does not set a KMS encryption context, so `kms:EncryptionContext` conditions do not apply.
- **Do not delete, disable or schedule the deletion of a master key that existing values still need.** Every data key in `key_storage`, and every backup of it, can be decrypted only through its master key. Once KMS deletes the key, those data keys and every value encrypted under them cannot be recovered, by you or by AWS. A disabled key or one pending deletion fails every `Decrypt` call, so a column stops being readable as soon as the cached data keys expire. Before retiring a key, re-encrypt every column that uses it onto another key, and confirm that no row of `key_storage` and no backup you may restore still names it. Prefer disabling a key for a trial period (and watching CloudTrail for `Decrypt` calls) over scheduling its deletion, and use the longest waiting period.

To change a column's master key, see [Moving a column to a new master key](#moving-a-column-to-a-new-master-key).

### Separating the runtime role from the admin role

Run the application and the setup and rotation tooling under different credentials, so that a compromised application cannot change which keys protect a column.

| | Runtime (application) | Admin (`KeyManagementUtility`, DDL) |
|---|---|---|
| AWS | `kms:Decrypt` only, on the specific master key ARNs the columns use | `kms:GenerateDataKey` and `kms:Decrypt` on those ARNs, plus `kms:DescribeKey` for `validateMasterKey()`. `kms:CreateKey` and `kms:CreateAlias` only if it creates master keys |
| Database | `SELECT` on `key_storage` and `encryption_metadata`; normal DML on the application tables | Write access to `key_storage` and `encryption_metadata`; `SELECT` and `UPDATE` on the encrypted columns' tables (re-encryption reads and rewrites every row); DDL and trigger creation on the application tables |

The runtime role must not have `kms:GenerateDataKey`, `kms:Encrypt`, `kms:CreateKey` or `"Resource": "*"`, and must not be able to write the metadata tables. Without write access to `key_storage`, a compromised application cannot point a column at a different master key or HMAC key, and the `encryptionAllowedMasterKeyArns` list and the trigger stay meaningful. The runtime role must also not own the application tables, since that lets it drop or disable the HMAC triggers, or hold the `TRIGGER` privilege on them (see [Who can disable or skip the HMAC trigger](#who-can-disable-or-skip-the-hmac-trigger)); create those tables as the admin role, or move ownership with `ALTER TABLE ... OWNER TO app_admin` on PostgreSQL. On PostgreSQL it must not own the schemas that contain the metadata or application tables either, because a schema's owner can drop any table in it. Use a separate admin credential, held by your deployment pipeline or an operator, and do not ship it with the application.

**PostgreSQL**, with `app_runtime` as the application's role and `app_admin` as the admin role (replace `encrypt` with your metadata schema):

```sql
-- Ownership first: changing an object's owner rewrites the previous owner's grants to the new owner,
-- so do it before the GRANT statements below.
-- Transfer the schema before the tables: a schema's owner can drop any table in it, even one owned by another role
ALTER SCHEMA encrypt OWNER TO app_admin;
ALTER TABLE encrypt.key_storage OWNER TO app_admin;
ALTER TABLE encrypt.encryption_metadata OWNER TO app_admin;
-- The admin role owns the application table so that the runtime role cannot drop its triggers.
-- Also make sure app_runtime does not own the table's schema (on PostgreSQL 15 and later, public is owned
-- by the database owner) or the database; if it does, transfer them with ALTER SCHEMA ... OWNER TO app_admin
-- and ALTER DATABASE ... OWNER TO app_admin
ALTER TABLE users OWNER TO app_admin;

-- Metadata: read-only for the application
REVOKE ALL ON ALL TABLES IN SCHEMA encrypt FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA encrypt FROM app_runtime;
GRANT USAGE ON SCHEMA encrypt TO app_runtime;
GRANT SELECT ON encrypt.key_storage, encrypt.encryption_metadata TO app_runtime;

-- Repeat for every other role that writes an encrypted column (an ETL or batch role, for example)
GRANT USAGE ON SCHEMA encrypt TO <other_writer_role>;
GRANT SELECT ON encrypt.key_storage, encrypt.encryption_metadata TO <other_writer_role>;

-- Metadata: writable only by the admin role
GRANT USAGE ON SCHEMA encrypt TO app_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON encrypt.key_storage, encrypt.encryption_metadata TO app_admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA encrypt TO app_admin;

-- Application table: normal DML for the runtime role (including the id sequence)
GRANT SELECT, INSERT, UPDATE, DELETE ON users TO app_runtime;
GRANT USAGE ON SEQUENCE users_id_seq TO app_runtime;
```

The simplest way to avoid ownership problems is to create the metadata schema and tables (steps 2 to 4) and the application tables (step 6) as `app_admin` in the first place, and to run this block as `app_admin`; the `ALTER ... OWNER` statements then change nothing. To run it later against objects that another role owns, connect as a role that may take over their ownership: the RDS master user (`rds_superuser`), or a role that is a member of both the current owner and `app_admin`. A role that is neither fails with `must be owner of schema encrypt` on the first statement (or `must be owner of table key_storage` if the schema is already transferred), and its `GRANT` and `REVOKE` statements on those tables then fail with `permission denied for table key_storage`. A member-of-both role can run `ALTER TABLE ... OWNER TO app_admin` only if `app_admin` has `CREATE` on the table's schema, which is why the block transfers the schema first.

Once `app_admin` owns the application tables, migrations that alter them (Flyway, Liquibase, Rails and similar) can no longer run as `app_runtime`. Run migrations that touch tables with encrypted columns as the admin role.

These statements only remove direct grants. Also make sure `app_runtime` is not a superuser (on RDS, a member of `rds_superuser`), and is not a member of `app_admin` or of `pg_write_all_data` (PostgreSQL 14 and later): any of these still lets it write the metadata tables. A role that owns the metadata tables can also grant itself access again, which is why the admin role should own them.

The trigger function runs with the privileges of the user whose statement fires it, so every role that writes an encrypted column, not only `app_runtime`, needs the `SELECT` grants above. A role that has not been granted `USAGE` on the schema and `SELECT` on both tables fails on every write to an encrypted column, inside `validate_encrypted_data_hmac`, with `permission denied for schema encrypt` (or `for table key_storage`); this includes a role that used to read the metadata through `PUBLIC`, which the block revokes. Grant it the access shown above. Roles that never write encrypted columns need no access to the schema.

**MySQL**, with `'app_runtime'@'%'` and `'app_admin'@'%'` (replace `encrypt` with your metadata schema):

```sql
REVOKE ALL PRIVILEGES ON encrypt.* FROM 'app_runtime'@'%';
GRANT SELECT ON encrypt.key_storage TO 'app_runtime'@'%';
GRANT SELECT ON encrypt.encryption_metadata TO 'app_runtime'@'%';

GRANT SELECT, INSERT, UPDATE, DELETE ON encrypt.key_storage TO 'app_admin'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON encrypt.encryption_metadata TO 'app_admin'@'%';

-- Routines and triggers (steps 3 and 7) and re-encryption, in the database that holds your tables (app_db)
GRANT CREATE ROUTINE, ALTER ROUTINE, EXECUTE ON app_db.* TO 'app_admin'@'%';
GRANT SELECT, UPDATE, TRIGGER ON app_db.users TO 'app_admin'@'%';
```

`REVOKE ALL PRIVILEGES ON encrypt.*` removes only schema-level grants. It does not touch global grants (`ON *.*`, for example when the application connects as the RDS master user) or table-level grants such as an earlier `GRANT INSERT ON encrypt.key_storage`, and it fails with error 1141 if the account holds no schema-level grant at all. Run `SHOW GRANTS FOR 'app_runtime'@'%'` and revoke any global or table-level privilege that gives write access to the metadata tables, and do not grant `app_runtime` the `TRIGGER` privilege on the application tables.

On MySQL the triggers and routines run with the privileges of their `DEFINER`, which is the account that installed them (the shipped script sets no `SQL SECURITY` clause). That account, the admin role, therefore needs `SELECT` on the metadata tables and the privileges to create routines and triggers (see step 3). The trigger reads the metadata as its definer, so a role that writes an encrypted column without going through the plugin (for example, a batch job or the `mysql` client) needs no privileges on the metadata schema. The plugin still reads both tables itself, so the database user it connects as always needs the `SELECT` grants above.

> [!WARNING]\
> Do not drop the definer account or revoke its `SELECT` on the metadata tables while the triggers exist. Every `INSERT` and `UPDATE` on an encrypted table then fails, with `The user specified as a definer ('app_admin'@'%') does not exist` or `SELECT command denied to user 'app_admin'@'%' for table 'key_storage'`. To retire the account, re-create the routines and triggers under another definer first. Locking it with `ALTER USER ... ACCOUNT LOCK` only blocks logins and is safe: the triggers keep working.


Verify the separation by connecting as the runtime role and confirming that `INSERT INTO encrypt.key_storage ...` and `UPDATE encrypt.encryption_metadata ...` fail with a permission error, that `DROP TABLE encrypt.key_storage` and `DROP TRIGGER` on an encrypted table fail, and that `aws kms generate-data-key` with the runtime credentials is denied.

### Who can disable or skip the HMAC trigger

The HMAC trigger is the only database-side guarantee that an encrypted column holds no plaintext, and it is an ordinary trigger. Anyone who can remove it, bypass it or run code after it can store any value. Treat these as part of the trust boundary:

- **The table owner**, who can drop or disable the trigger (`DROP TRIGGER`, `ALTER TABLE ... DISABLE TRIGGER`). Make the admin role the owner of tables with encrypted columns (see [Separating the runtime role from the admin role](#separating-the-runtime-role-from-the-admin-role)).
- **Roles with the `TRIGGER` privilege on the table.** On MySQL such a role can drop the trigger or add one that follows it and replaces the value. On PostgreSQL it can create its own trigger, which can run after the HMAC trigger (triggers fire in name order) and replace the value it validated. Do not grant `TRIGGER` to the runtime role.
- **Superusers and the RDS master user.** A superuser can disable any trigger and is not restricted by grants. On RDS the master user (and, on PostgreSQL, members of `rds_superuser`) is not a true superuser, but is typically granted enough to drop or disable triggers and can grant itself or others the privileges above.
- **PostgreSQL `session_replication_role = replica`.** In that mode PostgreSQL does not fire ordinary triggers, so the HMAC trigger is skipped. Only a superuser, a role granted `SET` on that parameter (PostgreSQL 15 and later), or on RDS a member of `rds_superuser`, can set it, but it is also what logical replication apply workers and some bulk-load and migration tools use. Do not grant it to the runtime role.
- **Anyone who can change the trigger's inputs**: the function and the metadata tables it reads (see the next section).

None of these is stopped by the plugin. Keep the runtime role off this list, and audit who is on it for the admin role and for any person or tool with direct database access.

### Why write access to `key_storage` is dangerous

`key_storage` holds, for each column, the KMS-encrypted data key, the `master_key_arn` and the HMAC key in plaintext. `encryption_metadata` says which column uses which row. A role that can write either table can:

- **Compute a valid HMAC** for any value by writing its own `hmac_key` (or reading the current one). The trigger and the driver's read check then accept a forged or plaintext value.
- **Point a column at a different `key_storage` row or master key.** The `encryptionAllowedMasterKeyArns` list limits which master keys the plugin will call, but not which permitted key a column uses, so a column can be moved to another column's key or to any other listed key. A column whose row names a key that is not listed is refused (`KEY07`).
- **Remove a column from `encryption_metadata`.** The plugin then stops treating the column as encrypted. While the trigger exists it finds no HMAC key and rejects every write to the column; if the trigger is removed as well, plaintext is accepted.

Even read access to `key_storage` exposes the HMAC key. It does not expose the data, because the data key is only usable through KMS, but it lets a user who can also write the encrypted column forge values. Give write access to the metadata tables only to the admin role, and read access only to the roles that write an encrypted column.

### Encrypted values are not tied to their row

Each value is encrypted and authenticated on its own: AES-GCM is used without associated data, and the HMAC covers only `[Type][IV][Ciphertext]`. Nothing in a value records which row, table or column it was written for. A user who can write the encrypted column, with or without the plugin, can therefore copy a valid value from one row to another, or between columns that use the same `key_storage` row, and the trigger and the driver accept it. The copy decrypts to the original plaintext. For example, `UPDATE users SET ssn = (SELECT ssn FROM users WHERE id = 7) WHERE id = 8` is accepted, and so is swapping the values of two accounts.

The HMAC protects against a value that was altered or never encrypted. It does not protect against a value being moved. If your application needs to detect that, keep an integrity check outside the encrypted column, such as a signed record or an application-level check against another column, and limit write access to these tables as above. A value copied into a column that uses a different `key_storage` row fails verification; columns that point at the same row do not have that protection.

### What the plugin keeps out of logs and exceptions

The plugin masks master keys, KMS error text and URL credentials in its own log records, audit log entries and exception messages, at every log level:

- **Master keys.** A key is shown as `arn:aws:kms:***:***:key/<first 4>***<last 4>`, whether it was configured as an ARN, `key/<id>` or a bare key id, and a key id is masked the same way everywhere it appears. A key id of 8 characters or fewer is shown as `***`. An alias is shown as `arn:aws:kms:***:***:alias/***`, and any other ARN, such as a role, as `arn:***`. This covers the allow-list in `encryptionAllowedMasterKeyArns` when the plugin logs it.
- **KMS errors.** An error returned by KMS is logged and wrapped as its type, error code and HTTP status only, for example `KmsException[errorCode=AccessDeniedException, httpStatus=400]`. The AWS message is dropped because it repeats the key ARN and the caller's role ARN. This applies to the retry warning, to the failure logs for creating a key, generating or decrypting a data key and validating a master key, and to the messages of the exceptions the plugin throws for those failures. Those exceptions carry no cause, so the original error does not appear in a stack trace either. The master key failure logs also show the masked key and the error summary.
- **Other errors** reported by the same code keep their type and message, with ARNs, URL credentials (`user:password@host`) and the value of any `name=value` pair whose name contains `password`, `passwd`, `pwd`, `secret`, `token` or `credential`, or is `key`, masked. Messages are cut at 200 characters.
- **Connection URLs** that the plugin logs or puts in an exception have their user info and password parameters (any parameter whose name contains `password`, `passwd`, `pwd`, `secret`, `token` or `credential`) masked, and `ConnectionParameters.toString()` prints the names of the connection properties but not their values.
- **Control characters** (line breaks, other ISO control characters, and invisible formatting characters such as bidirectional overrides) in a key, URL or error are replaced with `?`. A `master_key_arn` that someone rewrote in `key_storage` therefore cannot forge log lines.

Masking is deliberately cautious. The user info of a URL is everything up to the last `@` before the next `?`, `#` or whitespace, so a URL whose path contains `@` (for example `jdbc:mysql://host/db@x`) is logged as `jdbc:mysql://***@x`. Hiding too much is accepted so that a password containing an unencoded `/` is never logged.

What the plugin does not mask:

- **A password written in a URL with `?`, `#` or a space in it.** These end the user info, so only the part before them is masked. Pass the password as the `password` property, or percent-encode those characters (`%3F`, `%23`, `%20`).
- **A secret in a property or parameter whose name does not look like a secret**, for example `foo=<secret>`. The plugin cannot recognize it.
- **Other values the plugin logs**: table and column names, SQL text, primary key values, the description you pass to `createMasterKey`, and errors from the database. Do not put secrets in a master key description.
- **Anything it did not write.** Log records and exceptions from the JDBC driver, the AWS SDK or your own code are not masked, and the AWS SDK can log the full request and error text when its logging is turned up. Do not enable wire or request logging for the driver or the SDK in production.
- **Values you read yourself.** `master_key_arn` and key ids that you read from the metadata tables with SQL (see Debugging) are shown in full, so do not paste them into shared logs.

## Performance Considerations

- KMS calls are only needed for data key generation/decryption, not for each data encryption/decryption
- Data key caching significantly reduces KMS API calls for repeated operations (`dataKeyCacheExpirationMs`, default 1 hour)
- The plugin caches both metadata and data keys to minimize external calls
- Rotating the KMS key has no runtime impact. Rotating a column's data key re-encrypts every row of the column and needs a maintenance window; see [Key Rotation](#key-rotation)
- KMS has rate limits that may affect very high-volume key operations
- Reuse connections through a connection pool to avoid repeated initialization
- Use batch inserts/updates when possible
- Indexes on encrypted columns are not useful: the stored values are random

## Troubleshooting

### Common Issues

1. **KMS calls fail**: Ensure your AWS credentials are configured and have `kms:Decrypt` on the key (`KeyManagementUtility` also needs `kms:GenerateDataKey`), that the KMS key is in the "Enabled" state, that `kms.region` matches the region of the key, and that the key ARN in `key_storage.master_key_arn` is correct.
2. **Metadata table not found**: Verify that the tables in the metadata schema exist and are accessible, and that `encryptionMetadataSchema` names that schema.
3. **`No encryption configuration found for ...`** (from `KeyManagementUtility`): Verify the column is registered in `encrypt.encryption_metadata`, that the table and column names match exactly (case-sensitive), and that its `key_id` references a row in `encrypt.key_storage`.
4. **`encryptionAllowedMasterKeyArns is required: list the KMS master key ARNs the KMS encryption plugin may use`**: The plugin was loaded without `encryptionAllowedMasterKeyArns`. Set it to the master keys your columns use; `SELECT DISTINCT master_key_arn FROM encrypt.key_storage` lists them. Check each one before you add it: the point of the list is that `key_storage` is not trusted.
5. **`Refusing to use master key arn:aws:kms:***:***:key/1234***abcd: it is not listed in encryptionAllowedMasterKeyArns`** (SQL state `KEY07`): The column's `key_storage` row names a master key that is not on the list. The message masks the key's region and account and the middle of its id (an alias is shown as `arn:aws:kms:***:***:alias/***`); the full value is in `key_storage.master_key_arn`. If the key is yours, add it to the list in exactly the form stored in `key_storage.master_key_arn`. If it is not, someone changed the row: investigate before you change anything.
6. **`HMAC verification failed`** (driver, on read):
   - Data may have been tampered with, or the HMAC key in `encrypt.key_storage.hmac_key` changed
   - The column's data key was rotated without re-encrypting existing rows (for example with `rotateDataKey()` in a release before it refused columns with data), or a re-encryption has not finished. See [Finishing or repairing a re-encryption](#finishing-or-repairing-a-re-encryption)
7. **`HMAC verification failed for column ...` or `Invalid encrypted data length for column ...`** (PostgreSQL trigger), **`HMAC verification failed (plaintext or tampered value)`** (MySQL trigger): The trigger rejected a plaintext or tampered value. This is expected when a value reaches the database unencrypted. Write the column through a plugin connection with a statement the plugin maps (see [Limitations](#limitations)), or use an annotation: `/*@encrypt:table.column*/`. Annotations do not bypass the parameter-mapping check described in entry 9.
   - The current HMAC triggers on both PostgreSQL and MySQL cache nothing — they look up the key from the metadata per row — so pooled sessions pick up a rotated key on their own. With a routine created by an earlier (caching) version, re-create it (see step 6 of the [Data key rotation procedure](#data-key-rotation-procedure)) or recycle the connection
8. **`No HMAC key found for ...`** (PostgreSQL trigger), **`No HMAC key is configured for the encrypted column`** (MySQL trigger): The trigger found no row for the column in `encryption_metadata`. Check that the column is registered, and that the metadata schema written into the script in [step 3](#3-install-the-encrypted-data-type-and-hmac-functions) is the one that holds the metadata.
9. **`Refusing to prepare a statement on table '...': it has encrypted columns, but the statement's parameters cannot be reliably mapped to columns (...), so a value bound for an encrypted column would not be encrypted`**: The statement writes to a table with encrypted columns, but the plugin cannot tell which parameter belongs to which column, so it cannot guarantee that the encrypted column values get encrypted. The text in parentheses names the reason, for example `INSERT without a column list` or `column ssn receives parameter(s) 2 inside an expression`. The message never contains the SQL text or parameter values. To fix it, rewrite the statement in one of the supported shapes listed in [Limitations](#limitations):
   - list the columns explicitly: `INSERT INTO users (name, ssn) VALUES (?, ?)`;
   - bind each encrypted column value with its own `?`, for example `SET ssn = ?` instead of `SET ssn = lower(?)` or `SET ssn = (SELECT ...)`;
   - use `INSERT` or `UPDATE` instead of `REPLACE` or `MERGE`, and plain `?` placeholders instead of numbered ones (`?1`).
10. **`Refusing a statement on table '...': it writes encrypted columns with values that are not bind parameters, which cannot be encrypted (...)`**: The statement writes a literal, or an expression that reads no column, into an encrypted column, for example `UPDATE users SET ssn = '123-45-6789'`. Only bind parameters can be encrypted, so the value would be stored as plaintext. The text in parentheses names each column and what it is written with, for example `column ssn is written with a literal`. Pass the value as a parameter instead: a `?` placeholder in JDBC, a `?` or named parameter with Spring's `JdbcTemplate`, or `setParameter` and entity properties with JPA and Hibernate. While the encryption metadata can be read, `encryptionFailOnUnmappedParameter` does not affect this check (see [Statements always refused](#statements-always-refused)).
11. **`... is not supported for encrypted columns. Use setString, setNString, setBytes or setObject.`**: A setter whose value cannot be encrypted (a stream or LOB setter such as `setCharacterStream`, `setBinaryStream`, `setClob` or `setBlob`, or `setURL`, `setArray`, `setRef`, `setRowId` or `setSQLXML`) was called for a parameter that maps to an encrypted column. Pass the value with one of the listed setters, converting a URL, `Array`, `Ref`, `RowId` or `SQLXML` value to a supported type first (see [Other limitations](#other-limitations)).
12. **Type `encrypted_data` not found (PostgreSQL)**: Run the `encrypted_data_type.sql` script as described in [step 3](#3-install-the-encrypted-data-type-and-hmac-functions), in the schema where your application tables live, and verify that the `pgcrypto` extension is installed.

### Debugging

Enable audit logging to track encryption operations:

```java
props.setProperty("auditLoggingEnabled", "true");
```

Enable detailed logging:

```java
// Set log level
System.setProperty("java.util.logging.config.file", "logging.properties");

// logging.properties:
// software.amazon.jdbc.plugin.encryption.level=FINEST
```

Check the metadata:

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

## Best Practices

1. **Key Management**
   - Enable automatic rotation on the KMS key (yearly by default). It is transparent to the plugin and needs no data changes
   - Rotate a column's data key only when required, with `rotateDataKeyAndReencrypt()` (see [Key Rotation](#key-rotation))
   - Never delete an old data key or KMS key while data or backups encrypted under it exist
   - Use separate KMS keys for different environments and sensitivity levels
   - Monitor key usage via CloudTrail
2. **Security**
   - Create the HMAC validation trigger on every encrypted column (see [step 7 of Setup](#7-add-hmac-validation-triggers))
   - Use IAM roles instead of hardcoded credentials or access keys
   - Apply least-privilege IAM policies
   - Run the application under a runtime role with only `kms:Decrypt` and `SELECT` on the metadata schema, and keep a separate admin credential for `KeyManagementUtility` (see [Separating the runtime role from the admin role](#separating-the-runtime-role-from-the-admin-role))
   - Restrict access to the metadata schema, and regularly audit the encryption metadata
3. **Performance**
   - Use connection pooling
   - Configure appropriate cache TTLs
   - Minimize encrypted columns (only sensitive data)
   - Test the performance impact in your specific use case
4. **Operations**
   - Rehearse data key rotation, including the repair pass and a restore from backup, in non-production first
   - Include the encryption metadata tables in backups
   - Document which columns are encrypted
   - Monitor KMS API usage and costs

## Database Differences Summary

| Feature | PostgreSQL | MySQL |
|---------|------------|-------|
| Encrypted column type | `encrypted_data` (custom DOMAIN) | `VARBINARY(256)` |
| Primary key type | `SERIAL` | `INT AUTO_INCREMENT` |
| Binary data type | `BYTEA` | `VARBINARY` |
| Timestamp type | `TIMESTAMPTZ` | `TIMESTAMP` |
| HMAC function | Native `public.hmac()` via pgcrypto | Pure SQL implementation |
| Custom type validation | Domain CHECK constraint | None (HMAC trigger only) |
| Auto-update timestamp | Manual | `ON UPDATE CURRENT_TIMESTAMP` |

## Example Application

Complete example projects:

- [EncryptionExample](../../../examples/EncryptionExample): plain JDBC examples.
  - [AwsWrapperEncryptionExample.java](../../../examples/EncryptionExample/src/main/java/software/amazon/AwsWrapperEncryptionExample.java) - configuring and using encrypted connections
  - [PropertiesFileExample.java](../../../examples/EncryptionExample/src/main/java/software/amazon/PropertiesFileExample.java) - configuring the plugin from a properties file
  - [DataSourceLifecycleExample.java](../../../examples/EncryptionExample/src/main/java/software/amazon/DataSourceLifecycleExample.java) - managing a `DataSource` that uses the plugin
  - [KeyManagementExample.java](../../../examples/EncryptionExample/src/main/java/software/amazon/KeyManagementExample.java) - setting up columns, rotating data keys and other `KeyManagementUtility` tasks
- [EncryptSpring](../../../examples/EncryptSpring): a Spring Boot application with an encrypted column. See its [README](../../../examples/EncryptSpring/README.md).

## Additional Resources

- [AWS KMS Documentation](https://docs.aws.amazon.com/kms/)
- [Envelope Encryption](https://docs.aws.amazon.com/kms/latest/developerguide/concepts.html#enveloping)
- [PostgreSQL Domains](https://www.postgresql.org/docs/current/domains.html)
- [AES-GCM Encryption](https://en.wikipedia.org/wiki/Galois/Counter_Mode)
- [AWS JDBC Wrapper Documentation](../../Documentation.md)
- [JSQLParser Documentation](https://github.com/JSQLParser/JSqlParser)
