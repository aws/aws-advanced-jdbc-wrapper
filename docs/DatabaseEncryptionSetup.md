# Database Encryption Setup Guide

This guide outlines the steps required to prepare a PostgreSQL or MySQL database for encryption using the AWS Advanced JDBC Wrapper KMS Encryption Plugin.

## Prerequisites

- PostgreSQL (Aurora PostgreSQL or RDS PostgreSQL) or MySQL (Aurora MySQL or RDS MySQL) database
- AWS KMS key ARN
- AWS credentials with permissions to generate and decrypt data keys
- PostgreSQL: `pgcrypto` extension available

## Setup Steps

### 1. Create Metadata Schema

Create a dedicated schema to store encryption metadata:

```sql
CREATE SCHEMA encrypt;
```

**Note:** The default schema name is `encrypt`, but this can be configured using the `encryptionMetadataSchema` connection property.

### 2. Install Database-Specific Extensions

**PostgreSQL only** — required for HMAC verification functions:

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;
```

MySQL does not require any extensions; the plugin includes a pure SQL HMAC-SHA256 implementation.

### 3. Install Encrypted Data Type and HMAC Functions

The database objects are defined in SQL scripts in the driver's source repository. The released driver jar does not contain an installer for them. Take the scripts from the release tag that matches your driver version:

- PostgreSQL: [`wrapper/src/test/resources/sql/encrypted_data_type.sql`](../wrapper/src/test/resources/sql/encrypted_data_type.sql)
- MySQL: [`wrapper/src/test/resources/sql/encrypted_data_type_mysql.sql`](../wrapper/src/test/resources/sql/encrypted_data_type_mysql.sql)

Replace every `SCHEMA_NAME` in the script with your metadata schema (default `encrypt`, see step 1), then run it against the application database.

**PostgreSQL** creates the `encrypted_data` DOMAIN type (a `BYTEA` with a minimum length check), the `has_valid_hmac_structure()` function and the `validate_encrypted_data_hmac()` trigger function. Run it in the schema where your application tables live, so that `encrypted_data` resolves in step 7:

```bash
sed 's/SCHEMA_NAME/encrypt/g' encrypted_data_type.sql | psql -h host -U dbuser -d database -v ON_ERROR_STOP=1
```

> [!WARNING]\
> The PostgreSQL script starts with `DROP DOMAIN IF EXISTS encrypted_data CASCADE`. Running it again after you created tables drops every column of type `encrypted_data`. Run it only once, before step 7.

**MySQL** creates the HMAC functions and the `validate_encrypted_data_hmac_before_insert` procedure used by the triggers in step 8. The script uses `DELIMITER`, so run it with the `mysql` command-line client rather than through JDBC:

```bash
sed 's/SCHEMA_NAME/encrypt/g' encrypted_data_type_mysql.sql | mysql -h host -u dbuser -p database
```

MySQL does not support DOMAINs. Encrypted columns use `VARBINARY(256)` instead (see step 7), so MySQL has no type-level check that a column holds encrypted data.

### 4. Create Key Storage Table

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

**Key differences:** PostgreSQL uses `SERIAL`, `BYTEA`, and `TIMESTAMPTZ`; MySQL uses `INT AUTO_INCREMENT`, `VARBINARY(32)`, and `TIMESTAMP`.

### 5. Create Encryption Metadata Table

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

### 6. Generate and Store Encryption Keys

For each column you want to encrypt:

```java
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.*;
import java.security.SecureRandom;
import java.util.Base64;

// Generate data key from KMS
KmsClient kmsClient = KmsClient.builder().region(Region.of("us-east-1")).build();
GenerateDataKeyResponse response = kmsClient.generateDataKey(
    GenerateDataKeyRequest.builder()
        .keyId("arn:aws:kms:us-east-1:123456789012:key/...")
        .keySpec(DataKeySpec.AES_256)
        .build()
);

String encryptedDataKey = Base64.getEncoder()
    .encodeToString(response.ciphertextBlob().asByteArray());

// Generate separate HMAC key
byte[] hmacKey = new byte[32];
new SecureRandom().nextBytes(hmacKey);

// Store in database (PostgreSQL uses RETURNING id; MySQL uses getGeneratedKeys)
PreparedStatement stmt = conn.prepareStatement(
    "INSERT INTO encrypt.key_storage " +
    "(name, master_key_arn, encrypted_data_key, hmac_key, key_spec) " +
    "VALUES (?, ?, ?, ?, ?)",
    Statement.RETURN_GENERATED_KEYS
);
stmt.setString(1, "users.ssn");
stmt.setString(2, kmsKeyArn);
stmt.setString(3, encryptedDataKey);
stmt.setBytes(4, hmacKey);
stmt.setString(5, "AES_256");
stmt.executeUpdate();
ResultSet rs = stmt.getGeneratedKeys();
rs.next();
int keyId = rs.getInt(1);
```

Register the encrypted column:

```sql
INSERT INTO encrypt.encryption_metadata 
    (table_name, column_name, encryption_algorithm, key_id)
VALUES 
    ('users', 'ssn', 'AES-256-GCM', 1);
```

### 7. Create Application Tables

**PostgreSQL** — use the `encrypted_data` custom type:
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    ssn encrypted_data,
    email VARCHAR(100)
);
```

**MySQL** — use `VARBINARY` for encrypted columns:
```sql
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    ssn VARBINARY(256),
    email VARCHAR(100)
);
```

### 8. Add HMAC Validation Triggers (Recommended)

HMAC validation triggers check on every insert and update that the value written to an encrypted column carries a valid HMAC for the column's key. They reject plaintext and tampered values on the database side, so they act as a fail-closed backstop when a value reaches the database unencrypted: for example, written by a client that does not use the plugin, or by a statement the plugin was configured not to reject (`encryptionFailOnUnmappedParameter=false`). Add a trigger for every encrypted column.

On PostgreSQL the `encrypted_data` domain only checks the minimum length. On MySQL there is no domain-level type check at all, so the trigger is the only database-side guard.

**PostgreSQL:**
```sql
CREATE TRIGGER validate_ssn_hmac
    BEFORE INSERT OR UPDATE ON users
    FOR EACH ROW 
    EXECUTE FUNCTION validate_encrypted_data_hmac('ssn');
```

**MySQL:**
```sql
DELIMITER $$
CREATE TRIGGER validate_ssn_hmac_insert
BEFORE INSERT ON users
FOR EACH ROW
BEGIN
    CALL validate_encrypted_data_hmac_before_insert('users', 'ssn', NEW.ssn);
END$$

CREATE TRIGGER validate_ssn_hmac_update
BEFORE UPDATE ON users
FOR EACH ROW
BEGIN
    CALL validate_encrypted_data_hmac_before_insert('users', 'ssn', NEW.ssn);
END$$
DELIMITER ;
```

| Feature | PostgreSQL | MySQL |
|---------|------------|-------|
| Native HMAC | `hmac()` via pgcrypto | Pure SQL implementation |
| Trigger Syntax | Single trigger for INSERT/UPDATE | Separate triggers needed |
| Session Caching | `current_setting()` | User variables (`@var`) |

**Recommendation:** Create the triggers in all deployments. The driver validates the HMAC when it reads and decrypts a value, but only a trigger stops an unencrypted value from being stored.

### 9. Configure JDBC Connection

The connection configuration is the same for both PostgreSQL and MySQL:

```java
Properties props = new Properties();
props.setProperty("user", "dbuser");
props.setProperty("password", "password");
props.setProperty("wrapperPlugins", "sqlParser,kmsEncryption");  // sqlParser is required
props.setProperty("kmsKeyArn", "arn:aws:kms:us-east-1:123456789012:key/...");
props.setProperty("kmsRegion", "us-east-1");
props.setProperty("encryptionMetadataSchema", "encrypt");  // Optional, defaults to "encrypt"

// PostgreSQL
String pgUrl = "jdbc:aws-wrapper:postgresql://host:5432/database";

// MySQL
String mysqlUrl = "jdbc:aws-wrapper:mysql://host:3306/database";

Connection conn = DriverManager.getConnection(pgUrl, props);  // or mysqlUrl
```

## Encrypted Data Format

The encryption system uses a two-key approach that separates data encryption from integrity verification:

1. **Data Encryption Key (DEK)** — Used for encrypting/decrypting the actual data. Stored encrypted by AWS KMS; only decrypted on the client side when needed.
2. **HMAC Verification Key** — Used for verifying data integrity without decryption. Stored in plaintext in the metadata table so database triggers can validate data without accessing the encryption key.

Even if the HMAC key is compromised, the encrypted data remains secure.

### Binary Format

```
[HMAC:32 bytes][Type:1 byte][IV:12 bytes][Ciphertext:variable]
```

- **HMAC (32 bytes)**: HMAC-SHA256 tag computed over `[Type][IV][Ciphertext]` using the HMAC key
- **Type (1 byte)**: Data type indicator (0x01 = String, 0x02 = Integer, etc.)
- **IV (12 bytes)**: Initialization vector for AES-GCM
- **Ciphertext**: AES-256-GCM encrypted data

Minimum length: 45 bytes (32 + 1 + 12 + 16-byte auth tag)

### API Usage

```java
EncryptionService service = new EncryptionService();

byte[] dataKey = ...; // KMS-encrypted key, decrypted on client
byte[] hmacKey = ...; // Plaintext key from metadata

// Encrypt with both keys
byte[] encrypted = service.encrypt("sensitive data", dataKey, hmacKey, "AES-256-GCM");

// Decrypt with both keys (HMAC verified before decryption)
String decrypted = (String) service.decrypt(encrypted, dataKey, hmacKey, "AES-256-GCM", String.class);

// Verify integrity without decryption
boolean isValid = service.verifyEncryptedData(encrypted, hmacKey);
```

## Usage Example

Once configured, encryption is transparent for both PostgreSQL and MySQL:

```java
// Insert - automatic encryption
PreparedStatement stmt = conn.prepareStatement(
    "INSERT INTO users (name, ssn, email) VALUES (?, ?, ?)"
);
stmt.setString(1, "Alice");
stmt.setString(2, "123-45-6789");  // Automatically encrypted
stmt.setString(3, "alice@example.com");
stmt.executeUpdate();

// Query - automatic decryption
ResultSet rs = conn.createStatement()
    .executeQuery("SELECT name, ssn FROM users");
while (rs.next()) {
    String name = rs.getString("name");
    String ssn = rs.getString("ssn");  // Automatically decrypted
    System.out.println(name + ": " + ssn);
}
```

## SQL Annotation Syntax

In addition to metadata-based encryption, you can use SQL comment annotations to explicitly mark parameters for encryption:

```java
PreparedStatement stmt = conn.prepareStatement(
    "INSERT INTO users (name, ssn, email) VALUES (?, /*@encrypt:users.ssn*/ ?, ?)"
);
stmt.setString(1, "Bob");
stmt.setString(2, "987-65-4321");  // Encrypted via annotation
stmt.setString(3, "bob@example.com");
stmt.executeUpdate();
```

Annotations take precedence over metadata-based encryption when present. Both approaches can be used together.

## Database Differences Summary

| Feature | PostgreSQL | MySQL |
|---------|------------|-------|
| Encrypted column type | `encrypted_data` (custom DOMAIN) | `VARBINARY(256)` |
| Primary key type | `SERIAL` | `INT AUTO_INCREMENT` |
| Binary data type | `BYTEA` | `VARBINARY` |
| Timestamp type | `TIMESTAMPTZ` | `TIMESTAMP` |
| HMAC function | Native `hmac()` via pgcrypto | Pure SQL implementation |
| Custom type validation | Domain CHECK constraint | Application-level only |
| Auto-update timestamp | Manual | `ON UPDATE CURRENT_TIMESTAMP` |

## Security Considerations

1. **Key Rotation**: Periodically rotate KMS keys and re-encrypt data
2. **Access Control**: Restrict access to `encrypt` schema tables
3. **Audit Logging**: Enable CloudTrail for KMS operations
4. **HMAC Keys**: Store HMAC keys securely, consider encrypting them at rest
5. **Connection Security**: Always use SSL/TLS for database connections
6. **IAM Permissions**: Use least-privilege IAM policies for KMS access

## Troubleshooting

### HMAC Verification Failed
- Ensure HMAC key matches the one used during encryption
- Check that data wasn't corrupted during storage/retrieval
- Verify trigger is using correct metadata schema

### Type Not Found (PostgreSQL)
- Run the `encrypted_data_type.sql` script as described in [step 3](#3-install-encrypted-data-type-and-hmac-functions)
- Verify `pgcrypto` extension is installed
- Check that domain was created in correct schema

### Key Not Found
- Verify encryption metadata exists for the column
- Check foreign key relationship between metadata and key_storage
- Ensure key_id references valid row in key_storage

## References

- [AWS KMS Documentation](https://docs.aws.amazon.com/kms/)
- [PostgreSQL Domains](https://www.postgresql.org/docs/current/domains.html)
- [AES-GCM Encryption](https://en.wikipedia.org/wiki/Galois/Counter_Mode)
