# Database Encryption Setup Guide

This guide outlines the steps required to prepare a PostgreSQL database for encryption using the AWS Advanced JDBC Wrapper KMS Encryption Plugin.

## Prerequisites

- PostgreSQL database (Aurora PostgreSQL or RDS PostgreSQL)
- AWS KMS key ARN
- AWS credentials with permissions to:
  - Generate data keys from KMS
  - Decrypt data keys
- PostgreSQL `pgcrypto` extension available

## Setup Steps

### 1. Create Metadata Schema

Create a dedicated schema to store encryption metadata:

```sql
CREATE SCHEMA encrypt;
```

**Note:** The default schema name is `encrypt`, but this can be configured using the `encryptionMetadataSchema` connection property.

### 2. Install pgcrypto Extension

Required for HMAC verification functions:

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;
```

### 3. Install encrypted_data Custom Type

The `encrypted_data` type is a PostgreSQL DOMAIN over BYTEA with validation functions. Install it using:

**Java:**
```java
import software.amazon.jdbc.plugin.encryption.schema.EncryptedDataTypeInstaller;

Connection conn = DriverManager.getConnection(url, props);
EncryptedDataTypeInstaller.installEncryptedDataType(conn, "encrypt");
```

**Or manually via SQL:**
```sql
-- Create the domain
DROP DOMAIN IF EXISTS encrypted_data CASCADE;
CREATE DOMAIN encrypted_data AS bytea CHECK (length(VALUE) >= 45);

-- Create HMAC verification function
CREATE OR REPLACE FUNCTION verify_encrypted_data_hmac(
    data encrypted_data,
    hmac_key bytea
) RETURNS boolean AS $$
DECLARE
    data_bytes bytea := data::bytea;
    stored_hmac bytea;
    encrypted_payload bytea;
    calculated_hmac bytea;
BEGIN
    stored_hmac := substring(data_bytes from 1 for 32);
    encrypted_payload := substring(data_bytes from 33);
    calculated_hmac := hmac(encrypted_payload, hmac_key, 'sha256');
    RETURN stored_hmac = calculated_hmac;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Create structure validation function
CREATE OR REPLACE FUNCTION has_valid_hmac_structure(data encrypted_data)
RETURNS boolean AS $$
BEGIN
    RETURN length(data::bytea) >= 45;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Create trigger function for automatic HMAC validation
CREATE OR REPLACE FUNCTION validate_encrypted_data_hmac()
RETURNS trigger AS $$
DECLARE
    metadata_schema text := 'encrypt';
    col_name text := TG_ARGV[0];
    col_value encrypted_data;
    hmac_key bytea;
    data_bytes bytea;
    stored_hmac bytea;
    encrypted_payload bytea;
    calculated_hmac bytea;
    cache_key text;
BEGIN
    EXECUTE format('SELECT ($1).%I', col_name) INTO col_value USING NEW;
    
    IF col_value IS NOT NULL THEN
        cache_key := 'hmac_key.' || TG_TABLE_NAME || '.' || col_name;
        BEGIN
            hmac_key := decode(current_setting(cache_key), 'hex');
        EXCEPTION WHEN OTHERS THEN
            EXECUTE format(
                'SELECT ks.hmac_key FROM %I.encryption_metadata em ' ||
                'JOIN %I.key_storage ks ON em.key_id = ks.id ' ||
                'WHERE em.table_name = $1 AND em.column_name = $2',
                metadata_schema, metadata_schema
            ) INTO hmac_key USING TG_TABLE_NAME, col_name;
            
            IF hmac_key IS NULL THEN
                RAISE EXCEPTION 'No HMAC key found for %.%', TG_TABLE_NAME, col_name;
            END IF;
            
            PERFORM set_config(cache_key, encode(hmac_key, 'hex'), false);
        END;
        
        data_bytes := col_value::bytea;
        
        IF length(data_bytes) < 45 THEN
            RAISE EXCEPTION 'Invalid encrypted data length for column %', col_name;
        END IF;
        
        stored_hmac := substring(data_bytes from 1 for 32);
        encrypted_payload := substring(data_bytes from 33);
        calculated_hmac := hmac(encrypted_payload, hmac_key, 'sha256');
        
        IF stored_hmac != calculated_hmac THEN
            RAISE EXCEPTION 'HMAC verification failed for column %. Stored: %, Calculated: %', 
                col_name,
                encode(stored_hmac, 'hex'),
                encode(calculated_hmac, 'hex');
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

### 4. Create Key Storage Table

Store encryption keys and metadata:

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

**Columns:**
- `name`: Unique identifier for the key (e.g., "users.ssn")
- `master_key_arn`: AWS KMS key ARN used to encrypt the data key
- `encrypted_data_key`: Base64-encoded encrypted data key from KMS
- `hmac_key`: 32-byte HMAC key for data integrity verification
- `key_spec`: KMS key specification (AES_128, AES_256)

### 5. Create Encryption Metadata Table

Track which columns are encrypted:

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

### 6. Generate and Store Encryption Keys

For each column you want to encrypt:

**Java:**
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

// Store in database
PreparedStatement stmt = conn.prepareStatement(
    "INSERT INTO encrypt.key_storage " +
    "(name, master_key_arn, encrypted_data_key, hmac_key, key_spec) " +
    "VALUES (?, ?, ?, ?, ?) RETURNING id"
);
stmt.setString(1, "users.ssn");
stmt.setString(2, kmsKeyArn);
stmt.setString(3, encryptedDataKey);
stmt.setBytes(4, hmacKey);
stmt.setString(5, "AES_256");
ResultSet rs = stmt.executeQuery();
rs.next();
int keyId = rs.getInt(1);
```

### 7. Register Encrypted Columns

Add metadata for each encrypted column:

```sql
INSERT INTO encrypt.encryption_metadata 
    (table_name, column_name, encryption_algorithm, key_id)
VALUES 
    ('users', 'ssn', 'AES-256-GCM', 1);
```

### 8. Create Application Tables

Define tables with `encrypted_data` type for encrypted columns:

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    ssn encrypted_data,  -- Encrypted column
    email VARCHAR(100)
);
```

### 9. Add HMAC Validation Triggers

Automatically validate HMAC on insert/update:

```sql
CREATE TRIGGER validate_ssn_hmac
    BEFORE INSERT OR UPDATE ON users
    FOR EACH ROW 
    EXECUTE FUNCTION validate_encrypted_data_hmac('ssn');
```

**Note:** The trigger function parameter is the column name to validate.

### 10. Configure JDBC Connection

Set up the connection with encryption plugin:

```java
Properties props = new Properties();
props.setProperty("user", "postgres");
props.setProperty("password", "password");
props.setProperty("wrapperPlugins", "kmsEncryption");
props.setProperty("kmsKeyArn", "arn:aws:kms:us-east-1:123456789012:key/...");
props.setProperty("kmsRegion", "us-east-1");
props.setProperty("encryptionMetadataSchema", "encrypt");  // Optional, defaults to "encrypt"

String url = "jdbc:aws-wrapper:postgresql://host:5432/database";
Connection conn = DriverManager.getConnection(url, props);
```

## Encrypted Data Format

The encrypted data uses the following binary format:

```
[HMAC:32 bytes][Type:1 byte][IV:12 bytes][Ciphertext:variable]
```

- **HMAC (32 bytes)**: HMAC-SHA256 tag for integrity verification
- **Type (1 byte)**: Data type indicator (0x01 = String, 0x02 = Integer, etc.)
- **IV (12 bytes)**: Initialization vector for AES-GCM
- **Ciphertext**: AES-256-GCM encrypted data

Minimum length: 45 bytes (32 + 1 + 12 + 16-byte auth tag)

## Usage Example

Once configured, encryption is transparent:

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

In addition to metadata-based encryption, you can use SQL comment annotations to explicitly mark parameters for encryption. This is useful when:
- You want explicit control over which parameters are encrypted
- Column order varies in different queries
- You're working with dynamic SQL

### Annotation Format

Use SQL comments with the format `/*@encrypt:table.column*/` before the parameter placeholder:

```java
// Basic annotation
PreparedStatement stmt = conn.prepareStatement(
    "INSERT INTO users (name, ssn, email) VALUES (?, /*@encrypt:users.ssn*/ ?, ?)"
);
stmt.setString(1, "Bob");
stmt.setString(2, "987-65-4321");  // Encrypted via annotation
stmt.setString(3, "bob@example.com");
stmt.executeUpdate();

// Update with annotation
PreparedStatement updateStmt = conn.prepareStatement(
    "UPDATE users SET ssn = /*@encrypt:users.ssn*/ ? WHERE name = ?"
);
updateStmt.setString(1, "111-22-3333");  // Encrypted
updateStmt.setString(2, "Bob");
updateStmt.executeUpdate();

// Different column order
PreparedStatement stmt2 = conn.prepareStatement(
    "INSERT INTO users (name, email, ssn) VALUES (?, ?, /*@encrypt:users.ssn*/ ?)"
);
stmt2.setString(1, "Carol");
stmt2.setString(2, "carol@example.com");
stmt2.setString(3, "555-66-7777");  // Encrypted even though it's the 3rd parameter
stmt2.executeUpdate();
```

### Annotation vs Metadata

**Annotations:**
- Explicit control in SQL
- Works regardless of column position
- Useful for ad-hoc queries
- Requires modifying SQL statements

**Metadata (recommended):**
- Centralized configuration
- No SQL changes needed
- Automatic detection based on column name
- Easier to maintain

Both approaches can be used together. Annotations take precedence when present.

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

### Type Not Found
- Run `EncryptedDataTypeInstaller.installEncryptedDataType()`
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
