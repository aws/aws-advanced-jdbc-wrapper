# PostgreSQL encrypted_data Custom Type

This directory contains SQL scripts for creating a PostgreSQL custom type that provides HMAC verification for encrypted data at the database level.

## Overview

The `encrypted_data` type stores encrypted data with a random salt and HMAC tag prepended, allowing verification of data integrity in the database using only the encryption key.

**Format:** `[salt:16bytes][HMAC:32bytes][type:1byte][IV:12bytes][ciphertext]`

## Features

- **Random salt per encryption** - Each encrypted value has a unique salt
- **Database-level HMAC verification** - Verify integrity using only the encryption key
- **Automatic format validation** on INSERT/UPDATE
- **HMAC structure verification** without requiring any key
- **Full HMAC verification** with encryption key (no separate verification key needed)
- **Base64 encoding** for text representation
- **Binary storage** for efficiency

## Installation

### Using Java API

```java
import software.amazon.jdbc.plugin.encryption.schema.EncryptedDataTypeInstaller;

// Install the type
EncryptedDataTypeInstaller.installEncryptedDataType(connection);

// Check if already installed
boolean installed = EncryptedDataTypeInstaller.isEncryptedDataTypeInstalled(connection);
```

### Using SQL

```sql
-- Enable required extension
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Run the script
\i encrypted_data_type.sql
```

## Usage

### Create Table

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    ssn encrypted_data,
    credit_card encrypted_data
);
```

### Insert Data

```sql
-- Data must be base64-encoded with salt + HMAC prepended
INSERT INTO users (name, ssn)
VALUES ('John Doe', 'base64_encoded_salt_hmac_and_encrypted_data'::encrypted_data);
```

### Verify HMAC Structure

```sql
-- Check if data has valid HMAC structure (doesn't require key)
SELECT name, has_valid_hmac_structure(ssn) as valid_structure
FROM users;
```

### Verify HMAC with Encryption Key

```sql
-- Verify HMAC authenticity using encryption key
-- The function extracts the salt and derives the verification key automatically
SELECT name, verify_encrypted_data_hmac(ssn, encryption_key_bytes) as hmac_valid
FROM users;
```

## Functions

### `encrypted_data_in(cstring)`
Input function that validates format when data is inserted. Checks for minimum 61 bytes (16 salt + 32 HMAC + 13 minimum encrypted data).

### `encrypted_data_out(encrypted_data)`
Output function that converts to base64 string.

### `has_valid_hmac_structure(encrypted_data)`
Returns `true` if data has valid HMAC structure (minimum 61 bytes).

### `verify_encrypted_data_hmac(encrypted_data, bytea)`
Verifies HMAC using the provided **encryption key** (not verification key). The function:
1. Extracts the salt from the encrypted data
2. Derives the verification key using HMAC(salt, encryption_key)
3. Verifies the HMAC tag
Returns `true` if HMAC is valid.

## How It Works

### Encryption (Java)
1. Generate random 16-byte salt
2. Derive verification key: `HMAC-SHA256(salt, encryption_key)`
3. Encrypt data with AES-GCM
4. Calculate HMAC: `HMAC-SHA256(encrypted_data, verification_key)`
5. Store: `[salt][HMAC][encrypted_data]`

### Verification (PostgreSQL)
1. Extract salt from encrypted data
2. Derive verification key: `HMAC-SHA256(salt, encryption_key)`
3. Calculate HMAC: `HMAC-SHA256(encrypted_data, verification_key)`
4. Compare with stored HMAC

## Requirements

- PostgreSQL 12 or higher
- `pgcrypto` extension (for HMAC verification)

## Security Notes

1. **Random salt** ensures each encryption is unique, even for identical plaintext
2. **Encryption key** is all that's needed for verification (no separate verification key)
3. **Salt is not secret** - it's stored with the encrypted data
4. **HMAC verification** at database level provides tamper detection before data reaches application
5. **Actual decryption** still requires the application layer with proper key management

## Uninstallation

```java
EncryptedDataTypeInstaller.uninstallEncryptedDataType(connection);
```

Or via SQL:
```sql
DROP TYPE IF EXISTS encrypted_data CASCADE;
```

**Warning:** Uninstallation will fail if any tables are using the type.
