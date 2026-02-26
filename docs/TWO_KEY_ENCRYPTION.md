# Two-Key Encryption System

## Overview

The encryption system now supports a two-key approach that separates data encryption from integrity verification:

1. **Data Encryption Key (DEK)** - Used for encrypting/decrypting the actual data
2. **HMAC Verification Key** - Used for verifying data integrity without decryption

## Architecture

### Key Storage

- **Data Encryption Key**: Stored encrypted by AWS KMS in the metadata table
  - Only decrypted on the client side when needed for encryption/decryption operations
  - Never stored in plaintext in the database
  
- **HMAC Verification Key**: Stored in plaintext in the metadata table
  - Used by database triggers/constraints to verify data is properly encrypted
  - Does not provide access to the actual encrypted data
  - Can be safely exposed to database-level operations

### Encrypted Data Format

```
[HMAC:32bytes][type:1byte][IV:12bytes][ciphertext:variable]
```

- **HMAC (32 bytes)**: HMAC-SHA256 tag computed over `[type:1byte][IV:12bytes][ciphertext]` using the HMAC key
- **Type marker (1 byte)**: Indicates the data type (String, Integer, etc.)
- **IV (12 bytes)**: Initialization vector for AES-GCM
- **Ciphertext (variable)**: The encrypted data

## Benefits

### Security Separation

- **Encryption key** remains secure and is only accessible to authorized clients
- **HMAC key** can be used by database for verification without exposing encrypted data
- Even if HMAC key is compromised, data remains encrypted and secure

### Database-Level Verification

Database triggers and constraints can verify that:
- Data is properly encrypted (has valid HMAC structure)
- Data has not been tampered with (HMAC verification passes)
- All this without needing access to the encryption key

### Example Use Cases

1. **CHECK Constraint**: Verify column contains encrypted data
   ```sql
   ALTER TABLE users ADD CONSTRAINT check_ssn_encrypted 
   CHECK (verify_encrypted_data_hmac(ssn, hmac_key));
   ```

2. **Trigger Validation**: Prevent insertion of unencrypted data
   ```sql
   CREATE TRIGGER validate_encrypted_data
   BEFORE INSERT OR UPDATE ON users
   FOR EACH ROW
   EXECUTE FUNCTION validate_encrypted_data_hmac();
   ```

## API Usage

### Encrypting Data with Two Keys

```java
EncryptionService service = new EncryptionService();

// Separate keys
byte[] dataKey = ...; // KMS-encrypted key, decrypted on client
byte[] hmacKey = ...; // Plaintext key from metadata

// Encrypt with both keys
byte[] encrypted = service.encrypt(
    "sensitive data",
    dataKey,
    hmacKey,
    "AES-256-GCM"
);
```

### Decrypting Data with Two Keys

```java
// Decrypt with both keys
String decrypted = (String) service.decrypt(
    encrypted,
    dataKey,
    hmacKey,
    "AES-256-GCM",
    String.class
);
```

### Verifying Data Without Decryption

```java
// Verify using only HMAC key (no decryption)
boolean isValid = service.verifyEncryptedData(encrypted, hmacKey);
```

### Backward Compatibility

For existing code that uses a single key:

```java
// Single key for both encryption and HMAC
byte[] key = ...;

// These methods use the same key for both operations
byte[] encrypted = service.encrypt("data", key, "AES-256-GCM");
String decrypted = (String) service.decrypt(encrypted, key, "AES-256-GCM", String.class);
```

## Migration Strategy

### For New Deployments

1. Generate two separate keys:
   - Data encryption key (encrypt with KMS)
   - HMAC verification key (store in plaintext)

2. Store both keys in metadata table:
   ```sql
   INSERT INTO encrypt.encryption_metadata (
       table_name, column_name, 
       encrypted_data_key, hmac_key,
       algorithm
   ) VALUES (
       'users', 'ssn',
       '<kms-encrypted-key>', '<plaintext-hmac-key>',
       'AES-256-GCM'
   );
   ```

3. Use two-key API methods for all operations

### For Existing Deployments

Option 1: Continue using single-key mode (backward compatible)
- No changes required
- Use existing API methods

Option 2: Migrate to two-key mode
1. Add `hmac_key` column to metadata table
2. Generate HMAC keys for existing entries
3. Re-encrypt existing data with new format
4. Update application code to use two-key API

## Security Considerations

### HMAC Key Security

While the HMAC key is stored in plaintext:
- It only verifies data integrity, not confidentiality
- Compromising it does not expose encrypted data
- It prevents unauthorized modification of encrypted data
- Should still be protected with appropriate database access controls

### Data Encryption Key Security

The data encryption key must remain secure:
- Always encrypted at rest using AWS KMS
- Only decrypted in application memory when needed
- Never logged or stored in plaintext
- Rotated periodically according to security policy

## Performance

- **Encryption**: Minimal overhead from separate HMAC computation
- **Decryption**: HMAC verification happens before decryption (fail fast)
- **Verification**: Fast HMAC-only check without decryption overhead
- **Storage**: 32 bytes HMAC overhead per encrypted value (no salt needed)

## Comparison with Previous Implementation

### Previous (Single Key with Salt)

```
[salt:16bytes][HMAC:32bytes][type:1byte][IV:12bytes][ciphertext]
```
- Total overhead: 48 bytes + ciphertext
- HMAC key derived from encryption key using salt
- Database verification requires encryption key

### Current (Two Keys)

```
[HMAC:32bytes][type:1byte][IV:12bytes][ciphertext]
```
- Total overhead: 32 bytes + ciphertext (16 bytes saved)
- HMAC key separate from encryption key
- Database verification uses plaintext HMAC key
- Better security separation
