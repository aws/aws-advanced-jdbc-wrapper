# HMAC Validation Triggers: PostgreSQL vs MySQL

## Overview

The encryption plugin uses HMAC (Hash-based Message Authentication Code) to verify data integrity. Database triggers can optionally validate HMAC before INSERT/UPDATE operations.

## PostgreSQL Implementation

**File:** `sql/encrypted_data_type.sql`

**Features:**
- Native `hmac()` function available
- Trigger function: `validate_encrypted_data_hmac()`
- Session-level caching of HMAC keys
- Automatic validation on INSERT/UPDATE

**Usage:**
```sql
CREATE TRIGGER users_ssn_hmac_check
BEFORE INSERT OR UPDATE ON users
FOR EACH ROW 
EXECUTE FUNCTION validate_encrypted_data_hmac('ssn');
```

## MySQL Implementation

**File:** `sql/encrypted_data_type_mysql.sql`

**Limitations:**
- MySQL < 8.0 lacks native HMAC functions
- Requires stored procedure approach
- No session-level variable caching like PostgreSQL
- More verbose trigger syntax

**Usage:**
```sql
DELIMITER $$
CREATE TRIGGER users_ssn_hmac_check
BEFORE INSERT ON users
FOR EACH ROW
BEGIN
    CALL validate_encrypted_data_hmac_before_insert('users', 'ssn', NEW.ssn);
END$$

CREATE TRIGGER users_ssn_hmac_check_update
BEFORE UPDATE ON users
FOR EACH ROW
BEGIN
    CALL validate_encrypted_data_hmac_before_insert('users', 'ssn', NEW.ssn);
END$$
DELIMITER ;
```

## Key Differences

| Feature | PostgreSQL | MySQL |
|---------|------------|-------|
| Native HMAC | ✅ `hmac()` function | ✅ Pure SQL implementation |
| Trigger Syntax | Single trigger for INSERT/UPDATE | Separate triggers needed |
| Session Caching | ✅ `current_setting()` | ✅ User variables (`@var`) |
| Performance | Better (native HMAC) | Good (pure SQL HMAC) |

## Recommendations

### For MySQL Users

**Option 1: Application-Level Validation (Recommended)**
- HMAC validation happens in JDBC driver (current implementation)
- No database triggers needed
- Works consistently across PostgreSQL and MySQL
- Better performance (no trigger overhead)
- **This is the default and recommended approach**

**Option 2: MariaDB 10.5+**
- Use MariaDB which has native HMAC support
- Better compatibility with PostgreSQL approach
- Only if you specifically need database-level validation

**Option 3: Skip Database-Level Validation**
- Rely on application-level validation only (same as Option 1)
- Simpler deployment
- Adequate for most use cases

## Current JDBC Driver Behavior

The AWS Advanced JDBC Wrapper **already validates HMAC at the application level** in:
- `EncryptionService.decrypt()` - validates HMAC before decryption
- `DecryptingResultSet` - validates on data retrieval

**Database triggers are optional** and provide defense-in-depth but are not required for security.

## Migration Guide

### PostgreSQL → MySQL

If you have PostgreSQL triggers:
```sql
-- PostgreSQL
CREATE TRIGGER check_ssn BEFORE INSERT OR UPDATE ON users
FOR EACH ROW EXECUTE FUNCTION validate_encrypted_data_hmac('ssn');
```

Convert to MySQL:
```sql
-- MySQL
DELIMITER $$
CREATE TRIGGER check_ssn_insert BEFORE INSERT ON users
FOR EACH ROW BEGIN
    CALL validate_encrypted_data_hmac_before_insert('users', 'ssn', NEW.ssn);
END$$

CREATE TRIGGER check_ssn_update BEFORE UPDATE ON users
FOR EACH ROW BEGIN
    CALL validate_encrypted_data_hmac_before_insert('users', 'ssn', NEW.ssn);
END$$
DELIMITER ;
```

### Recommendation

**For new deployments:** Skip database triggers and rely on application-level HMAC validation (already implemented in the JDBC driver). This provides:
- Consistent behavior across databases
- Better performance
- Simpler deployment
- Adequate security

Database triggers add defense-in-depth but are not required.
