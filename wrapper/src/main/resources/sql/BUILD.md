# Building encrypted_data PostgreSQL Extension

This directory contains C source code for the `encrypted_data` PostgreSQL custom type.

## Prerequisites

- PostgreSQL development headers (`postgresql-server-dev` or `postgresql-devel`)
- C compiler (gcc or clang)
- make

## Building

```bash
cd wrapper/src/main/resources/sql
make
sudo make install
```

## Installing in Database

```sql
CREATE EXTENSION pgcrypto;
CREATE EXTENSION encrypted_data;
```

## Uninstalling

```sql
DROP EXTENSION encrypted_data CASCADE;
```

```bash
sudo make uninstall
```

## Files

- `encrypted_data.c` - C source code for I/O functions
- `encrypted_data--1.0.sql` - SQL definitions
- `encrypted_data.control` - Extension metadata
- `Makefile` - Build configuration

## Alternative: Use DOMAIN Instead

If you cannot compile C extensions, use the DOMAIN-based approach in `encrypted_data_type.sql` which provides similar functionality without requiring C code.

## Testing

After installation:

```sql
-- Create table
CREATE TABLE test (data encrypted_data);

-- Insert (will validate minimum 61 bytes)
INSERT INTO test VALUES (decode('base64_encoded_data_here', 'base64'));

-- Verify structure
SELECT has_valid_hmac_structure(data) FROM test;
```
