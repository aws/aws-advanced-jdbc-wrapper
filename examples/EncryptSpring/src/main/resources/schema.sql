-- Create metadata schema
CREATE SCHEMA IF NOT EXISTS encrypt^;

-- Enable pgcrypto extension for HMAC functions.
-- Installed into public so the trigger can call it as public.hmac under a locked search_path.
CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public^;

-- PostgreSQL domain for HMAC-verified encrypted data
-- Format: [HMAC:32bytes][type:1byte][IV:12bytes][ciphertext]
-- Minimum length: 32 (HMAC) + 1 (type) + 12 (IV) + 0 (ciphertext) + 16 (GCM tag) = 61.
-- This script runs on every application start, so the domain is created only when it does not
-- exist yet: dropping it would drop the encrypted columns and their data. On an existing install
-- with an older length check, only that check is replaced, NOT VALID so existing values are not
-- re-checked.
-- The domain and functions are copied from wrapper/src/test/resources/sql/encrypted_data_type.sql;
-- keep the two in sync.
DO $$
BEGIN
    IF to_regtype('encrypted_data') IS NULL THEN
        CREATE DOMAIN encrypted_data AS bytea
            CONSTRAINT encrypted_data_check CHECK (length(VALUE) >= 61);
    ELSIF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE contypid = 'encrypted_data'::regtype
           AND conname = 'encrypted_data_check'
           AND pg_get_constraintdef(oid) LIKE 'CHECK ((length(VALUE) >= 61))%') THEN
        ALTER DOMAIN encrypted_data DROP CONSTRAINT IF EXISTS encrypted_data_check;
        ALTER DOMAIN encrypted_data
            ADD CONSTRAINT encrypted_data_check CHECK (length(VALUE) >= 61) NOT VALID;
    END IF;
END
$$^;

-- Key storage table
CREATE TABLE IF NOT EXISTS encrypt.key_storage (
    id SERIAL PRIMARY KEY,
    key_id VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    master_key_arn VARCHAR(512) NOT NULL,
    encrypted_data_key TEXT NOT NULL,
    hmac_key BYTEA NOT NULL,
    key_spec VARCHAR(50) DEFAULT 'AES_256',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
)^;

-- Encryption metadata table
CREATE TABLE IF NOT EXISTS encrypt.encryption_metadata (
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    encryption_algorithm VARCHAR(50) NOT NULL,
    key_id INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, column_name),
    FOREIGN KEY (key_id) REFERENCES encrypt.key_storage(id)
)^;

-- Helper function to verify HMAC using HMAC key (two-key format).
-- Like the trigger below, its search_path is pinned to pg_catalog, pg_temp so a session cannot
-- shadow hmac() or the built-in operators, and hmac() is called as public.hmac (the schema
-- pgcrypto is installed into).
CREATE OR REPLACE FUNCTION verify_encrypted_data_hmac(
    data encrypted_data,
    hmac_key bytea
)
RETURNS boolean
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    data_bytes bytea := data::bytea;
    stored_hmac bytea;
    encrypted_payload bytea;
    calculated_hmac bytea;
BEGIN
    -- Format: [HMAC:32][type:1][IV:12][ciphertext]
    stored_hmac := substring(data_bytes from 1 for 32);
    encrypted_payload := substring(data_bytes from 33);
    calculated_hmac := public.hmac(encrypted_payload, hmac_key, 'sha256'::text);
    RETURN stored_hmac = calculated_hmac;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT^;

CREATE OR REPLACE FUNCTION has_valid_hmac_structure(data encrypted_data)
RETURNS boolean
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    RETURN length(data::bytea) >= 61;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT^;

-- Trigger function that validates HMAC for a specific column
-- Usage: CREATE TRIGGER trigger_name BEFORE INSERT OR UPDATE ON table_name
--        FOR EACH ROW EXECUTE FUNCTION validate_encrypted_data_hmac('column_name');
--
-- A trigger runs with the search_path of the session that fires it. Pinning it to
-- pg_catalog, pg_temp stops a session from putting its own functions or operators (such as
-- hmac() or <>) ahead of the built-in ones to slip a plaintext value past this check. hmac()
-- is therefore called as public.hmac, the schema pgcrypto is installed into.
CREATE OR REPLACE FUNCTION validate_encrypted_data_hmac()
RETURNS trigger
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    col_name text := TG_ARGV[0];
    col_value bytea;
    hmac_key bytea;
    unchanged boolean;
BEGIN
    -- Fetch the new value, and on UPDATE whether it is unchanged, in one query. An UPDATE that
    -- leaves the encrypted value as it was is not re-checked, so rows written with a previous key
    -- keep working while a data key rotation is in progress.
    IF TG_OP = 'UPDATE' THEN
        EXECUTE format('SELECT ($1).%I, ($1).%I IS NOT DISTINCT FROM ($2).%I', col_name, col_name, col_name)
            INTO col_value, unchanged USING NEW, OLD;
        IF unchanged THEN
            RETURN NEW;
        END IF;
    ELSE
        EXECUTE format('SELECT ($1).%I', col_name) INTO col_value USING NEW;
    END IF;

    IF col_value IS NULL THEN
        RETURN NEW;
    END IF;

    -- Read the column's current HMAC key from key_storage for every row. The key is never
    -- cached in the session, so a session cannot preset it ahead of this check. The query is static
    -- so PL/pgSQL caches its plan across rows.
    SELECT ks.hmac_key INTO hmac_key
      FROM encrypt.encryption_metadata em
      JOIN encrypt.key_storage ks ON em.key_id = ks.id
     WHERE em.table_name = TG_TABLE_NAME AND em.column_name = col_name;

    IF hmac_key IS NULL THEN
        RAISE EXCEPTION 'No HMAC key found for %.%', TG_TABLE_NAME, col_name;
    END IF;

    -- Verify HMAC (format: [HMAC:32][type:1][IV:12][ciphertext]). The error carries no values,
    -- so a rejected writer learns nothing it could retry with.
    IF length(col_value) < 61 THEN
        RAISE EXCEPTION 'Invalid encrypted data length for column %', col_name;
    END IF;

    IF substring(col_value from 1 for 32)
         <> public.hmac(substring(col_value from 33), hmac_key, 'sha256'::text) THEN
        RAISE EXCEPTION 'HMAC verification failed for column %', col_name;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql^;

-- Records which version of this SQL is installed. Check it with:
--   SELECT obj_description('validate_encrypted_data_hmac'::regproc, 'pg_proc');
-- A pre-v1 (uncommented) install returns NULL. This script runs the whole file, so the comment is
-- always applied alongside the function.
COMMENT ON FUNCTION validate_encrypted_data_hmac() IS 'aws-advanced-jdbc-wrapper kms_encryption trigger v1'^;
