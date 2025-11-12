-- encrypted_data extension version 1.0
-- PostgreSQL custom type for HMAC-verified encrypted data

-- Create C functions
CREATE FUNCTION encrypted_data_in(cstring)
RETURNS encrypted_data
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION encrypted_data_out(encrypted_data)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION encrypted_data_recv(internal)
RETURNS encrypted_data
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION encrypted_data_send(encrypted_data)
RETURNS bytea
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

-- Create the type
CREATE TYPE encrypted_data (
    INPUT = encrypted_data_in,
    OUTPUT = encrypted_data_out,
    RECEIVE = encrypted_data_recv,
    SEND = encrypted_data_send,
    INTERNALLENGTH = VARIABLE,
    STORAGE = EXTENDED,
    ALIGNMENT = int4
);

-- Create cast from encrypted_data to bytea
CREATE CAST (encrypted_data AS bytea) WITHOUT FUNCTION AS IMPLICIT;

-- Input validation function to check HMAC structure
CREATE OR REPLACE FUNCTION validate_encrypted_data_input(data bytea)
RETURNS boolean AS $$
BEGIN
    -- Check minimum length: HMAC(32) + type(1) + IV(12) + min_ciphertext(16) = 61 bytes
    IF length(data) < 61 THEN
        RAISE EXCEPTION 'Invalid encrypted_data: minimum length is 61 bytes, got %', length(data);
    END IF;
    RETURN true;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Helper function to verify HMAC using stored salt and encryption key
CREATE OR REPLACE FUNCTION verify_encrypted_data_hmac(
    data encrypted_data,
    encryption_key bytea
)
RETURNS boolean AS $$
DECLARE
    data_bytes bytea := data::bytea;
    salt bytea;
    stored_hmac bytea;
    encrypted_payload bytea;
    verification_key bytea;
    calculated_hmac bytea;
BEGIN
    salt := substring(data_bytes from 1 for 16);
    stored_hmac := substring(data_bytes from 17 for 32);
    encrypted_payload := substring(data_bytes from 49);
    verification_key := hmac(salt, encryption_key, 'sha256');
    calculated_hmac := hmac(encrypted_payload, verification_key, 'sha256');
    RETURN stored_hmac = calculated_hmac;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Helper function to check if data has valid HMAC structure
CREATE OR REPLACE FUNCTION has_valid_hmac_structure(data encrypted_data)
RETURNS boolean AS $$
DECLARE
    data_bytes bytea := data::bytea;
BEGIN
    RETURN length(data_bytes) >= 61;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Validation function (C implementation if available)
CREATE FUNCTION encrypted_data_validate_structure(encrypted_data)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

-- Trigger function to validate HMAC on INSERT/UPDATE
-- Fetches HMAC key from metadata and validates
CREATE OR REPLACE FUNCTION validate_encrypted_data_hmac()
RETURNS trigger AS $$
DECLARE
    metadata_schema text := 'encrypt';
    col_name text;
    col_value encrypted_data;
    hmac_key bytea;
    data_bytes bytea;
    stored_hmac bytea;
    encrypted_payload bytea;
    calculated_hmac bytea;
    cache_key text;
BEGIN
    -- Validate each encrypted_data column
    FOR col_name IN
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = TG_TABLE_SCHEMA
          AND table_name = TG_TABLE_NAME
          AND udt_name = 'encrypted_data'
    LOOP
        EXECUTE format('SELECT ($1).%I', col_name) INTO col_value USING NEW;
        
        IF col_value IS NOT NULL THEN
            -- Try to get HMAC key from session cache
            cache_key := 'hmac_key.' || TG_TABLE_NAME || '.' || col_name;
            BEGIN
                hmac_key := current_setting(cache_key)::bytea;
            EXCEPTION WHEN OTHERS THEN
                -- Not cached, fetch from metadata
                EXECUTE format(
                    'SELECT ks.hmac_key FROM %I.encryption_metadata em ' ||
                    'JOIN %I.key_storage ks ON em.key_id = ks.id ' ||
                    'WHERE em.table_name = $1 AND em.column_name = $2',
                    metadata_schema, metadata_schema
                ) INTO hmac_key USING TG_TABLE_NAME, col_name;
                
                IF hmac_key IS NULL THEN
                    RAISE EXCEPTION 'No HMAC key found for %.%', TG_TABLE_NAME, col_name;
                END IF;
                
                -- Cache in session variable
                PERFORM set_config(cache_key, encode(hmac_key, 'hex'), false);
            END;
            
            -- Verify HMAC (new format: [HMAC:32][type:1][IV:12][ciphertext])
            data_bytes := col_value::bytea;
            
            IF length(data_bytes) < 61 THEN
                RAISE EXCEPTION 'Invalid encrypted data length for column %', col_name;
            END IF;
            
            stored_hmac := substring(data_bytes from 1 for 32);
            encrypted_payload := substring(data_bytes from 33);
            
            calculated_hmac := hmac(encrypted_payload, hmac_key, 'sha256');
            
            IF stored_hmac != calculated_hmac THEN
                RAISE EXCEPTION 'HMAC verification failed for column %', col_name;
            END IF;
        END IF;
    END LOOP;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
RETURNS trigger AS $$
DECLARE
    encryption_key bytea;
    col_name text;
    col_value encrypted_data;
    is_valid boolean;
BEGIN
    -- Get encryption key from session variable (if set)
    BEGIN
        encryption_key := current_setting('app.encryption_key')::bytea;
    EXCEPTION WHEN OTHERS THEN
        -- If no key is set, skip HMAC validation (only structure validation)
        RETURN NEW;
    END;

    -- Validate each encrypted_data column
    FOR col_name IN
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = TG_TABLE_NAME
          AND udt_name = 'encrypted_data'
    LOOP
        EXECUTE format('SELECT ($1).%I', col_name) INTO col_value USING NEW;
        
        IF col_value IS NOT NULL THEN
            -- Validate HMAC
            is_valid := verify_encrypted_data_hmac(col_value, encryption_key);
            
            IF NOT is_valid THEN
                RAISE EXCEPTION 'HMAC validation failed for column %', col_name;
            END IF;
        END IF;
    END LOOP;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Example: Create trigger on a table
-- CREATE TRIGGER validate_encrypted_data
--     BEFORE INSERT OR UPDATE ON users
--     FOR EACH ROW
--     EXECUTE FUNCTION validate_encrypted_data_hmac();
--
-- Usage:
-- SET app.encryption_key = '\xDEADBEEF...';  -- Set encryption key
-- INSERT INTO users (ssn) VALUES (...);       -- HMAC will be validated

