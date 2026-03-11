-- Create metadata schema
CREATE SCHEMA IF NOT EXISTS encrypt^;

-- Enable pgcrypto extension for HMAC functions
CREATE EXTENSION IF NOT EXISTS pgcrypto^;

-- PostgreSQL domain for HMAC-verified encrypted data
-- Format: [HMAC:32bytes][type:1byte][IV:12bytes][ciphertext]
DROP DOMAIN IF EXISTS encrypted_data CASCADE^;
CREATE DOMAIN encrypted_data AS bytea
CHECK (length(VALUE) >= 45)^;

-- Key storage table
CREATE TABLE IF NOT EXISTS encrypt.key_storage (
    id SERIAL PRIMARY KEY,
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

-- Helper function to verify HMAC using HMAC key (two-key format)
CREATE OR REPLACE FUNCTION verify_encrypted_data_hmac(
    data encrypted_data,
    hmac_key bytea
)
RETURNS boolean AS $$
DECLARE
    data_bytes bytea := data::bytea;
    stored_hmac bytea;
    encrypted_payload bytea;
    calculated_hmac bytea;
BEGIN
    -- Format: [HMAC:32][type:1][IV:12][ciphertext]
    stored_hmac := substring(data_bytes from 1 for 32);
    encrypted_payload := substring(data_bytes from 33);
    calculated_hmac := hmac(encrypted_payload, hmac_key, 'sha256');
    RETURN stored_hmac = calculated_hmac;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT^;

CREATE OR REPLACE FUNCTION has_valid_hmac_structure(data encrypted_data)
RETURNS boolean AS $$
BEGIN
    RETURN length(data::bytea) >= 45;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT^;

-- Trigger function that validates HMAC for a specific column
-- Usage: CREATE TRIGGER trigger_name BEFORE INSERT OR UPDATE ON table_name
--        FOR EACH ROW EXECUTE FUNCTION validate_encrypted_data_hmac('column_name');
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
        -- Try to get HMAC key from session cache
        cache_key := 'hmac_key.' || TG_TABLE_NAME || '.' || col_name;
        BEGIN
            hmac_key := decode(current_setting(cache_key), 'hex');
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

            -- Cache in session variable as hex string
            PERFORM set_config(cache_key, encode(hmac_key, 'hex'), false);
        END;

        -- Verify HMAC (format: [HMAC:32][type:1][IV:12][ciphertext])
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
$$ LANGUAGE plpgsql^;
