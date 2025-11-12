-- PostgreSQL domain for HMAC-verified encrypted data
-- Format: [salt:16bytes][HMAC:32bytes][type:1byte][IV:12bytes][ciphertext]

CREATE DOMAIN encrypted_data AS bytea
CHECK (length(VALUE) >= 61);

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

CREATE OR REPLACE FUNCTION has_valid_hmac_structure(data encrypted_data)
RETURNS boolean AS $$
BEGIN
    RETURN length(data::bytea) >= 61;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Trigger function that fetches encryption key from metadata and validates HMAC
CREATE OR REPLACE FUNCTION validate_encrypted_data_hmac()
RETURNS trigger AS $$
DECLARE
    metadata_schema text := 'encrypt';
    col_name text;
    col_value encrypted_data;
    encrypted_data_key_base64 text;
    encrypted_data_key bytea;
    is_valid boolean;
BEGIN
    -- Validate each encrypted_data column
    FOR col_name IN
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = TG_TABLE_SCHEMA
          AND table_name = TG_TABLE_NAME
          AND domain_name = 'encrypted_data'
    LOOP
        EXECUTE format('SELECT ($1).%I', col_name) INTO col_value USING NEW;
        
        IF col_value IS NOT NULL THEN
            -- Fetch encrypted data key from metadata
            EXECUTE format(
                'SELECT ks.encrypted_data_key FROM %I.encryption_metadata em ' ||
                'JOIN %I.key_storage ks ON em.key_id = ks.id ' ||
                'WHERE em.table_name = $1 AND em.column_name = $2',
                metadata_schema, metadata_schema
            ) INTO encrypted_data_key_base64 USING TG_TABLE_NAME, col_name;
            
            IF encrypted_data_key_base64 IS NULL THEN
                RAISE EXCEPTION 'No encryption metadata found for %.%', TG_TABLE_NAME, col_name;
            END IF;
            
            -- The encrypted_data_key is encrypted by KMS
            -- We cannot decrypt it in the database without KMS access
            -- So we only validate structure here
            IF NOT has_valid_hmac_structure(col_value) THEN
                RAISE EXCEPTION 'Invalid HMAC structure for column %', col_name;
            END IF;
            
            -- Note: Full HMAC validation requires decrypted data key
            -- This must be done at application layer with KMS access
        END IF;
    END LOOP;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
