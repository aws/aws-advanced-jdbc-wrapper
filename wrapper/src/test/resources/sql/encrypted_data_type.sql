-- PostgreSQL domain for HMAC-verified encrypted data
-- Format: [HMAC:32bytes][type:1byte][IV:12bytes][ciphertext]
DROP DOMAIN IF EXISTS encrypted_data CASCADE;
CREATE DOMAIN encrypted_data AS bytea
CHECK (length(VALUE) >= 45);

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
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION has_valid_hmac_structure(data encrypted_data)
RETURNS boolean
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    RETURN length(data::bytea) >= 45;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

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
    -- (SCHEMA_NAME is replaced at install) so PL/pgSQL caches its plan across rows.
    SELECT ks.hmac_key INTO hmac_key
      FROM SCHEMA_NAME.encryption_metadata em
      JOIN SCHEMA_NAME.key_storage ks ON em.key_id = ks.id
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
$$ LANGUAGE plpgsql;

-- Records which version of this SQL is installed. Check it with:
--   SELECT obj_description('validate_encrypted_data_hmac'::regproc, 'pg_proc');
-- A pre-v1 (uncommented) install returns NULL. Run this statement together with the
-- CREATE OR REPLACE FUNCTION above when upgrading, or the marker stays NULL.
COMMENT ON FUNCTION validate_encrypted_data_hmac() IS 'aws-advanced-jdbc-wrapper kms_encryption trigger v1';
