-- MySQL stored routines for HMAC-verified encrypted data
-- Format: [HMAC:32bytes][type:1byte][IV:12bytes][ciphertext]
--
-- Requires MySQL 8.0+. HMAC-SHA256 is implemented in pure SQL with the SHA2() function.
-- Only static SQL is used, so the validation procedure can be called from triggers.
-- Before running this script, replace every occurrence of the metadata schema placeholder
-- (written in upper case in the procedure body below) with the encryption metadata schema name.

DELIMITER $$

-- HMAC-SHA256 implementation using MySQL's SHA2() function (RFC 2104)
-- HMAC(K, m) = SHA256((K xor opad) || SHA256((K xor ipad) || m))
DROP FUNCTION IF EXISTS hmac_sha256$$
CREATE FUNCTION hmac_sha256(
    message VARBINARY(65535),
    key_data VARBINARY(64)
)
RETURNS VARBINARY(32)
DETERMINISTIC
NO SQL
BEGIN
    DECLARE block_size INT DEFAULT 64;  -- SHA-256 block size
    DECLARE ipad VARBINARY(64);
    DECLARE opad VARBINARY(64);
    DECLARE key_padded VARBINARY(64);
    DECLARE inner_hash VARBINARY(32);
    DECLARE i INT DEFAULT 1;
    DECLARE key_byte INT;

    -- Prepare key: if longer than block size, hash it first
    IF LENGTH(key_data) > block_size THEN
        SET key_padded = CONCAT(UNHEX(SHA2(key_data, 256)), REPEAT(0x00, block_size - 32));
    ELSE
        SET key_padded = CONCAT(key_data, REPEAT(0x00, block_size - LENGTH(key_data)));
    END IF;

    -- Create ipad (0x36 repeated) and opad (0x5C repeated)
    SET ipad = '';
    SET opad = '';

    WHILE i <= block_size DO
        SET key_byte = ASCII(SUBSTRING(key_padded, i, 1));
        SET ipad = CONCAT(ipad, CHAR(key_byte ^ 0x36));
        SET opad = CONCAT(opad, CHAR(key_byte ^ 0x5C));
        SET i = i + 1;
    END WHILE;

    -- HMAC = SHA256(opad || SHA256(ipad || message))
    SET inner_hash = UNHEX(SHA2(CONCAT(ipad, message), 256));

    RETURN UNHEX(SHA2(CONCAT(opad, inner_hash), 256));
END$$

-- Helper function to verify HMAC using HMAC key (two-key format)
DROP FUNCTION IF EXISTS verify_encrypted_data_hmac$$
CREATE FUNCTION verify_encrypted_data_hmac(
    data VARBINARY(65535),
    hmac_key VARBINARY(32)
)
RETURNS BOOLEAN
DETERMINISTIC
NO SQL
BEGIN
    DECLARE stored_hmac VARBINARY(32);
    DECLARE encrypted_payload VARBINARY(65535);
    DECLARE calculated_hmac VARBINARY(32);

    -- Format: [HMAC:32][type:1][IV:12][ciphertext]
    IF LENGTH(data) < 45 THEN
        RETURN FALSE;
    END IF;

    SET stored_hmac = SUBSTRING(data, 1, 32);
    SET encrypted_payload = SUBSTRING(data, 33);

    -- Calculate HMAC-SHA256
    SET calculated_hmac = hmac_sha256(encrypted_payload, hmac_key);

    RETURN stored_hmac = calculated_hmac;
END$$

-- The earlier procedure used dynamic SQL and could not run from a trigger
DROP PROCEDURE IF EXISTS validate_encrypted_data_hmac_before_insert$$

-- Validates the HMAC of an encrypted column value. Called from BEFORE INSERT and BEFORE UPDATE
-- triggers. p_cached_key is the session cache of the column's HMAC key: the trigger passes the user
-- variable @hmac_key_<table>_<column>. An empty cache is filled from the metadata. When the value
-- fails the check against a cached key, the key is reloaded from the metadata once (it can be stale
-- after a data key rotation) and the value is checked again.
DROP PROCEDURE IF EXISTS validate_encrypted_data_hmac$$
CREATE PROCEDURE validate_encrypted_data_hmac(
    IN p_table_name VARCHAR(64),
    IN p_column_name VARCHAR(64),
    IN p_value VARBINARY(65535),
    INOUT p_cached_key VARBINARY(64)
)
READS SQL DATA
BEGIN
    DECLARE v_key VARBINARY(64);
    DECLARE v_from_cache BOOLEAN DEFAULT FALSE;
    DECLARE v_valid BOOLEAN;
    DECLARE v_msg VARCHAR(255);
    DECLARE CONTINUE HANDLER FOR NOT FOUND BEGIN END;

    IF p_value IS NOT NULL THEN
        IF LENGTH(p_value) < 45 THEN
            SET v_msg = CONCAT('Invalid encrypted data length for column ', p_table_name, '.', p_column_name);
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_msg;
        END IF;

        IF p_cached_key IS NOT NULL THEN
            SET v_key = p_cached_key;
            SET v_from_cache = TRUE;
        ELSE
            SET v_key = NULL;
            SELECT ks.hmac_key INTO v_key
            FROM SCHEMA_NAME.encryption_metadata em
            JOIN SCHEMA_NAME.key_storage ks ON em.key_id = ks.id
            WHERE em.table_name = p_table_name
              AND em.column_name = p_column_name;

            IF v_key IS NULL THEN
                SET v_msg = CONCAT('No HMAC key found for ', p_table_name, '.', p_column_name);
                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_msg;
            END IF;

            SET p_cached_key = v_key;
        END IF;

        SET v_valid = verify_encrypted_data_hmac(p_value, v_key);

        -- The cached key can be stale after a data key rotation: reload it once and retry
        IF NOT v_valid AND v_from_cache THEN
            SET v_key = NULL;
            SELECT ks.hmac_key INTO v_key
            FROM SCHEMA_NAME.encryption_metadata em
            JOIN SCHEMA_NAME.key_storage ks ON em.key_id = ks.id
            WHERE em.table_name = p_table_name
              AND em.column_name = p_column_name;

            IF v_key IS NULL THEN
                SET v_msg = CONCAT('No HMAC key found for ', p_table_name, '.', p_column_name);
                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_msg;
            END IF;

            SET p_cached_key = v_key;
            SET v_valid = verify_encrypted_data_hmac(p_value, v_key);
        END IF;

        IF NOT v_valid THEN
            SET v_msg = CONCAT('HMAC verification failed for column ', p_table_name, '.', p_column_name);
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_msg;
        END IF;
    END IF;
END$$

DELIMITER ;

-- Example triggers for a specific table/column (single statements, no DELIMITER needed):
--
-- CREATE TRIGGER users_ssn_hmac_ins BEFORE INSERT ON users FOR EACH ROW
--   CALL validate_encrypted_data_hmac('users', 'ssn', NEW.ssn, @hmac_key_users_ssn);
-- CREATE TRIGGER users_ssn_hmac_upd BEFORE UPDATE ON users FOR EACH ROW
--   CALL validate_encrypted_data_hmac('users', 'ssn', NEW.ssn, @hmac_key_users_ssn);
--
-- The user variable @hmac_key_<table>_<column> (at most 64 characters) is the per-session cache of
-- the HMAC key. The JDBC KeyManagementUtility sets the same variable before it re-encrypts a column.
-- A failed check (SIGNAL) leaves the variable unchanged.
--
-- With binary logging enabled, creating these routines and triggers needs the SUPER privilege or
-- log_bin_trust_function_creators=1.
--
-- Note: application-level HMAC validation by the JDBC driver is still the primary check. Database
-- triggers provide defense in depth and are optional.
