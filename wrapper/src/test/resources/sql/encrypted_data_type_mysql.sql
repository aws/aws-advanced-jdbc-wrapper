-- MySQL stored routines for HMAC-verified encrypted data
-- Format: [HMAC:32bytes][type:1byte][IV:12bytes][ciphertext]
--
-- Requires MySQL 8.0+. HMAC-SHA256 is implemented in pure SQL with the SHA2() function.
-- Only static SQL is used, so the validation routines can be called from triggers.
-- Before running this script, replace every occurrence of the metadata schema placeholder
-- SCHEMA_NAME (written in upper case in the procedure body below) with the encryption metadata
-- schema name.

DELIMITER $$

-- HMAC-SHA256 built from SHA2(), which MySQL has no native HMAC for (RFC 2104).
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
    DECLARE key_padded VARBINARY(64);
    DECLARE ipad VARBINARY(64);
    DECLARE opad VARBINARY(64);
    DECLARE i INT DEFAULT 1;
    DECLARE key_byte INT;

    -- Reduce an over-long key to its hash, then zero-pad the key to the block size.
    IF LENGTH(key_data) > block_size THEN
        SET key_padded = CONCAT(UNHEX(SHA2(key_data, 256)), REPEAT(X'00', block_size - 32));
    ELSE
        SET key_padded = CONCAT(key_data, REPEAT(X'00', block_size - LENGTH(key_data)));
    END IF;

    -- ipad = key XOR 0x36 per byte, opad = key XOR 0x5C per byte, one raw byte at a time.
    -- UNHEX(LPAD(HEX(...), 2, '0')) builds each byte from its hex, so a key byte >= 128 is written
    -- back as that exact raw byte. CHAR() would encode it through the connection charset and corrupt
    -- the key.
    SET ipad = X'';
    SET opad = X'';
    WHILE i <= block_size DO
        SET key_byte = ASCII(SUBSTRING(key_padded, i, 1));
        SET ipad = CONCAT(ipad, UNHEX(LPAD(HEX(key_byte ^ 0x36), 2, '0')));
        SET opad = CONCAT(opad, UNHEX(LPAD(HEX(key_byte ^ 0x5C), 2, '0')));
        SET i = i + 1;
    END WHILE;

    RETURN UNHEX(SHA2(CONCAT(opad, UNHEX(SHA2(CONCAT(ipad, message), 256))), 256));
END$$

-- Verifies that a stored value carries a valid HMAC for the given HMAC key.
DROP FUNCTION IF EXISTS verify_encrypted_data_hmac$$
CREATE FUNCTION verify_encrypted_data_hmac(
    data VARBINARY(65535),
    hmac_key VARBINARY(64)
)
RETURNS BOOLEAN
DETERMINISTIC
NO SQL
BEGIN
    -- Minimum payload: 32 (HMAC) + 1 (type) + 12 (IV) + 0 (ciphertext) + 16 (GCM tag) = 61.
    IF data IS NULL OR LENGTH(data) < 61 THEN
        RETURN FALSE;
    END IF;

    -- Format: [HMAC:32][type:1][IV:12][ciphertext]
    RETURN SUBSTRING(data, 1, 32) = hmac_sha256(SUBSTRING(data, 33), hmac_key);
END$$

-- The earlier single procedure validate_encrypted_data_hmac cached the column's HMAC key in a
-- session user variable, which a session could preset to slip a value past the check. It is
-- replaced by the two procedures below, which look the key up from the metadata for every row and
-- cache nothing.
DROP PROCEDURE IF EXISTS validate_encrypted_data_hmac$$

-- Called from a BEFORE INSERT trigger (and by the BEFORE UPDATE procedure) to reject a value that is
-- not a valid kms_encryption payload. The key is read from key_storage for every row, so a session
-- cannot preset it, and the error carries no values a rejected writer could retry with.
-- The COMMENT records which version of this SQL is installed. Both this procedure and
-- validate_encrypted_data_hmac_before_update carry it; a pre-v1 install returns an empty string, and
-- one marked while the other is not means a half-finished upgrade. Check both with:
--   SELECT ROUTINE_NAME, ROUTINE_COMMENT FROM information_schema.ROUTINES
--    WHERE ROUTINE_SCHEMA = DATABASE()
--      AND ROUTINE_NAME IN ('validate_encrypted_data_hmac_before_insert',
--                           'validate_encrypted_data_hmac_before_update');
DROP PROCEDURE IF EXISTS validate_encrypted_data_hmac_before_insert$$
CREATE PROCEDURE validate_encrypted_data_hmac_before_insert(
    IN p_table_name VARCHAR(64),
    IN p_column_name VARCHAR(64),
    IN column_value VARBINARY(65535)
)
COMMENT 'aws-advanced-jdbc-wrapper kms_encryption trigger v1'
READS SQL DATA
BEGIN
    DECLARE v_hmac_key VARBINARY(64);

    IF column_value IS NOT NULL THEN
        SELECT ks.hmac_key INTO v_hmac_key
        FROM SCHEMA_NAME.encryption_metadata em
        JOIN SCHEMA_NAME.key_storage ks ON em.key_id = ks.id
        WHERE em.table_name = p_table_name
          AND em.column_name = p_column_name
        LIMIT 1;

        IF v_hmac_key IS NULL THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'No HMAC key is configured for the encrypted column';
        END IF;

        IF NOT verify_encrypted_data_hmac(column_value, v_hmac_key) THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'HMAC verification failed (plaintext or tampered value)';
        END IF;
    END IF;
END$$

-- Called from a BEFORE UPDATE trigger. An UPDATE that leaves the value as it was is not re-checked,
-- so rows written under a previous key keep working while a data key rotation is in progress.
DROP PROCEDURE IF EXISTS validate_encrypted_data_hmac_before_update$$
CREATE PROCEDURE validate_encrypted_data_hmac_before_update(
    IN p_table_name VARCHAR(64),
    IN p_column_name VARCHAR(64),
    IN new_value VARBINARY(65535),
    IN old_value VARBINARY(65535)
)
COMMENT 'aws-advanced-jdbc-wrapper kms_encryption trigger v1'
READS SQL DATA
BEGIN
    IF NOT (new_value <=> old_value) THEN
        CALL validate_encrypted_data_hmac_before_insert(p_table_name, p_column_name, new_value);
    END IF;
END$$

DELIMITER ;

-- Example triggers for a specific table/column (single statements, no DELIMITER needed):
--
-- CREATE TRIGGER users_ssn_hmac_ins BEFORE INSERT ON users FOR EACH ROW
--   CALL validate_encrypted_data_hmac_before_insert('users', 'ssn', NEW.ssn);
-- CREATE TRIGGER users_ssn_hmac_upd BEFORE UPDATE ON users FOR EACH ROW
--   CALL validate_encrypted_data_hmac_before_update('users', 'ssn', NEW.ssn, OLD.ssn);
--
-- The insert and update procedures take different arguments, so triggers created against an earlier
-- version of this script must be dropped and re-created as shown above.
--
-- With binary logging enabled, creating these routines and triggers needs the SUPER privilege or
-- log_bin_trust_function_creators=1.
--
-- Note: application-level HMAC validation by the JDBC driver is still the primary check. Database
-- triggers provide defense in depth and are optional.
