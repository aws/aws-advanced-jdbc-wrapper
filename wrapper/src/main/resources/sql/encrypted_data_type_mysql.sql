-- MySQL stored procedures for HMAC-verified encrypted data
-- Format: [HMAC:32bytes][type:1byte][IV:12bytes][ciphertext]

-- Note: MySQL doesn't have built-in HMAC functions in older versions
-- This requires MySQL 8.0+ or MariaDB 10.5+ for SHA2_HMAC function

DELIMITER $$

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

-- HMAC-SHA256 implementation using MySQL's SHA2() function
-- HMAC(K, m) = SHA256((K ⊕ opad) || SHA256((K ⊕ ipad) || m))
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

-- Trigger procedure for validating HMAC before INSERT
DROP PROCEDURE IF EXISTS validate_encrypted_data_hmac_before_insert$$
CREATE PROCEDURE validate_encrypted_data_hmac_before_insert(
    IN table_name VARCHAR(64),
    IN column_name VARCHAR(64),
    IN column_value VARBINARY(65535)
)
BEGIN
    DECLARE hmac_key VARBINARY(32);
    DECLARE cache_var_name VARCHAR(128);
    DECLARE is_valid BOOLEAN;
    
    IF column_value IS NOT NULL THEN
        -- Build cache variable name
        SET cache_var_name = CONCAT('@hmac_key_', table_name, '_', column_name);
        
        -- Try to get cached HMAC key from user variable
        SET @cached_key = NULL;
        SET @query = CONCAT('SET @cached_key = ', cache_var_name);
        PREPARE stmt FROM @query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        
        IF @cached_key IS NULL THEN
            -- Not cached, fetch from metadata
            SELECT ks.hmac_key INTO hmac_key
            FROM aws.encryption_metadata em
            JOIN aws.key_storage ks ON em.key_id = ks.id
            WHERE em.table_name = table_name 
              AND em.column_name = column_name;
            
            IF hmac_key IS NULL THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'No HMAC key found for column';
            END IF;
            
            -- Cache in user variable for this session
            SET @query = CONCAT('SET ', cache_var_name, ' = ?');
            PREPARE stmt FROM @query;
            EXECUTE stmt USING hmac_key;
            DEALLOCATE PREPARE stmt;
        ELSE
            SET hmac_key = @cached_key;
        END IF;
        
        -- Verify HMAC
        IF LENGTH(column_value) < 45 THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Invalid encrypted data length';
        END IF;
        
        -- Verify using helper function
        SET is_valid = verify_encrypted_data_hmac(column_value, hmac_key);
        
        IF NOT is_valid THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'HMAC verification failed';
        END IF;
    END IF;
END$$

DELIMITER ;

-- Example trigger creation for a specific table/column:
-- DELIMITER $$
-- CREATE TRIGGER users_ssn_hmac_check
-- BEFORE INSERT ON users
-- FOR EACH ROW
-- BEGIN
--     CALL validate_encrypted_data_hmac_before_insert('users', 'ssn', NEW.ssn);
-- END$$
-- 
-- CREATE TRIGGER users_ssn_hmac_check_update
-- BEFORE UPDATE ON users
-- FOR EACH ROW
-- BEGIN
--     CALL validate_encrypted_data_hmac_before_insert('users', 'ssn', NEW.ssn);
-- END$$
-- DELIMITER ;

-- Note: For production use with MySQL on AWS RDS:
-- 1. This implementation uses pure SQL with SHA2() function (works on all MySQL 5.7+)
-- 2. Application-level HMAC validation (current JDBC driver approach) is still recommended for performance
-- 3. Database triggers provide defense-in-depth but are optional
