/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.encryptspring;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import java.security.SecureRandom;
import java.util.Base64;

@Component
public class EncryptionConfig implements CommandLineRunner {
    private final JdbcTemplate jdbc;
    private final KmsClient kms;
    
    @Value("${spring.datasource.hikari.data-source-properties.kmsEncryptionKeyId}")
    private String kmsKeyId;
    
    public EncryptionConfig(JdbcTemplate jdbc, KmsClient kms) {
        this.jdbc = jdbc;
        this.kms = kms;
    }
    
    @Override
    public void run(String... args) {
        Integer keyId = jdbc.query(
            "SELECT id FROM encrypt.key_storage WHERE master_key_arn = ?",
            rs -> rs.next() ? rs.getInt("id") : null,
            kmsKeyId
        );
        
        if (keyId == null) {
            GenerateDataKeyResponse response = kms.generateDataKey(
                GenerateDataKeyRequest.builder()
                    .keyId(kmsKeyId)
                    .keySpec(DataKeySpec.AES_256)
                    .build()
            );
            
            byte[] hmacKey = new byte[32];
            new SecureRandom().nextBytes(hmacKey);
            
            keyId = jdbc.queryForObject(
                "INSERT INTO encrypt.key_storage (name, master_key_arn, encrypted_data_key, hmac_key, key_spec) " +
                "VALUES (?, ?, ?, ?, ?) RETURNING id",
                Integer.class,
                "default-key",
                kmsKeyId,
                Base64.getEncoder().encodeToString(response.ciphertextBlob().asByteArray()),
                hmacKey,
                "AES_256"
            );
        }
        
        Integer exists = jdbc.queryForObject(
            "SELECT COUNT(*) FROM encrypt.encryption_metadata WHERE table_name = ? AND column_name = ?",
            Integer.class, "users", "ssn"
        );
        
        if (exists == 0) {
            jdbc.update(
                "INSERT INTO encrypt.encryption_metadata (table_name, column_name, encryption_algorithm, key_id) " +
                "VALUES (?, ?, ?, ?)",
                "users", "ssn", "AES-256-GCM", keyId
            );
        }
    }
}
