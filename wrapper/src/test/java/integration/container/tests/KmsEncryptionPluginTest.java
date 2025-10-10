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

package integration.container.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import integration.container.ConnectionStringHelper;
import integration.container.TestEnvironment;
import java.sql.*;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;

public class KmsEncryptionPluginTest {

  private static final String KMS_KEY_ARN_ENV = "AWS_KMS_KEY_ARN";
  private static final String TEST_SSN = "123-45-6789";
  private static final String TEST_NAME = "John Doe";
  private static final String TEST_EMAIL = "john.doe@example.com";

  private Connection connection;
  private String kmsKeyArn;
  private static final String DB_URL = "jdbc:aws-wrapper:postgresql://localhost:5432/myapp_db";

  @BeforeEach
  void setUp() throws Exception {
    kmsKeyArn = System.getenv(KMS_KEY_ARN_ENV);
    assumeTrue(kmsKeyArn != null && !kmsKeyArn.isEmpty(),
        "KMS Key ARN must be provided via " + KMS_KEY_ARN_ENV + " environment variable");

 //   Properties props = ConnectionStringHelper.getDefaultProperties();
    Properties props = new Properties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "kmsEncryption");
    props.setProperty(EncryptionConfig.KMS_MASTER_KEY_ARN.name, kmsKeyArn);
    props.setProperty(EncryptionConfig.KMS_REGION.name, "us-east-1");
    props.setProperty("user", "myapp_user");
    props.setProperty("password", "password");
    connection = DriverManager.getConnection(DB_URL, props);

    // Create test table and metadata
    try (Statement stmt = connection.createStatement()) {
      // Create metadata tables
      stmt.execute("CREATE TABLE IF NOT EXISTS key_storage (id SERIAL PRIMARY KEY, name VARCHAR(255), master_key_arn VARCHAR(512), encrypted_data_key TEXT, key_spec VARCHAR(50), created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, last_used_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP)");
      stmt.execute("CREATE TABLE IF NOT EXISTS encryption_metadata (table_name VARCHAR(255), column_name VARCHAR(255), encryption_algorithm VARCHAR(50), key_id INTEGER, created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (table_name, column_name))");
      
      // Insert test metadata
      stmt.execute("INSERT INTO key_storage (id, name, master_key_arn, encrypted_data_key, key_spec) VALUES (1, 'test-key', '" + kmsKeyArn + "', 'dummy-key', 'AES_256') ON CONFLICT (id) DO NOTHING");
      stmt.execute("INSERT INTO encryption_metadata (table_name, column_name, encryption_algorithm, key_id) VALUES ('users', 'ssn', 'AES-256-GCM', 1) ON CONFLICT (table_name, column_name) DO NOTHING");
      
      // Create test table
      stmt.execute("CREATE TABLE if not exists users ("
              + "id SERIAL PRIMARY KEY,"
              + "name VARCHAR(100),"
              + "ssn bytea,"
              + "email VARCHAR(100))");
    }
  }

  @AfterEach
  void tearDown() throws Exception {
    if (connection != null && !connection.isClosed()) {
      /**
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("DROP TABLE IF EXISTS users");
      }
     **/
      connection.close();
    }
  }

  @Test
  void testEncryptedSsnStorage() throws Exception {
    // Insert user with encrypted SSN
    String insertSql = "INSERT INTO users (name, ssn, email) VALUES (?, ?, ?)";
    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      pstmt.setString(1, TEST_NAME);
      pstmt.setString(2, TEST_SSN);
      pstmt.setString(3, TEST_EMAIL);
      pstmt.executeUpdate();
    }

    // Verify data can be retrieved and decrypted
    String selectSql = "SELECT name, ssn, email FROM users WHERE name = ?";
    try (PreparedStatement pstmt = connection.prepareStatement(selectSql)) {
      pstmt.setString(1, TEST_NAME);
      try (ResultSet rs = pstmt.executeQuery()) {
        assertNotNull(rs);
        assertEquals(true, rs.next());
        assertEquals(TEST_NAME, rs.getString("name"));
        assertEquals(TEST_SSN, rs.getString("ssn"));
        assertEquals(TEST_EMAIL, rs.getString("email"));
      }
    }

    // Verify SSN is actually encrypted in storage by connecting without encryption
    //Properties plainProps = ConnectionStringHelper.getDefaultProperties();
    Properties plainProps = new Properties();
    plainProps.setProperty("user", "myapp_user");
    plainProps.setProperty("password", "password");
    try (Connection plainConnection = DriverManager.getConnection(DB_URL, plainProps);
         PreparedStatement pstmt = plainConnection.prepareStatement(selectSql)) {
      pstmt.setString(1, TEST_NAME);
      try (ResultSet rs = pstmt.executeQuery()) {
        assertNotNull(rs);
        assertEquals(true, rs.next());
        assertEquals(TEST_NAME, rs.getString("name"));
        assertNotEquals(TEST_SSN, rs.getString("ssn")); // Should be encrypted
        assertEquals(TEST_EMAIL, rs.getString("email"));
      }
    }
  }
}
