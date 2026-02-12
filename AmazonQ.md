# Amazon Q MCP Server for AWS Advanced JDBC Wrapper

This document describes the Model Context Protocol (MCP) server implementation for the AWS Advanced JDBC Wrapper project.

## Overview

The MCP server provides additional context and tools for Amazon Q to assist with development, testing, and usage of the AWS Advanced JDBC Wrapper. It enables Amazon Q to:

1. Analyze JDBC connection configurations
2. Generate connection strings with appropriate parameters
3. Suggest optimal plugin configurations for different use cases
4. Troubleshoot common connection issues
5. Provide examples for specific scenarios

## Available Tools

### Connection String Generator

Generates JDBC connection strings for different database types with AWS Advanced JDBC Wrapper configuration.

### Plugin Configuration Analyzer

Analyzes and suggests optimal plugin configurations based on the use case.

### Connection Troubleshooter

Identifies common issues in connection configurations and suggests solutions.

## Supported Plugins

The MCP server supports all the plugins available in the AWS Advanced JDBC Wrapper:

1. **Failover Plugin** - Enables fast failover for Aurora clusters
2. **Failover2 Plugin** - Improved version of the failover plugin with better performance
3. **Host Monitoring Plugin** - Monitors database instance health
4. **IAM Authentication Plugin** - Enables IAM authentication for database access
5. **Secrets Manager Plugin** - Integrates with AWS Secrets Manager for credential management
6. **Read/Write Splitting Plugin** - Distributes read and write operations to appropriate instances
7. **Driver Metadata Plugin** - Allows overriding driver metadata
8. **Telemetry Plugin** - Provides metrics and tracing for database connections
9. **Initial Aurora Connection Strategy Plugin** - Controls how connections are established to Aurora clusters
10. **KMS Encryption Plugin** - Provides transparent encryption/decryption using AWS KMS for sensitive data

## KMS Encryption Plugin

The KMS Encryption Plugin enables transparent encryption and decryption of sensitive data using AWS Key Management Service (KMS).

### Requirements

**Dependencies:**
```xml
<!-- Maven -->
<dependency>
  <groupId>software.amazon.jdbc</groupId>
  <artifactId>aws-advanced-jdbc-wrapper</artifactId>
  <version>LATEST</version>
</dependency>

<!-- AWS SDK for KMS -->
<dependency>
  <groupId>software.amazon.awssdk</groupId>
  <artifactId>kms</artifactId>
  <version>2.41.20</version>
</dependency>

<!-- JSqlParser for SQL analysis -->
<dependency>
  <groupId>com.github.jsqlparser</groupId>
  <artifactId>jsqlparser</artifactId>
  <version>4.5</version>
</dependency>

<!-- PostgreSQL JDBC Driver -->
<dependency>
  <groupId>org.postgresql</groupId>
  <artifactId>postgresql</artifactId>
  <version>42.7.7</version>
</dependency>
```

```gradle
// Gradle
dependencies {
    implementation 'software.amazon.jdbc:aws-advanced-jdbc-wrapper:LATEST'
    implementation 'software.amazon.awssdk:kms:2.41.20'
    implementation 'com.github.jsqlparser:jsqlparser:4.5'
    implementation 'org.postgresql:postgresql:42.7.7'
}
```

**Database Schema:**
- Encrypted columns must be of type `encrypted_data` (PostgreSQL) or `VARBINARY` (MySQL)
- The `encrypted_data` type is a custom PostgreSQL DOMAIN over BYTEA with HMAC verification
- The encryption algorithms supported are `AES-256-GCM` and `AES-128-GCM` the default being `AES-256-GCM`

**Sensitive Data Types:**
- **PANs (Primary Account Numbers)** - Must use `encrypted_data` type
- **SSNs (Social Security Numbers)** - Must use `encrypted_data` type
- **Credit Card Numbers** - Must use `encrypted_data` type
- **Personal Identifiable Information (PII)** - Must use `encrypted_data` type

### Setup

**IMPORTANT:** Database setup must be performed using a plain JDBC connection WITHOUT the kmsEncryption plugin. Only after the metadata schema and tables are created can the kmsEncryption plugin be used.

**1. Create metadata schema:**

```sql
CREATE SCHEMA IF NOT EXISTS encrypt;
```

**2. Create the encrypted_data type and helper functions:**

```sql
-- Enable pgcrypto extension for HMAC functions
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Create the encrypted_data domain
DROP DOMAIN IF EXISTS encrypted_data CASCADE;
CREATE DOMAIN encrypted_data AS bytea CHECK (length(VALUE) >= 45);

-- Helper function to verify HMAC
CREATE OR REPLACE FUNCTION verify_encrypted_data_hmac(
    data encrypted_data,
    hmac_key bytea
) RETURNS boolean AS $$
DECLARE
    data_bytes bytea := data::bytea;
    stored_hmac bytea;
    encrypted_payload bytea;
    calculated_hmac bytea;
BEGIN
    stored_hmac := substring(data_bytes from 1 for 32);
    encrypted_payload := substring(data_bytes from 33);
    calculated_hmac := hmac(encrypted_payload, hmac_key, 'sha256');
    RETURN stored_hmac = calculated_hmac;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Trigger function for HMAC validation
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
BEGIN
    EXECUTE format('SELECT ($1).%I', col_name) INTO col_value USING NEW;
    
    IF col_value IS NOT NULL THEN
        -- Fetch HMAC key from metadata
        EXECUTE format(
            'SELECT ks.hmac_key FROM %I.encryption_metadata em ' ||
            'JOIN %I.key_storage ks ON em.key_id = ks.id ' ||
            'WHERE em.table_name = $1 AND em.column_name = $2',
            metadata_schema, metadata_schema
        ) INTO hmac_key USING TG_TABLE_NAME, col_name;
        
        IF hmac_key IS NULL THEN
            RAISE EXCEPTION 'No HMAC key found for %.%', TG_TABLE_NAME, col_name;
        END IF;
        
        -- Verify HMAC
        data_bytes := col_value::bytea;
        IF length(data_bytes) < 45 THEN
            RAISE EXCEPTION 'Invalid encrypted data length for column %', col_name;
        END IF;
        
        stored_hmac := substring(data_bytes from 1 for 32);
        encrypted_payload := substring(data_bytes from 33);
        calculated_hmac := hmac(encrypted_payload, hmac_key, 'sha256');
        
        IF stored_hmac != calculated_hmac THEN
            RAISE EXCEPTION 'HMAC verification failed for column %', col_name;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

**3. Create metadata tables:**

```sql
-- Key storage table
CREATE TABLE encrypt.key_storage (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    master_key_arn VARCHAR(512) NOT NULL,
    encrypted_data_key TEXT NOT NULL,
    hmac_key BYTEA NOT NULL,
    key_spec VARCHAR(50) DEFAULT 'AES_256',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Encryption metadata table
CREATE TABLE encrypt.encryption_metadata (
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    encryption_algorithm VARCHAR(50) NOT NULL,
    key_id INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, column_name),
    FOREIGN KEY (key_id) REFERENCES encrypt.key_storage(id)
);
```

### Example Schema

```sql
-- PostgreSQL
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    pan encrypted_data,  -- Primary Account Number
    ssn encrypted_data,  -- Social Security Number
    email VARCHAR(100)
);

-- Add HMAC validation triggers for encrypted columns
CREATE TRIGGER validate_pan_hmac
    BEFORE INSERT OR UPDATE ON customers
    FOR EACH ROW 
    EXECUTE FUNCTION validate_encrypted_data_hmac('pan');

CREATE TRIGGER validate_ssn_hmac
    BEFORE INSERT OR UPDATE ON customers
    FOR EACH ROW 
    EXECUTE FUNCTION validate_encrypted_data_hmac('ssn');
```

**Important:** Each `encrypted_data` column requires a trigger that calls `validate_encrypted_data_hmac()` to verify HMAC integrity on insert/update.

### Configuration

```properties
wrapperPlugins=kmsEncryption
kmsKeyArn=arn:aws:kms:us-east-1:123456789012:key/abc-123
kmsRegion=us-east-1
encryptionMetadataSchema=encrypt
```

### Usage

Encryption and decryption are transparent:

```java
// Insert - automatic encryption
PreparedStatement stmt = conn.prepareStatement(
    "INSERT INTO customers (name, pan, ssn) VALUES (?, ?, ?)"
);
stmt.setString(1, "John Doe");
stmt.setString(2, "1234567890123456");  // Automatically encrypted
stmt.setString(3, "123-45-6789");       // Automatically encrypted
stmt.executeUpdate();

// Query - automatic decryption
ResultSet rs = conn.createStatement()
    .executeQuery("SELECT name, pan, ssn FROM customers");
while (rs.next()) {
    String pan = rs.getString("pan");  // Automatically decrypted
    String ssn = rs.getString("ssn");  // Automatically decrypted
}
```

### Spring Boot Integration

**1. Add dependencies to pom.xml:**

```xml
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-data-jpa</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
        <groupId>software.amazon.jdbc</groupId>
        <artifactId>aws-advanced-jdbc-wrapper</artifactId>
        <version>2.5.3</version>
    </dependency>
    <dependency>
        <groupId>software.amazon.awssdk</groupId>
        <artifactId>kms</artifactId>
        <version>2.41.20</version>
    </dependency>
    <dependency>
        <groupId>com.github.jsqlparser</groupId>
        <artifactId>jsqlparser</artifactId>
        <version>4.5</version>
    </dependency>
    <dependency>
        <groupId>org.postgresql</groupId>
        <artifactId>postgresql</artifactId>
    </dependency>
    <dependency>
        <groupId>com.zaxxer</groupId>
        <artifactId>HikariCP</artifactId>
    </dependency>
</dependencies>
```

**2. Create DataSource configuration:**

```java
@Configuration
public class DataSourceConfig {
    
    @Value("${DB_HOST:localhost}")
    private String dbHost;
    
    @Value("${DB_NAME:userdb}")
    private String dbName;
    
    @Value("${DB_USER:postgres}")
    private String dbUser;
    
    @Value("${DB_PASSWORD:postgres}")
    private String dbPassword;
    
    @Value("${KMS_KEY_ARN:}")
    private String kmsKeyArn;
    
    @Value("${AWS_REGION:us-east-1}")
    private String awsRegion;
    
    @Bean
    @Primary
    public DataSource dataSource() {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl("jdbc:aws-wrapper:postgresql://" + dbHost + ":5432/" + dbName);
        ds.setUsername(dbUser);
        ds.setPassword(dbPassword);
        ds.setDriverClassName("software.amazon.jdbc.Driver");
        
        ds.addDataSourceProperty("wrapperPlugins", "kmsEncryption");
        ds.addDataSourceProperty("kmsKeyArn", kmsKeyArn);
        ds.addDataSourceProperty("kmsRegion", awsRegion);
        ds.addDataSourceProperty("encryptionMetadataSchema", "encrypt");
        ds.addDataSourceProperty("wrapperTargetDriverDialect", "pgjdbc");
        ds.addDataSourceProperty("targetDriverClassName", "org.postgresql.Driver");
        
        return ds;
    }
    
    @Bean(name = "setupDataSource")
    public DataSource setupDataSource() {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl("jdbc:postgresql://" + dbHost + ":5432/" + dbName);
        ds.setUsername(dbUser);
        ds.setPassword(dbPassword);
        ds.setDriverClassName("org.postgresql.Driver");
        return ds;
    }
}
```

**3. Create database setup component:**

```java
@Component
public class DatabaseSetup implements CommandLineRunner {
    private final DataSource setupDataSource;
    
    @Value("${AWS_REGION:us-east-1}")
    private String region;
    
    @Value("${KMS_KEY_ARN:}")
    private String kmsKeyArn;
    
    public DatabaseSetup(@Qualifier("setupDataSource") DataSource setupDataSource) {
        this.setupDataSource = setupDataSource;
    }
    
    @Override
    public void run(String... args) throws Exception {
        JdbcTemplate jdbc = new JdbcTemplate(setupDataSource);
        
        // Create KMS key if not provided
        if (kmsKeyArn == null || kmsKeyArn.isEmpty()) {
            kmsKeyArn = createKmsKey();
            System.out.println("Created KMS Key: " + kmsKeyArn);
        }
        
        // Create schema and metadata tables
        jdbc.execute("CREATE SCHEMA IF NOT EXISTS encrypt");
        jdbc.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto");
        
        // Create encrypted_data domain
        jdbc.execute("CREATE DOMAIN IF NOT EXISTS encrypted_data AS bytea CHECK (length(VALUE) >= 45)");
        
        // Create helper functions (see SQL above)
        // Create metadata tables (see SQL above)
        
        // Generate and store data key
        KmsClient kms = KmsClient.builder().region(Region.of(region)).build();
        GenerateDataKeyResponse dataKey = kms.generateDataKey(
            GenerateDataKeyRequest.builder()
                .keyId(kmsKeyArn)
                .keySpec(DataKeySpec.AES_256)
                .build()
        );
        
        String encryptedDataKey = Base64.getEncoder().encodeToString(
            dataKey.ciphertextBlob().asByteArray()
        );
        byte[] hmacKey = generateHmacKey();
        
        Integer keyId = jdbc.queryForObject(
            "INSERT INTO encrypt.key_storage (name, master_key_arn, encrypted_data_key, hmac_key) " +
            "VALUES (?, ?, ?, ?) RETURNING id",
            Integer.class,
            "app-key",
            kmsKeyArn,
            encryptedDataKey,
            hmacKey
        );
        
        // Create application table
        jdbc.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                ssn encrypted_data
            )
            """);
        
        // Insert encryption metadata
        jdbc.update(
            "INSERT INTO encrypt.encryption_metadata (table_name, column_name, encryption_algorithm, key_id) " +
            "VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
            "users", "ssn", "AES-256-GCM", keyId
        );
        
        // Create trigger
        jdbc.execute("""
            CREATE TRIGGER IF NOT EXISTS validate_ssn_hmac
                BEFORE INSERT OR UPDATE ON users
                FOR EACH ROW 
                EXECUTE FUNCTION validate_encrypted_data_hmac('ssn')
            """);
    }
    
    private String createKmsKey() {
        KmsClient kms = KmsClient.builder().region(Region.of(region)).build();
        CreateKeyResponse response = kms.createKey(
            CreateKeyRequest.builder()
                .description("Encryption key for application data")
                .keyUsage(KeyUsageType.ENCRYPT_DECRYPT)
                .build()
        );
        return response.keyMetadata().arn();
    }
    
    private byte[] generateHmacKey() {
        SecureRandom random = new SecureRandom();
        byte[] key = new byte[32];
        random.nextBytes(key);
        return key;
    }
}
```

**4. Set environment variables:**

```bash
export DB_HOST=localhost
export DB_NAME=userdb
export DB_USER=postgres
export DB_PASSWORD=postgres
export AWS_REGION=us-east-1
# Optional: export KMS_KEY_ARN=arn:aws:kms:...
```

**5. Run the application:**

```bash
mvn spring-boot:run
```

The application will automatically create the KMS key (if not provided) and set up the database schema and metadata tables.

## Configuration Properties Reference

### Core Wrapper Properties

| Property | Description | Example                                        |
|----------|-------------|------------------------------------------------|
| `wrapperDialect` | Database dialect (auto-detected if not specified) | `aurora-pg`, `aurora-mysql`, `pgjdbc`, `mysql` |
| `wrapperPlugins` | Comma-separated list of plugins to load | `failover,iam,efm`                             |
| `wrapperDriverName` | Override driver name for compatibility | `PostgreSQL JDBC Driver`                       |
| `wrapperLoggerLevel` | Logging level | `SEVERE`, `WARNING`, `INFO`, `FINE`, `FINEST`  |
| `wrapperProfileName` | Configuration profile name | `production`, `development`                    |
| `autoSortWrapperPluginOrder` | Auto-sort plugins by dependency | `true`, `false`                                |
| `wrapperLogUnclosedConnections` | Log unclosed connections | `true`, `false`                                |

### Connection Properties

| Property | Description | Default | Example |
|----------|-------------|---------|---------|
| `loginTimeout` | Login timeout in seconds | Driver default | `30` |
| `connectTimeout` | Connection timeout in seconds | Driver default | `10` |
| `socketTimeout` | Socket read timeout in seconds | Driver default | `30` |
| `tcpKeepAlive` | Enable TCP keep-alive | Driver default | `true` |

### Failover Plugin Properties

| Property | Description | Default | Example |
|----------|-------------|---------|---------|
| `clusterId` | Aurora cluster identifier | Auto-detected | `my-cluster` |
| `clusterInstanceHostPattern` | Pattern for cluster instance hostnames | Auto-detected | `?.my-cluster.cluster-xyz.us-east-1.rds.amazonaws.com` |
| `failoverMode` | Failover mode | `strict-writer` | `strict-writer`, `reader-or-writer` |
| `enableClusterAwareFailover` | Enable cluster-aware failover | `true` | `true`, `false` |
| `failoverTimeoutMs` | Failover timeout in milliseconds | `60000` | `120000` |
| `failoverClusterTopologyRefreshRateMs` | Topology refresh rate | `5000` | `2000` |
| `failoverReaderConnectTimeoutMs` | Reader connection timeout | `30000` | `5000` |
| `failoverWriterReconnectIntervalMs` | Writer reconnect interval | `2000` | `5000` |

### Failover2 Plugin Properties

| Property | Description | Default | Example |
|----------|-------------|---------|---------|
| `clusterTopologyHighRefreshRateMs` | High-frequency topology refresh rate | `100` | `50` |
| `failoverReaderHostSelectorStrategy` | Reader selection strategy | `random` | `random`, `round-robin`, `least-connections` |

### Host Monitoring Plugin Properties

| Property | Description | Default | Example |
|----------|-------------|---------|---------|
| `failureDetectionEnabled` | Enable enhanced failure monitoring | `true` | `true`, `false` |
| `failureDetectionTime` | Time to detect failure (ms) | `30000` | `10000` |
| `failureDetectionInterval` | Monitoring interval (ms) | `5000` | `1000` |
| `failureDetectionCount` | Failed checks before marking unhealthy | `3` | `5` |
| `monitorDisposalTime` | Time before disposing inactive monitor (ms) | `60000` | `120000` |

### IAM Authentication Plugin Properties

| Property | Description | Default | Example |
|----------|-------------|---------|---------|
| `iamHost` | Database hostname for IAM token | Connection host | `mydb.cluster-xyz.us-east-1.rds.amazonaws.com` |
| `iamDefaultPort` | Default port for IAM authentication | `5432` (PG), `3306` (MySQL) | `5432` |
| `iamRegion` | AWS region for IAM | Auto-detected | `us-east-1` |
| `iamExpiration` | Token expiration time (seconds) | `900` | `600` |

### Secrets Manager Plugin Properties

| Property | Description | Example |
|----------|-------------|---------|
| `secretsManagerSecretId` | Secret ARN or name | `arn:aws:secretsmanager:us-east-1:123456789012:secret:mydb-abc123` |
| `secretsManagerRegion` | AWS region for Secrets Manager | `us-east-1` |

### AWS Credentials

| Property | Description | Example |
|----------|-------------|---------|
| `awsProfile` | AWS credentials profile name | `default`, `production` |

## Usage Examples

### Basic Aurora PostgreSQL with Failover
```properties
wrapperPlugins=failover,efm
clusterId=my-aurora-cluster
failoverTimeoutMs=60000
```

### IAM Authentication
```properties
wrapperPlugins=iam
iamRegion=us-east-1
iamDefaultPort=5432
```

### Secrets Manager Integration
```properties
wrapperPlugins=awsSecretsManager
secretsManagerSecretId=arn:aws:secretsmanager:us-east-1:123456789012:secret:db-creds
secretsManagerRegion=us-east-1
```

### Read/Write Splitting with Failover
```properties
wrapperPlugins=readWriteSplitting,failover,efm
clusterId=my-cluster
```

### RDS Multi-AZ with Fast Failover
```properties
wrapperPlugins=failover2,efm
clusterTopologyHighRefreshRateMs=100
failoverTimeoutMs=30000
```

## Common Configuration Patterns

### Production Aurora Cluster
```properties
wrapperPlugins=failover2,efm,iam
clusterId=prod-cluster
failoverTimeoutMs=30000
failureDetectionTime=10000
failureDetectionInterval=1000
iamRegion=us-east-1
wrapperLoggerLevel=WARNING
```

### Development with Secrets Manager
```properties
wrapperPlugins=awsSecretsManager
secretsManagerSecretId=dev-db-credentials
secretsManagerRegion=us-east-1
wrapperLoggerLevel=FINE
```

### High Availability Setup
```properties
wrapperPlugins=failover2,efm,readWriteSplitting
clusterId=ha-cluster
clusterTopologyHighRefreshRateMs=50
failureDetectionEnabled=true
failureDetectionTime=5000
failureDetectionInterval=500
failureDetectionCount=3
```

## Troubleshooting Guide

### Connection Issues
- Check `connectTimeout` and `socketTimeout` values
- Verify `clusterId` matches your Aurora cluster
- Ensure `iamRegion` is correct for IAM authentication
- Validate `secretsManagerSecretId` format and permissions

### Failover Issues
- Increase `failoverTimeoutMs` for slow networks
- Adjust `failureDetectionTime` for faster detection
- Verify `clusterInstanceHostPattern` matches your cluster DNS
- Check `enableClusterAwareFailover` is enabled

### Performance Issues
- Reduce `clusterTopologyHighRefreshRateMs` for faster updates
- Adjust `failureDetectionInterval` based on network latency
- Use `failover2` plugin for better performance
- Enable connection pooling (HikariCP recommended)

## Usage

To use these tools with Amazon Q, simply ask questions related to AWS Advanced JDBC Wrapper configuration, connection issues, or request examples for specific scenarios.

Example queries:
- "Generate a connection string for Aurora PostgreSQL with failover support"
- "What's the optimal plugin configuration for read/write splitting with Aurora MySQL?"
- "Help me troubleshoot my connection string for IAM authentication"
- "Show me an example of using the AWS JDBC Driver with HikariCP"
- "What parameters should I use for the failover2 plugin with RDS Multi-AZ DB Clusters?"
- "How do I enable Enhanced Failure Monitoring?"
- "Generate a connection string with telemetry enabled"
- "What's the difference between failover and failover2 plugins?"
- "How do I configure IAM authentication with connection pooling?"
- "What are the recommended settings for production Aurora clusters?"

## Implementation

The MCP server is implemented as a set of Python scripts that analyze the project structure, documentation, and code to provide contextual assistance.

### Files
- `mcp_server.py`: Main server implementation
- `connection_generator.py`: Tool for generating connection strings
- `plugin_analyzer.py`: Tool for analyzing and suggesting plugin configurations
- `troubleshooter.py`: Tool for identifying and resolving connection issues

## Development

To extend the MCP server with additional capabilities:

1. Add new tool implementations in the appropriate files
2. Update the tool registration in `mcp_server.py`
3. Document the new tools in this file

## License

This MCP server implementation is released under the Apache 2.0 license, consistent with the AWS Advanced JDBC Wrapper project.
