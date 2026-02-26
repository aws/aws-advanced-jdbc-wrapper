# AWS JDBC Encryption MCP Server

This Model Context Protocol (MCP) configuration enables Claude to assist with building applications using the AWS Advanced JDBC Wrapper's KMS encryption features.

## Setup

### 1. Add to Claude Desktop Configuration

Add this to your Claude Desktop MCP configuration file:

**macOS/Linux:** `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "aws-jdbc-encryption": {
      "command": "npx",
      "args": [
        "-y",
        "@modelcontextprotocol/server-filesystem",
        "/path/to/aws-advanced-jdbc-wrapper"
      ]
    }
  }
}
```

Replace `/path/to/aws-advanced-jdbc-wrapper` with the actual path to this repository.

### 2. Restart Claude Desktop

After adding the configuration, restart Claude Desktop to load the MCP server.

## Available Prompts

### Setup Encrypted Database
Generate SQL and Java code to set up database encryption for specific tables and columns.

**Example:**
```
Use the setup-encrypted-database prompt with:
- table_name: users
- encrypted_columns: ssn,credit_card
- kms_key_arn: arn:aws:kms:us-east-1:123456789012:key/abc-123
```

### Create Encrypted Application
Generate a complete Java application with encrypted database access.

**Example:**
```
Use the create-encrypted-application prompt with:
- application_type: spring-boot
- database_url: jdbc:postgresql://localhost:5432/mydb
- kms_key_arn: arn:aws:kms:us-east-1:123456789012:key/abc-123
```

### Add Encryption to Existing Column
Generate migration code to add encryption to an existing database column.

**Example:**
```
Use the add-encryption-to-existing-column prompt with:
- table_name: customers
- column_name: ssn
- kms_key_arn: arn:aws:kms:us-east-1:123456789012:key/abc-123
```

### Troubleshoot Encryption
Get help diagnosing encryption-related issues.

**Example:**
```
Use the troubleshoot-encryption prompt with:
- error_message: HMAC verification failed for column users.ssn
```

## Available Tools

### generate_encryption_setup_sql
Generates complete SQL scripts for database encryption setup including:
- Metadata schema creation
- Custom type installation
- Key storage tables
- Encryption metadata tables
- Triggers for HMAC validation

### generate_java_encryption_code
Generates Java code for encrypted database operations with support for:
- Spring Boot applications
- Standalone JDBC applications
- HikariCP connection pooling
- CRUD operations with automatic encryption/decryption

### validate_encryption_config
Validates encryption configuration including:
- JDBC connection properties
- Metadata schema setup
- Key storage configuration
- Column encryption metadata

## Usage Examples

### Example 1: Create New Encrypted Application

```
I need to create a Spring Boot application that stores user data with encrypted SSN and credit card fields. 
The database is PostgreSQL on RDS, and I have a KMS key arn:aws:kms:us-east-1:123456789012:key/abc-123.
```

Claude will:
1. Generate the database setup SQL
2. Create Spring Boot application structure
3. Configure encryption properties
4. Generate entity classes with encrypted fields
5. Create repository and service layers
6. Provide testing examples

### Example 2: Add Encryption to Existing Table

```
I have an existing 'customers' table with a 'ssn' column that needs to be encrypted. 
How do I migrate this without data loss?
```

Claude will:
1. Generate migration SQL to add encrypted_data type
2. Create backup procedures
3. Generate data migration script
4. Update application code
5. Provide rollback procedures

### Example 3: Troubleshoot HMAC Error

```
I'm getting "HMAC verification failed" errors when reading encrypted data. 
The error shows: stored=5c78653337656133, calculated=e37ea320f45ffc57
```

Claude will:
1. Analyze the error pattern
2. Check for hex encoding issues
3. Verify HMAC key configuration
4. Suggest fixes and validation steps

## Resources Available to Claude

When using this MCP server, Claude has access to:

1. **Database Encryption Setup Guide** - Complete setup documentation
2. **KMS Encryption Plugin Documentation** - Plugin configuration reference
3. **Integration Test Examples** - Working code examples
4. **Source Code** - Implementation reference for advanced use cases

## Requirements

- Node.js and npm (for MCP server)
- AWS credentials configured
- PostgreSQL database (Aurora or RDS)
- AWS KMS key

## Security Notes

- Never commit KMS key ARNs to version control
- Use environment variables for sensitive configuration
- Restrict access to encryption metadata schema
- Enable CloudTrail logging for KMS operations
- Use IAM roles instead of access keys when possible

## Troubleshooting

### MCP Server Not Loading
- Check Claude Desktop logs: `~/Library/Logs/Claude/mcp*.log`
- Verify Node.js is installed: `node --version`
- Ensure path to repository is correct

### Claude Can't Access Files
- Verify the filesystem path in the MCP configuration
- Check file permissions on the repository directory
- Restart Claude Desktop after configuration changes

### Generated Code Doesn't Compile
- Ensure you have the correct AWS JDBC Wrapper version
- Check that all dependencies are in your pom.xml or build.gradle
- Verify AWS SDK version compatibility

## Support

For issues with:
- **MCP Configuration**: Check Claude Desktop documentation
- **Encryption Plugin**: See [GitHub Issues](https://github.com/aws/aws-advanced-jdbc-wrapper/issues)
- **AWS KMS**: Refer to [AWS KMS Documentation](https://docs.aws.amazon.com/kms/)
