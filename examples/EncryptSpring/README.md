# Encrypted Spring Boot Application

Spring Boot application demonstrating AWS KMS encryption for database columns using the AWS Advanced JDBC Wrapper.

## Prerequisites

- Java 17+
- PostgreSQL database
- AWS account with KMS access
- AWS credentials configured

## Setup

### 1. Create KMS Key

Create a KMS key in AWS:

```bash
aws kms create-key --description "Database encryption key"
```

Note the `KeyId` from the output. Get the full ARN:

```bash
aws kms describe-key --key-id <KeyId>
```

### 2. Configure AWS Credentials

Set up AWS credentials using one of these methods:

**Option A: AWS CLI Configuration**
```bash
aws configure
```

**Option B: Environment Variables**
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
```

**Option C: IAM Role** (recommended for EC2/ECS)
- Attach IAM role with `kms:Decrypt` and `kms:GenerateDataKey` permissions

### 3. Set Application Properties

Create environment variables or update `application.properties`:

```bash
# Database Configuration
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=mydb
export DB_USERNAME=postgres
export DB_PASSWORD=your_password

# AWS KMS Configuration
export AWS_KMS_KEY_ID=arn:aws:kms:us-east-1:123456789012:key/your-key-id
export AWS_KMS_ENCRYPTION_COLUMNS=users.ssn
```

### 4. Setup Database

Ensure PostgreSQL is running and create the database:

```bash
createdb mydb
```

The application will automatically:
- Install `pgcrypto` extension
- Create `encrypted_data` type
- Create HMAC validation functions
- Create `users` table with encrypted `ssn` column

### 5. Grant KMS Permissions

Ensure your AWS user/role has these KMS permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kms:Decrypt",
        "kms:GenerateDataKey"
      ],
      "Resource": "arn:aws:kms:us-east-1:123456789012:key/your-key-id"
    }
  ]
}
```

## Running

```bash
./gradlew bootRun
```

Or with inline properties:

```bash
./gradlew bootRun --args="\
  --aws.kms.key.id=arn:aws:kms:us-east-1:123456789012:key/your-key-id \
  --db.password=your_password"
```

## Testing

The application exposes REST endpoints:

```bash
# Create user with encrypted SSN
curl -X POST http://localhost:8080/users \
  -H "Content-Type: application/json" \
  -d '{"name":"John Doe","ssn":"123-45-6789"}'

# Get all users (SSN automatically decrypted)
curl http://localhost:8080/users

# Get specific user
curl http://localhost:8080/users/1
```

## Configuration Reference

| Property | Environment Variable | Default | Description |
|----------|---------------------|---------|-------------|
| `db.host` | `DB_HOST` | `localhost` | Database host |
| `db.port` | `DB_PORT` | `5432` | Database port |
| `db.name` | `DB_NAME` | `mydb` | Database name |
| `db.username` | `DB_USERNAME` | `postgres` | Database user |
| `db.password` | `DB_PASSWORD` | `password` | Database password |
| `aws.kms.key.id` | `AWS_KMS_KEY_ID` | - | KMS key ARN |
| `aws.kms.encryption.columns` | `AWS_KMS_ENCRYPTION_COLUMNS` | `users.ssn` | Columns to encrypt |
| `aws.jdbc.wrapper.plugins` | `AWS_JDBC_WRAPPER_PLUGINS` | `kmsEncryption` | JDBC wrapper plugins |

## Security Notes

- Never commit KMS key ARNs or credentials to version control
- Use IAM roles instead of access keys when possible
- Enable CloudTrail logging for KMS operations
- Restrict database access to application service accounts
- Use separate KMS keys for different environments

## Troubleshooting

**KMS Access Denied**
- Verify AWS credentials are configured
- Check KMS key policy allows your IAM user/role
- Ensure key is in the same region as configured

**Database Connection Failed**
- Verify PostgreSQL is running
- Check database credentials
- Ensure database exists

**HMAC Verification Failed**
- Check that `pgcrypto` extension is installed
- Verify encryption metadata tables exist
- Ensure HMAC keys are properly stored
