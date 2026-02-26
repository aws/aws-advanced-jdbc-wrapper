#!/bin/bash
# Revert all non-encryption-related changes (checkstyle fixes)

# Get list of files to revert (exclude encryption-related)
FILES=$(git diff origin/main --name-only | grep -v -E "(encryption|kms|KmsEncryption|Encryption|parser|Parser|JSQLParser|Dialect|HMAC|docs/|\.github/|\.md$|environment\.txt|build\.gradle)")

for file in $FILES; do
  if [ -f "$file" ]; then
    echo "Reverting: $file"
    git checkout origin/main -- "$file"
  fi
done

echo ""
echo "Reverted $(echo "$FILES" | wc -l) files to origin/main"
echo ""
echo "Encryption-related changes preserved:"
git diff origin/main --name-only | grep -E "(encryption|kms|parser|JSQLParser|Dialect|HMAC|docs/|\.github/|\.md$)"
