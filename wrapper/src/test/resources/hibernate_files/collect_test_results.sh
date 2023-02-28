#!/bin/sh

find /app/hibernate-orm/*/target/reports/tests -type d -name test -print0 | tar -czvf  "/app/build/test-results/hibernate-orm_$(date '+%Y-%m-%d_%H-%M-%S').tar.gz" --files-from -
