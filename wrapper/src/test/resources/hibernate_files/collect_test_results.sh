#!/bin/bash

find /app/hibernate-orm/*/target/reports/tests -type d -name test -print0 | tar -czvf  "/app/build/test-results/hibernate-orm-reports_$(date '+%Y-%m-%d_%H-%M-%S').tar.gz" --files-from -
find /app/hibernate-orm/*/target/test-results/test -type d -name test -print0 | tar -czvf  "/app/build/test-results/hibernate-orm-test-results_$(date '+%Y-%m-%d_%H-%M-%S').tar.gz" --files-from -
