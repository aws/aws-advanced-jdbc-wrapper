#!/bin/bash
#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Grants the XA_RECOVER_ADMIN dynamic privilege (required since MySQL 8.0 for "XA RECOVER") to the
# test user, so XADataSource integration tests can call XAResource.recover().
set -e

if [ -n "${MYSQL_USER}" ]; then
  mysql -u root -p"${MYSQL_ROOT_PASSWORD}" \
    -e "GRANT XA_RECOVER_ADMIN ON *.* TO '${MYSQL_USER}'@'%'; FLUSH PRIVILEGES;"
fi