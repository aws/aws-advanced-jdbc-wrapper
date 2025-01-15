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

package software.amazon.jdbc.dialect;

public class DialectCodes {
  public static final String GLOBAL_AURORA_MYSQL = "global-aurora-mysql";
  public static final String AURORA_MYSQL = "aurora-mysql";
  public static final String RDS_MYSQL = "rds-mysql";
  public static final String MYSQL = "mysql";
  // https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html
  public static final String RDS_MULTI_AZ_MYSQL_CLUSTER = "rds-multi-az-mysql-cluster";

  public static final String GLOBAL_AURORA_PG = "global-aurora-pg";
  public static final String AURORA_PG = "aurora-pg";
  public static final String RDS_PG = "rds-pg";
  // https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html
  public static final String RDS_MULTI_AZ_PG_CLUSTER = "rds-multi-az-pg-cluster";
  public static final String PG = "pg";

  public static final String MARIADB = "mariadb";

  public static final String UNKNOWN = "unknown";
  public static final String CUSTOM = "custom";
}
