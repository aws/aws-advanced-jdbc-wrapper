/*
*    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* 
*    Licensed under the Apache License, Version 2.0 (the "License").
*    You may not use this file except in compliance with the License.
*    You may obtain a copy of the License at
* 
*    http://www.apache.org/licenses/LICENSE-2.0
* 
*    Unless required by applicable law or agreed to in writing, software
*    distributed under the License is distributed on an "AS IS" BASIS,
*    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*    See the License for the specific language governing permissions and
*    limitations under the License.
*/

package integration.util;

public class TestSettings {

  public static final String mysqlServerName = System.getProperty("mysqlServerName");
  public static final String mysqlUser = System.getProperty("mysqlUser");
  public static final String mysqlPassword = System.getProperty("mysqlPassword");
  public static final String mysqlDatabase = System.getProperty("mysqlDatabase");

  public static final String postgresqlServerName = System.getProperty("postgresqlServerName");
  public static final String postgresqlUser = System.getProperty("postgresqlUser");
  public static final String postgresqlPassword = System.getProperty("postgresqlPassword");
  public static final String postgresqlDatabase = System.getProperty("postgresqlDatabase");
}
