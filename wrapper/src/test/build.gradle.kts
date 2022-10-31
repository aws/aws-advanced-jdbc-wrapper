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

import org.gradle.api.tasks.testing.logging.TestExceptionFormat.*
import org.gradle.api.tasks.testing.logging.TestLogEvent.*

plugins {
    java
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.junit.platform:junit-platform-commons:1.8.2")
    testImplementation("org.junit.platform:junit-platform-engine:1.8.2")
    testImplementation("org.junit.platform:junit-platform-launcher:1.8.2")
    testImplementation("org.junit.platform:junit-platform-suite-engine:1.8.2")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.8.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    testImplementation("org.apache.commons:commons-dbcp2:2.8.0")
    testImplementation("org.postgresql:postgresql:42.5.0")
    testImplementation("mysql:mysql-connector-java:8.0.30")
    testImplementation("org.mariadb.jdbc:mariadb-java-client:3.+")
    testImplementation("com.zaxxer:HikariCP:4.+") // version 4.+ is compatible with Java 8
    testImplementation("org.springframework.boot:spring-boot-starter-jdbc:2.7.+")
    testImplementation("org.mockito:mockito-inline:4.+")
    testImplementation("software.amazon.awssdk:rds:2.17.285")
    testImplementation("software.amazon.awssdk:ec2:2.17.285")
    testImplementation("org.testcontainers:testcontainers:1.17.+")
    testImplementation("org.testcontainers:mysql:1.17.+")
    testImplementation("org.testcontainers:postgresql:1.17.+")
    testImplementation("org.testcontainers:mariadb:1.17.+")
    testImplementation("org.testcontainers:junit-jupiter:1.17.+")
    testImplementation("org.testcontainers:toxiproxy:1.17.+")
    testImplementation("org.apache.poi:poi-ooxml:5.2.2")
    testImplementation("org.slf4j:slf4j-simple:1.7.+")
}

tasks.withType<Test> {
    useJUnitPlatform()

    testLogging {
        events(FAILED, SKIPPED)
        showStandardStreams = true
        exceptionFormat = FULL
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }

    systemProperty("java.util.logging.config.file", "./test/resources/logging-test.properties")
}

// Integration tests are run in a specific order.
// To add more tests, see integration.container.aurora.postgres.AuroraPostgresTestSuite.java
tasks.register<Test>("in-container-aurora-postgres") {
    filter.includeTestsMatching("integration.container.aurora.postgres.AuroraPostgresTestSuite")
}

tasks.register<Test>("in-container-aurora-postgres-performance") {
    filter.includeTestsMatching("integration.container.aurora.postgres.AuroraPostgresPerformanceTest")
    filter.includeTestsMatching("integration.container.aurora.postgres.AuroraAdvancedPerformanceTest")
}

// Integration tests are run in a specific order.
// To add more tests, see integration.container.standard.postgres.StandardPostgresTestSuite.java
tasks.register<Test>("in-container-standard-postgres") {
    filter.includeTestsMatching("integration.container.standard.postgres.StandardPostgresTestSuite")
}

// Integration tests are run in a specific order.
// To add more tests, see integration.container.aurora.mysql.mysqldriver.AuroraMysqlTestSuite.java
// and integration.container.aurora.mysql.mariadbdriver.MariadbAuroraMysqlTestSuite.java
tasks.register<Test>("in-container-aurora-mysql") {
    filter.includeTestsMatching("integration.container.aurora.mysql.mysqldriver.MysqlAuroraMysqlTestSuite")
    filter.includeTestsMatching("integration.container.aurora.mysql.mariadbdriver.MariadbAuroraMysqlTestSuite")
}

// Integration tests are run in a specific order.
// To add more tests, see integration.container.standard.mysql.mysqldriver.StandardMysqlTestSuite.java
// and integration.container.standard.mysql.mariadbdriver.MariadbStandardMysqlTestSuite.java
tasks.register<Test>("in-container-standard-mysql") {
    filter.includeTestsMatching("integration.container.standard.mysql.mysqldriver.MysqlStandardMysqlTestSuite")
    filter.includeTestsMatching("integration.container.standard.mysql.mariadbdriver.MariadbStandardMysqlTestSuite")
}

tasks.register<Test>("in-container-standard-mariadb") {
    filter.includeTestsMatching("integration.container.standard.mariadb.StandardMariadbTestSuite")
}

tasks.withType<Test> {
    testClassesDirs += fileTree("./libs") { include("*.jar") } + project.files("./test")
    classpath += fileTree("./libs") { include("*.jar") } + project.files("./test")
    outputs.upToDateWhen { false }
    useJUnitPlatform()
}
