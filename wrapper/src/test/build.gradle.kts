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
    testImplementation("org.junit.platform:junit-platform-commons:1.11.3")
    testImplementation("org.junit.platform:junit-platform-engine:1.11.0")
    testImplementation("org.junit.platform:junit-platform-launcher:1.11.3")
    testImplementation("org.junit.platform:junit-platform-suite-engine:1.11.3")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.11.3")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.10.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    testImplementation("org.apache.commons:commons-dbcp2:2.12.0")
    testImplementation("org.postgresql:postgresql:42.7.4")
    testImplementation("com.mysql:mysql-connector-j:9.1.0")
    testImplementation("org.mariadb.jdbc:mariadb-java-client:3.4.1")
    testImplementation("com.zaxxer:HikariCP:4.0.3") // Version 4.+ is compatible with Java 8
    testImplementation("org.springframework.boot:spring-boot-starter-jdbc:2.7.13") // 2.7.13 is the last version compatible with Java 8
    testImplementation("org.mockito:mockito-inline:4.11.0") // 4.11.0 is the last version compatible with Java 8
    testImplementation("software.amazon.awssdk:ec2:2.29.34")
    testImplementation("software.amazon.awssdk:rds:2.29.34")
    testImplementation("software.amazon.awssdk:sts:2.29.34")
    // Note: all org.testcontainers dependencies should have the same version
    testImplementation("org.testcontainers:testcontainers:1.20.4")
    testImplementation("org.testcontainers:mysql:1.20.4")
    testImplementation("org.testcontainers:postgresql:1.20.4")
    testImplementation("org.testcontainers:mariadb:1.20.4")
    testImplementation("org.testcontainers:junit-jupiter:1.20.4")
    testImplementation("org.testcontainers:toxiproxy:1.20.4")
    testImplementation("org.apache.poi:poi-ooxml:5.3.0")
    testImplementation("org.slf4j:slf4j-simple:2.0.13")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.17.1")
    testImplementation("com.amazonaws:aws-xray-recorder-sdk-core:2.18.2")
    testImplementation("io.opentelemetry:opentelemetry-sdk:1.42.1")
    testImplementation("io.opentelemetry:opentelemetry-sdk-metrics:1.43.0")
    testImplementation("io.opentelemetry:opentelemetry-exporter-otlp:1.44.1")
    testImplementation("de.vandermeer:asciitable:0.3.2")
}

tasks.withType<Test> {

    testClassesDirs += fileTree("./libs") { include("*.jar") } + project.files("./test")
    classpath += fileTree("./libs") { include("*.jar") } + project.files("./test")
    outputs.upToDateWhen { false }

    useJUnitPlatform {
        System.getProperty("test-include-tags")?.split(",")?.forEach { tag ->
            includeTags(tag)
            println("Include tests with tag: $tag")
        }
        System.getProperty("test-exclude-tags")?.split(",")?.forEach { tag ->
            excludeTags(tag)
            println("Exclude tests with tag: $tag")
        }
    }

    testLogging {
        events(PASSED, FAILED, SKIPPED)
        showStandardStreams = true
        exceptionFormat = FULL
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }

    systemProperty("java.util.logging.config.file", "./test/resources/logging-test.properties")
    systemProperty("junit.jupiter.params.displayname.default", "{displayName} - {arguments}")

    reports.junitXml.required.set(true)
    reports.junitXml.outputLocation.set(file("${project.layout.buildDirectory.get()}/test-results/container-" + System.currentTimeMillis()))

    reports.html.required.set(false)
}

tasks.register<Test>("in-container") {
    filter.excludeTestsMatching("software.*") // exclude unit tests

    // modify below filter to select specific integration tests
    // see https://docs.gradle.org/current/javadoc/org/gradle/api/tasks/testing/TestFilter.html
    filter.includeTestsMatching("integration.container.tests.*")
}
