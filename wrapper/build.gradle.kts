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

plugins {
    checkstyle
    java
    jacoco
    id("com.diffplug.spotless")
    id("com.github.spotbugs")
    id("com.github.vlsi.gradle-extensions")
    id("com.github.vlsi.ide")
}

dependencies {
    implementation("org.checkerframework:checker-qual:3.26.0")
    compileOnly("software.amazon.awssdk:rds:2.17.285")
    compileOnly("com.zaxxer:HikariCP:4.0.3") // Version 4.+ is compatible with Java 8
    compileOnly("software.amazon.awssdk:secretsmanager:2.17.285")
    compileOnly("com.fasterxml.jackson.core:jackson-databind:2.13.4")

    testImplementation("org.junit.platform:junit-platform-commons:1.9.0")
    testImplementation("org.junit.platform:junit-platform-engine:1.9.0")
    testImplementation("org.junit.platform:junit-platform-launcher:1.9.0")
    testImplementation("org.junit.platform:junit-platform-suite-engine:1.9.0")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.0")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.9.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    testImplementation("org.apache.commons:commons-dbcp2:2.9.0")
    testImplementation("org.postgresql:postgresql:42.5.0")
    testImplementation("mysql:mysql-connector-java:8.0.30")
    testImplementation("org.mariadb.jdbc:mariadb-java-client:3.0.6")
    testImplementation("com.zaxxer:HikariCP:4.0.3") // Version 4.+ is compatible with Java 8
    testImplementation("org.springframework.boot:spring-boot-starter-jdbc:2.7.4")
    testImplementation("org.mockito:mockito-inline:4.8.0")
    testImplementation("software.amazon.awssdk:rds:2.17.285")
    testImplementation("software.amazon.awssdk:ec2:2.17.285")
    testImplementation("software.amazon.awssdk:secretsmanager:2.17.285")
    testImplementation("org.testcontainers:testcontainers:1.17.4")
    testImplementation("org.testcontainers:mysql:1.17.4")
    testImplementation("org.testcontainers:postgresql:1.17.4")
    testImplementation("org.testcontainers:mariadb:1.17.3")
    testImplementation("org.testcontainers:junit-jupiter:1.17.4")
    testImplementation("org.testcontainers:toxiproxy:1.17.4")
    testImplementation("org.apache.poi:poi-ooxml:5.2.2")
    testImplementation("org.slf4j:slf4j-simple:2.0.3")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.13.4")
}

repositories {
    mavenCentral()
}

tasks.check {
    dependsOn("jacocoTestCoverageVerification")
}

tasks.test {
    filter.excludeTestsMatching("integration.*")
}

java {
    withJavadocJar()
    withSourcesJar()
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(8))
    }
}

checkstyle {
    // Checkstyle versions 7.x, 8.x, and 9.x are supported by JRE version 8 and above.
    toolVersion = "9.3"
    // Fail the build if there is at least one Checkstyle warning.
    maxWarnings = 0
    configDirectory.set(File(rootDir, "config/checkstyle"))
    configFile = configDirectory.get().file("google_checks.xml").asFile
}

spotless {
    isEnforceCheck = false

    format("misc") {
        target("*.gradle", "*.md", ".gitignore")

        trimTrailingWhitespace()
        indentWithTabs()
        endWithNewline()
    }

    java {
        googleJavaFormat("1.7")
    }
}

spotbugs {
    ignoreFailures.set(true)
}

tasks.spotbugsMain {
    reports.create("html") {
        required.set(true)
        outputLocation.set(file("$buildDir/reports/spotbugsMain.html"))
        setStylesheet("fancy-hist.xsl")
    }
}
tasks.spotbugsTest {
    reports.create("html") {
        required.set(true)
        outputLocation.set(file("$buildDir/reports/spotbugsTest.html"))
        setStylesheet("fancy-hist.xsl")
    }
}

tasks.jacocoTestCoverageVerification {
    violationRules {
        rule {
            limit {
                // Coverage verification will pass if it is greater than or equal to 1%.
                minimum = "0.01".toBigDecimal()
            }
        }
    }
}

tasks.jar {
    from("${project.rootDir}") {
        include("README")
        include("LICENSE")
        include("THIRD-PARTY-LICENSES")
        into("META-INF/")
    }

    from("${buildDir}/META-INF/services/") {
        into("META-INF/services/")
    }

    doFirst {
        mkdir("${buildDir}/META-INF/services/")
        val driverFile = File("${buildDir}/META-INF/services/java.sql.Driver")
        if (driverFile.createNewFile()) {
            driverFile.writeText("software.amazon.jdbc.Driver")
        }
    }
}

tasks.withType<Test> {
    useJUnitPlatform()

    systemProperty("java.util.logging.config.file", "${project.buildDir}/resources/test/logging-test.properties")
}

// Run Aurora Postgres integrations tests in container
tasks.register<Test>("test-integration-aurora-postgres") {
    group = "verification"
    filter.includeTestsMatching("integration.host.AuroraPostgresContainerTest.runTestInContainer")
}

tasks.register<Test>("test-performance-aurora-postgres") {
    group = "verification"
    filter.includeTestsMatching("integration.host.AuroraPostgresContainerTest.runPerformanceTestInContainer")
}

// Run standard Postgres tests in container
tasks.register<Test>("test-integration-standard-postgres") {
    group = "verification"
    filter.includeTestsMatching("integration.host.StandardPostgresContainerTest.runTestInContainer")
}

// Run Aurora Postgres integration tests in container with debugger
tasks.register<Test>("debug-integration-aurora-postgres") {
    group = "verification"
    filter.includeTestsMatching("integration.host.AuroraPostgresContainerTest.debugTestInContainer")
}

tasks.register<Test>("debug-performance-aurora-postgres") {
    group = "verification"
    filter.includeTestsMatching("integration.host.AuroraPostgresContainerTest.debugPerformanceTestInContainer")
}

// Run standard Postgres integration tests in container with debugger
tasks.register<Test>("debug-integration-standard-postgres") {
    group = "verification"
    filter.includeTestsMatching("integration.host.StandardPostgresContainerTest.debugTestInContainer")
}

// Run Aurora Mysql integrations tests in container
tasks.register<Test>("test-integration-aurora-mysql") {
    group = "verification"
    filter.includeTestsMatching("integration.host.AuroraMysqlContainerTest.runTestInContainer")
}

// Run standard Mysql tests in container
tasks.register<Test>("test-integration-standard-mysql") {
    group = "verification"
    filter.includeTestsMatching("integration.host.StandardMysqlContainerTest.runTestInContainer")
}

// Run Aurora Mysql integration tests in container with debugger
tasks.register<Test>("debug-integration-aurora-mysql") {
    group = "verification"
    filter.includeTestsMatching("integration.host.AuroraMysqlContainerTest.debugTestInContainer")
}

// Run standard Mysql integration tests in container with debugger
tasks.register<Test>("debug-integration-standard-mysql") {
    group = "verification"
    filter.includeTestsMatching("integration.host.StandardMysqlContainerTest.debugTestInContainer")
}

// Run standard Mariadb tests in container
tasks.register<Test>("test-integration-standard-mariadb") {
    group = "verification"
    filter.includeTestsMatching("integration.host.StandardMariadbContainerTest.runTestInContainer")
}

// Run standard Mariadb integration tests in container with debugger
tasks.register<Test>("debug-integration-standard-mariadb") {
    group = "verification"
    filter.includeTestsMatching("integration.host.StandardMariadbContainerTest.debugTestInContainer")
}

tasks.withType<Test> {
    dependsOn("jar")
    this.testLogging {
        this.showStandardStreams = true
    }
    useJUnitPlatform()
}
