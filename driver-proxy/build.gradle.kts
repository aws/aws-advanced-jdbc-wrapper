/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

import org.jdbcProxyDriver.buildtools.JavaCommentPreprocessorTask

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
    implementation("org.checkerframework:checker-qual:3.22.2")

    testImplementation("org.slf4j:slf4j-api:1.7.30")
    testImplementation("ch.qos.logback:logback-classic:0.9.26")

    testImplementation("org.junit.platform:junit-platform-commons:1.8.2")
    testImplementation("org.junit.platform:junit-platform-engine:1.8.2")
    testImplementation("org.junit.platform:junit-platform-launcher:1.8.2")
    testImplementation("org.junit.platform:junit-platform-suite-engine:1.8.2")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.8.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    testImplementation("org.postgresql:postgresql:42.+")
    testImplementation("mysql:mysql-connector-java:8.0.+")
    testImplementation("com.zaxxer:HikariCP:4.+") // version 4.+ is compatible with Java 8
    testImplementation("org.springframework.boot:spring-boot-starter-jdbc:2.7.+")
    testImplementation("org.mockito:mockito-inline:4.+")
    testImplementation("software.amazon.awssdk:rds:2.17.165")
    testImplementation("software.amazon.awssdk:ec2:2.17.165")
    testImplementation("org.testcontainers:testcontainers:1.17.2")
    testImplementation("org.testcontainers:mysql:1.17.2")
    testImplementation("org.testcontainers:postgresql:1.17.2")
    testImplementation("org.testcontainers:junit-jupiter:1.17.2")
    testImplementation("org.testcontainers:toxiproxy:1.17.2")
}

tasks.check {
    dependsOn("jacocoTestCoverageVerification")
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

val preprocessVersion by tasks.registering(JavaCommentPreprocessorTask::class) {
    baseDir.set(projectDir)
    sourceFolders.add("src/main/version/")
}

ide {
    generatedJavaSources(
        preprocessVersion,
        preprocessVersion.get().outputDirectory.get().asFile,
        sourceSets.main
    )
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()

    fun passProperty(name: String, default: String? = null) {
        val value = System.getProperty(name) ?: default
        value?.let { systemProperty(name, it) }
    }

    passProperty("mysqlServerName")
    passProperty("mysqlUser")
    passProperty("mysqlPassword")
    passProperty("mysqlDatabase")

    passProperty("postgresqlServerName")
    passProperty("postgresqlUser")
    passProperty("postgresqlPassword")
    passProperty("postgresqlDatabase")

    systemProperty("java.util.logging.config.file", "${project.buildDir}/resources/test/logging-test.properties")
}

// Run integrations tests in container
// Environment is being configured and started
tasks.register<Test>("test-integration-aurora-postgres") {
    group = "verification"
    filter.includeTestsMatching("integration.host.AuroraPostgresContainerTest.runTestInContainer")
}

tasks.register<Test>("test-performance-aurora-postgres") {
    group = "verification"
    filter.includeTestsMatching("integration.host.AuroraPostgresContainerTest.runPerformanceTestInContainer")
}

// Run standard Postgres tests in container
// Environment (like supplementary containers) should be up and running!
tasks.register<Test>("test-integration-standard-postgres") {
    group = "verification"
    filter.includeTestsMatching("integration.host.StandardPostgresContainerTest.runTestInContainer")
}

// Run integration tests in container with debugger
// Environment is being configured and started
tasks.register<Test>("debug-integration-aurora-postgres") {
    group = "verification"
    filter.includeTestsMatching("integration.host.AuroraPostgresContainerTest.debugTestInContainer")
}

tasks.register<Test>("debug-performance-aurora-postgres") {
    group = "verification"
    filter.includeTestsMatching("testsuite.integration.host.AuroraPostgresContainerTest.debugPerformanceTestInContainer")
}

tasks.register<Test>("debug-integration-standard-postgres") {
    group = "verification"
    filter.includeTestsMatching("integration.host.StandardPostgresContainerTest.debugTestInContainer")
}

// Integration tests are run in a specific order.
// To add more tests, see testsuite.integration.container.aurora.postgres.AuroraPostgresTestSuite.java
tasks.register<Test>("in-container-aurora-postgres") {
    filter.includeTestsMatching("integration.container.aurora.postgres.AuroraPostgresTestSuite")
}

tasks.register<Test>("in-container-aurora-postgres-performance") {
    filter.includeTestsMatching("integration.container.aurora.postgres.AuroraPostgresPerformanceTest")
}

// Integration tests are run in a specific order.
// To add more tests, see integration.container.standard.postgres.StandardPostgresTestSuite.java
tasks.register<Test>("in-container-standard-postgres") {
    filter.includeTestsMatching("integration.container.standard.postgres.StandardPostgresTestSuite")
}

tasks.withType<Test> {
    this.testLogging {
        this.showStandardStreams = true
    }
    useJUnitPlatform()
}
