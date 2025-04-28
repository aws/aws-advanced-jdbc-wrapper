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
    id("biz.aQute.bnd.builder")
    id("com.diffplug.spotless") version "6.13.0" // 6.13.0 is the last version that is compatible with Java 8
    id("com.github.spotbugs")
    id("com.github.vlsi.gradle-extensions")
    id("com.github.vlsi.ide")
    id("com.kncept.junit.reporter")
}

dependencies {
    implementation("org.checkerframework:checker-qual:3.49.2")
    compileOnly("org.apache.httpcomponents:httpclient:4.5.14")
    compileOnly("software.amazon.awssdk:rds:2.31.12")
    compileOnly("software.amazon.awssdk:auth:2.31.16") // Required for IAM (light implementation)
    compileOnly("software.amazon.awssdk:http-client-spi:2.31.17") // Required for IAM (light implementation)
    compileOnly("software.amazon.awssdk:sts:2.30.27")
    compileOnly("com.zaxxer:HikariCP:4.0.3") // Version 4.+ is compatible with Java 8
    compileOnly("com.mchange:c3p0:0.10.2")
    compileOnly("software.amazon.awssdk:secretsmanager:2.31.12")
    compileOnly("com.fasterxml.jackson.core:jackson-databind:2.19.0")
    compileOnly("com.mysql:mysql-connector-j:9.2.0")
    compileOnly("org.postgresql:postgresql:42.7.5")
    compileOnly("org.mariadb.jdbc:mariadb-java-client:3.5.3")
    compileOnly("org.osgi:org.osgi.core:6.0.0")
    compileOnly("org.osgi:org.osgi.core:6.0.0")
    compileOnly("com.amazonaws:aws-xray-recorder-sdk-core:2.18.2")
    compileOnly("io.opentelemetry:opentelemetry-api:1.49.0")
    compileOnly("io.opentelemetry:opentelemetry-sdk:1.49.0")
    compileOnly("io.opentelemetry:opentelemetry-sdk-metrics:1.49.0")
    compileOnly("org.jsoup:jsoup:1.19.1")
    compileOnly("org.jetbrains.kotlin:kotlin-stdlib:2.1.20")

    testImplementation("org.junit.platform:junit-platform-commons:1.12.2")
    testImplementation("org.junit.platform:junit-platform-engine:1.11.0")
    testImplementation("org.junit.platform:junit-platform-launcher:1.12.2")
    testImplementation("org.junit.platform:junit-platform-suite-engine:1.12.2")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.12.1")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.12.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    testImplementation("org.apache.commons:commons-dbcp2:2.13.0")
    testImplementation("org.postgresql:postgresql:42.7.5")
    testImplementation("com.mysql:mysql-connector-j:9.2.0")
    testImplementation("org.mariadb.jdbc:mariadb-java-client:3.5.3")
    testImplementation("com.zaxxer:HikariCP:4.0.3") // Version 4.+ is compatible with Java 8
    testImplementation("com.mchange:c3p0:0.10.2")
    testImplementation("org.springframework.boot:spring-boot-starter-jdbc:2.7.13") // 2.7.13 is the last version compatible with Java 8
    testImplementation("org.mockito:mockito-inline:4.11.0") // 4.11.0 is the last version compatible with Java 8
    testImplementation("software.amazon.awssdk:rds:2.31.12")
    testImplementation("software.amazon.awssdk:auth:2.31.16") // Required for IAM (light implementation)
    testImplementation("software.amazon.awssdk:http-client-spi:2.31.17") // Required for IAM (light implementation)
    testImplementation("software.amazon.awssdk:ec2:2.31.31")
    testImplementation("software.amazon.awssdk:secretsmanager:2.31.12")
    testImplementation("software.amazon.awssdk:sts:2.30.27")
    // Note: all org.testcontainers dependencies should have the same version
    testImplementation("org.testcontainers:testcontainers:1.20.6")
    testImplementation("org.testcontainers:mysql:1.20.6")
    testImplementation("org.testcontainers:postgresql:1.20.6")
    testImplementation("org.testcontainers:mariadb:1.20.6")
    testImplementation("org.testcontainers:junit-jupiter:1.20.6")
    testImplementation("org.testcontainers:toxiproxy:1.20.6")
    testImplementation("eu.rekawek.toxiproxy:toxiproxy-java:2.1.7")
    testImplementation("org.apache.poi:poi-ooxml:5.4.1")
    testImplementation("org.slf4j:slf4j-simple:2.0.17")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.19.0")
    testImplementation("com.amazonaws:aws-xray-recorder-sdk-core:2.18.2")
    testImplementation("io.opentelemetry:opentelemetry-api:1.49.0")
    testImplementation("io.opentelemetry:opentelemetry-sdk:1.49.0")
    testImplementation("io.opentelemetry:opentelemetry-sdk-metrics:1.49.0")
    testImplementation("io.opentelemetry:opentelemetry-exporter-otlp:1.48.0")
    testImplementation("org.jsoup:jsoup:1.19.1")
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

tasks.named("sourcesJar") {
    dependsOn("preprocessVersion")
}

tasks.named("jacocoTestCoverageVerification") {
    dependsOn("preprocessVersion")
    dependsOn("compileJava")
    dependsOn("processResources")
}

checkstyle {
    // Checkstyle versions 7.x, 8.x, and 9.x are supported by JRE version 8 and above.
    toolVersion = "9.3"
    // Fail the build if there is at least one Checkstyle warning.
    maxWarnings = 0
    configDirectory.set(File(rootDir, "config/checkstyle"))
    configFile = configDirectory.get().file("google_checks.xml").asFile

    // Checkstyle will throw an error if a driver-specific import is detected in the new changes.
    // If the change is intentional, add the file to the suppression filter in checkstyle-suppressions.xml.
    configProperties = mapOf("suppressionFile" to configDirectory.get().file("checkstyle-suppressions.xml").asFile)
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
        outputLocation.set(file("${layout.buildDirectory.get()}/reports/spotbugsMain.html"))
        setStylesheet("fancy-hist.xsl")
    }
}
tasks.spotbugsTest {
    reports.create("html") {
        required.set(true)
        outputLocation.set(file("${layout.buildDirectory.get()}/reports/spotbugsTest.html"))
        setStylesheet("fancy-hist.xsl")
    }
}

tasks.withType<JacocoCoverageVerification> {
    violationRules {
        rule {
            limit {
                minimum = BigDecimal(0.50)
            }
        }
    }

    afterEvaluate {
        classDirectories.setFrom(files(classDirectories.files.map {
            fileTree(it).apply {
                exclude(
                    "software/amazon/jdbc/wrapper/*",
                    "software/amazon/jdbc/util/*",
                    "software/amazon/jdbc/profile/*",
                    "software/amazon/jdbc/plugin/DataCacheConnectionPlugin*"
                )
            }
        }))
    }
}

tasks.withType<JacocoReport> {
    afterEvaluate {
        classDirectories.setFrom(files(classDirectories.files.map {
            fileTree(it).apply {
                exclude(
                    "software/amazon/jdbc/wrapper/*",
                    "software/amazon/jdbc/util/*",
                    "software/amazon/jdbc/profile/*",
                    "software/amazon/jdbc/plugin/DataCacheConnectionPlugin*"
                )
            }
        }))
    }
}

tasks.jar {
    from("${project.rootDir}") {
        include("README")
        include("LICENSE")
        include("THIRD-PARTY-LICENSES")
        into("META-INF/")
    }

    from("${layout.buildDirectory.get()}/META-INF/services/") {
        into("META-INF/services/")
    }

    bundle {
        bnd(
            """
            -exportcontents: software.*
            -removeheaders: Created-By
            Bundle-Description: Amazon Web Services (AWS) Advanced JDBC Wrapper Driver
            Bundle-DocURL: https://github.com/awslabs/aws-advanced-jdbc-wrapper
            Bundle-Vendor: Amazon Web Services (AWS)
            Import-Package: javax.sql, javax.transaction.xa, javax.naming, javax.security.sasl;resolution:=optional, *;resolution:=optional
            Bundle-Activator: software.amazon.jdbc.osgi.WrapperBundleActivator
            Bundle-SymbolicName: software.aws.rds
            Bundle-Name: Amazon Web Services (AWS) Advanced JDBC Wrapper Driver
            Bundle-Copyright: Copyright Amazon.com Inc. or affiliates.
            Require-Capability: osgi.ee;filter:="(&(|(osgi.ee=J2SE)(osgi.ee=JavaSE))(version>=1.8))"
            """
        )
    }

    doFirst {
        mkdir("${layout.buildDirectory.get()}/META-INF/services/")
        val driverFile = File("${layout.buildDirectory.get()}/META-INF/services/java.sql.Driver")
        if (driverFile.createNewFile()) {
            driverFile.writeText("software.amazon.jdbc.Driver")
        }
    }
}

junitHtmlReport {
    // The maximum depth to traverse from the results dir.
    // Any eligible reports will be included
    maxDepth = 9

    //RAG status css overrides
    cssRed = "red"
    cssAmber = "orange"
    cssGreen = "green"

    //Processing directories
    testResultsDir = "test-results"
    testReportsDir = "report"

    //Fail build when no XML files to process
    isFailOnEmpty = false
}

tasks.withType<Test> {
    dependsOn("jar")
    testLogging {
        this.showStandardStreams = true
    }
    useJUnitPlatform()
    outputs.upToDateWhen { false }

    System.getProperties().forEach {
        if (it.key.toString().startsWith("test-no-")
            || it.key.toString() == "test-include-tags"
            || it.key.toString() == "test-exclude-tags"
        ) {
            systemProperty(it.key.toString(), it.value.toString())
        }
    }

    // Disable the test report for the individual test task
    reports.junitXml.required.set(true)
    reports.html.required.set(false)

    systemProperty("java.util.logging.config.file", "${project.layout.buildDirectory.get()}/resources/test/logging-test.properties")

    if (!name.contains("performance")) {
        finalizedBy("junitHtmlReport")
    }

    val testReportsPath = "${layout.buildDirectory.get()}/test-results"
    val testReportsDir: File = file(testReportsPath)
    doFirst {
        testReportsDir.deleteRecursively()
    }
}

tasks.register("maskJunitHtmlReport") {
    doLast {
        if (project.file("${layout.buildDirectory.get()}/report/data.js").exists()) {
            val jsFile = project.file("${layout.buildDirectory.get()}/report/data.js")
            var text = jsFile.readText()
            var regex = "\"([^\"]*(AWS_ACCESS_|AWS_SECRET_|AWS_SESSION_)[^\"]*)\", value: \"([^\"]*)\"".toRegex(setOf(RegexOption.MULTILINE, RegexOption.IGNORE_CASE))
            val maskedText = regex.replace(text, "\"$1\", value: \"*****\"")
            jsFile.writeText(maskedText)
        }
    }
}

tasks.register<Test>("test-all-environments") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-performance", "true")
    }
}

tasks.register<Test>("test-all-docker") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-aurora", "true")
        systemProperty("test-no-multi-az", "true")
        systemProperty("test-no-performance", "true")
    }
}

tasks.register<Test>("test-hibernate-only") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-aurora", "true")
        systemProperty("test-no-multi-az", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-graalvm", "true")
        systemProperty("test-hibernate-only", "true")
    }
}

tasks.register<Test>("test-all-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-graalvm", "true")
        systemProperty("test-no-openjdk8", "true")
        systemProperty("test-no-multi-az", "true")
    }
}

tasks.register<Test>("test-all-multi-az") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-graalvm", "true")
        systemProperty("test-no-openjdk8", "true")
        systemProperty("test-no-aurora", "true")
    }
}

tasks.register<Test>("test-all-pg-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-multi-az", "true")
        systemProperty("test-no-graalvm", "true")
        systemProperty("test-no-openjdk8", "true")
    }
}

tasks.register<Test>("test-all-pg-multi-az") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-graalvm", "true")
        systemProperty("test-no-openjdk8", "true")
        systemProperty("test-no-aurora", "true")
    }
}

tasks.register<Test>("test-all-mysql-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-pg-driver", "true")
        systemProperty("test-no-pg-engine", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-graalvm", "true")
        systemProperty("test-no-openjdk8", "true")
        systemProperty("test-no-multi-az", "true")
    }
}

tasks.register<Test>("test-all-mysql-multi-az") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-pg-driver", "true")
        systemProperty("test-no-pg-engine", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-graalvm", "true")
        systemProperty("test-no-openjdk8", "true")
        systemProperty("test-no-aurora", "true")
    }
}

// Debug

tasks.register<Test>("debug-all-environments") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("test-no-performance", "true")
    }
}

tasks.register<Test>("debug-all-docker") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("test-no-aurora", "true")
        systemProperty("test-no-multi-az", "true")
        systemProperty("test-no-performance", "true")
    }
}

tasks.register<Test>("debug-all-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
    }
}

tasks.register<Test>("debug-hibernate-only") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("test-no-aurora", "true")
        systemProperty("test-no-multi-az", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-hibernate-only", "true")
    }
}

// Performance

tasks.register<Test>("test-all-aurora-performance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-multi-az", "true")
        systemProperty("test-no-iam", "true")
        systemProperty("test-no-hikari", "true")
        systemProperty("test-no-secrets-manager", "true")
        systemProperty("test-no-graalvm", "true")
        systemProperty("test-no-openjdk8", "true")
        systemProperty("test-no-instances-1", "true")
        systemProperty("test-no-instances-2", "true")
        systemProperty("test-exclude-tags", "advanced,rw-splitting")
    }
}

tasks.register<Test>("test-aurora-pg-performance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-multi-az", "true")
        systemProperty("test-no-iam", "true")
        systemProperty("test-no-hikari", "true")
        systemProperty("test-no-secrets-manager", "true")
        systemProperty("test-no-graalvm", "true")
        systemProperty("test-no-openjdk8", "true")
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-instances-1", "true")
        systemProperty("test-no-instances-2", "true")
        systemProperty("test-exclude-tags", "advanced,rw-splitting")
    }
}

tasks.register<Test>("debug-aurora-pg-performance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-multi-az", "true")
        systemProperty("test-no-iam", "true")
        systemProperty("test-no-hikari", "true")
        systemProperty("test-no-secrets-manager", "true")
        systemProperty("test-no-graalvm", "true")
        systemProperty("test-no-openjdk8", "true")
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-instances-1", "true")
        systemProperty("test-no-instances-2", "true")
        systemProperty("test-exclude-tags", "advanced,rw-splitting")
    }
}

tasks.register<Test>("test-aurora-mysql-performance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-multi-az", "true")
        systemProperty("test-no-iam", "true")
        systemProperty("test-no-hikari", "true")
        systemProperty("test-no-secrets-manager", "true")
        systemProperty("test-no-graalvm", "true")
        systemProperty("test-no-openjdk8", "true")
        systemProperty("test-no-pg-driver", "true")
        systemProperty("test-no-pg-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-instances-1", "true")
        systemProperty("test-no-instances-2", "true")
        systemProperty("test-exclude-tags", "advanced,rw-splitting")
    }
}

tasks.register<Test>("debug-aurora-mysql-performance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-multi-az", "true")
        systemProperty("test-no-iam", "true")
        systemProperty("test-no-hikari", "true")
        systemProperty("test-no-secrets-manager", "true")
        systemProperty("test-no-graalvm", "true")
        systemProperty("test-no-openjdk8", "true")
        systemProperty("test-no-pg-driver", "true")
        systemProperty("test-no-pg-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-instances-1", "true")
        systemProperty("test-no-instances-2", "true")
        systemProperty("test-exclude-tags", "advanced,rw-splitting")
    }
}

tasks.register<Test>("test-aurora-pg-advanced-performance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-multi-az", "true")
        systemProperty("test-no-iam", "true")
        systemProperty("test-no-hikari", "true")
        systemProperty("test-no-secrets-manager", "true")
        systemProperty("test-no-graalvm", "true")
        systemProperty("test-no-openjdk8", "true")
        systemProperty("test-no-mysql-driver", "true")
        systemProperty("test-no-mysql-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-instances-1", "true")
        systemProperty("test-no-instances-2", "true")
        systemProperty("test-include-tags", "advanced")
    }
}

tasks.register<Test>("test-aurora-mysql-advanced-performance") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-multi-az", "true")
        systemProperty("test-no-iam", "true")
        systemProperty("test-no-hikari", "true")
        systemProperty("test-no-secrets-manager", "true")
        systemProperty("test-no-graalvm", "true")
        systemProperty("test-no-openjdk8", "true")
        systemProperty("test-no-pg-driver", "true")
        systemProperty("test-no-pg-engine", "true")
        systemProperty("test-no-mariadb-driver", "true")
        systemProperty("test-no-mariadb-engine", "true")
        systemProperty("test-no-instances-1", "true")
        systemProperty("test-no-instances-2", "true")
        systemProperty("test-include-tags", "advanced")
    }
}

// Autoscaling

tasks.register<Test>("test-autoscaling-only") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("test-autoscaling-only", "true")
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-graalvm", "true")
    }
}

tasks.register<Test>("debug-autoscaling-only") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.debugTests")
    doFirst {
        systemProperty("test-autoscaling-only", "true")
        systemProperty("test-no-docker", "true")
        systemProperty("test-no-performance", "true")
        systemProperty("test-no-graalvm", "true")
    }
}
