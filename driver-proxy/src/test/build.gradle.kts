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
    testImplementation("org.postgresql:postgresql:42.+")
    testImplementation("mysql:mysql-connector-java:8.0.+")
    testImplementation("com.zaxxer:HikariCP:4.+") // version 4.+ is compatible with Java 8
    testImplementation("org.springframework.boot:spring-boot-starter-jdbc:2.7.+")
    testImplementation("org.mockito:mockito-inline:4.+")
    testImplementation("software.amazon.awssdk:rds:2.17.165")
    testImplementation("software.amazon.awssdk:ec2:2.17.165")
    testImplementation("org.testcontainers:testcontainers:1.17.+")
    testImplementation("org.testcontainers:mysql:1.17.+")
    testImplementation("org.testcontainers:postgresql:1.17.+")
    testImplementation("org.testcontainers:junit-jupiter:1.17.+")
    testImplementation("org.testcontainers:toxiproxy:1.17.+")

    implementation(fileTree("./libs") { include("*.jar") })
    testImplementation(files("./test"))
}

// Integration tests are run in a specific order.
// To add more tests, see integration.container.aurora.postgres.AuroraPostgresTestSuite.java
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
    outputs.upToDateWhen { false }
    useJUnitPlatform()
    testClassesDirs += files("./test")
}
