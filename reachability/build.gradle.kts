plugins {
    id("java")
    id("org.graalvm.buildtools.native") version "0.10.3"
    application
}

group = "reachability.software.amazon.jdbc"
version = "2.5.3"

repositories {
    mavenCentral()
    gradlePluginPortal()
}

application {
    mainClass.set("reachability.software.amazon.jdbc.Main")
}

dependencies {
    implementation(project(":aws-advanced-jdbc-wrapper"))
    // TODO: does graalvm only have reachability data for 42.7.3, or for all versions past 42.3.4?
    // https://github.com/oracle/graalvm-reachability-metadata/tree/master/metadata/org.postgresql/postgresql/42.7.3
    // https://www.graalvm.org/native-image/libraries-and-frameworks/#footnote-1
    implementation("org.postgresql:postgresql:42.7.3")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.1")
    implementation("software.amazon.awssdk:secretsmanager:2.28.11")
    implementation("com.zaxxer:HikariCP:4.0.3")
    implementation("software.amazon.awssdk:sts:2.27.22")
    implementation("org.jsoup:jsoup:1.18.1")

    testImplementation(files("../wrapper/build/classes/java/test")) // Important!!!
    testImplementation(files("../wrapper/build/resources/test")) // Important!!!

    testImplementation("org.graalvm.buildtools:junit-platform-native:0.10.3")
    testImplementation("org.junit.platform:junit-platform-commons:1.11.2")
    testImplementation("org.junit.platform:junit-platform-engine:1.11.0")
    testImplementation("org.junit.platform:junit-platform-launcher:1.11.3")
    testImplementation("org.junit.platform:junit-platform-suite-engine:1.11.3")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.11.3")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.10.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testImplementation("org.postgresql:postgresql:42.7.3")
    testImplementation("mysql:mysql-connector-java:8.0.33")
    testImplementation("org.mariadb.jdbc:mariadb-java-client:3.1.0")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.17.1")
    testImplementation("software.amazon.awssdk:secretsmanager:2.28.11")
    testImplementation("com.zaxxer:HikariCP:4.0.3")
    testImplementation("software.amazon.awssdk:sts:2.27.22")
    testImplementation("org.jsoup:jsoup:1.18.1")
}

tasks.test {
    useJUnitPlatform()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(11)
    }
}

graalvmNative {
    metadataRepository {
        enabled.set(true)
    }
    binaries.all {
        resources.autodetect()
    }
}
