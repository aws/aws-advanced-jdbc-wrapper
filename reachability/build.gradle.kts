plugins {
    id("java")
    id("org.graalvm.buildtools.native") version "0.10.3"
    application
}

group = "reachability.software.amazon.jdbc"
version = "2.5.3"

repositories {
    mavenCentral()
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
}

tasks.test {
    useJUnitPlatform()
}

graalvmNative {
    metadataRepository {
        enabled.set(true)
    }
}
