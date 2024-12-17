plugins {
    id("java")
    id("org.graalvm.buildtools.native") version "0.10.4"
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

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(11)
    }
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
    implementation("mysql:mysql-connector-java:8.0.33")
    implementation("org.mariadb.jdbc:mariadb-java-client:3.1.0")
    implementation(files("../wrapper/build/classes/java/test")) // Important!!!

//    testImplementation(files("../wrapper/build/classes/java/test")) // Important!!!
//
//    testImplementation("org.graalvm.buildtools:junit-platform-native:0.10.3")
    testImplementation("org.junit.platform:junit-platform-commons:1.11.2")
    testImplementation("org.junit.platform:junit-platform-engine:1.11.0")
    testImplementation("org.junit.platform:junit-platform-launcher:1.11.3")
    testImplementation("org.junit.platform:junit-platform-suite-engine:1.11.3")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.11.3")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.10.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
//    testImplementation("org.postgresql:postgresql:42.7.3")
//    testImplementation("mysql:mysql-connector-java:8.0.33")
//    testImplementation("org.mariadb.jdbc:mariadb-java-client:3.1.0")
//    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.17.1")
//    testImplementation("software.amazon.awssdk:secretsmanager:2.28.11")
//    testImplementation("com.zaxxer:HikariCP:4.0.3")
//    testImplementation("software.amazon.awssdk:sts:2.27.22")
//    testImplementation("org.jsoup:jsoup:1.18.1")
//    testImplementation("io.opentelemetry:opentelemetry-api:1.42.1")
//    testImplementation("io.opentelemetry:opentelemetry-sdk:1.42.1")
//    testImplementation("io.opentelemetry:opentelemetry-sdk-metrics:1.43.0")
}

//tasks.test {
//    useJUnitPlatform()
//    filter.excludeTestsMatching("integration.*")
//}

tasks.compileJava {
    dependsOn(":aws-advanced-jdbc-wrapper:compileTestJava")
}

tasks.compileTestJava {
    dependsOn(":aws-advanced-jdbc-wrapper:compileTestJava")
}

//sourceSets {
//    create("inContainer") {
////        compileClasspath += sourceSets.main.get().output
////        runtimeClasspath += sourceSets.main.get().output
//    }
//}

//configurations {
//    create("inContainerImplementation") {
//        extendsFrom(configurations.testImplementation.get())
//    }
//}
//configurations.getByName("inContainerImplementation").extendsFrom(configurations.testImplementation.get())
//val inContainerImplementation: Configuration by configurations.getting {
//    extendsFrom(configurations.testImplementation.get())
//}


//dependencies {
//    testImplementation(platform("org.junit:junit-bom:5.11.3"))
//    testImplementation("org.junit.jupiter:junit-jupiter")
//    inContainerImplementation(project(":"))
//}
//
//var inContainerTask = tasks.register<Test>("inContainer") {
//    testClassesDirs = sourceSets.getByName("inContainer").output.classesDirs
//    classpath = sourceSets.getByName("inContainer").runtimeClasspath
//}
//
//tasks.withType<Test>().configureEach {
//    useJUnitPlatform()
//}



//val inContainerImplementation by configurations.getting {
//    extendsFrom(configurations.implementation.get())
//}
//val inContainerRuntimeOnly by configurations.getting
//
//configurations["inContainerRuntimeOnly"].extendsFrom(configurations.runtimeOnly.get())

//dependencies {
//    inContainerImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
//    inContainerRuntimeOnly("org.junit.platform:junit-platform-launcher")
//}

//tasks.register<Test>("inContainer") {
//    description = "Integration tests to be run inside of docker test container."
//    group = "verification"
//
//    testClassesDirs = sourceSets.getByName("inContainer").output.classesDirs
//    classpath = sourceSets.getByName("inContainer").runtimeClasspath
//
//    useJUnitPlatform()
//    filter.excludeTestsMatching("reachability.software.*") // exclude unit tests
//    filter.includeTestsMatching("integration.container.tests.*")
//}

graalvmNative {
    toolchainDetection.set(true)
    metadataRepository {
        enabled.set(true)
    }
//    registerTestBinary("inContainer") {
//        usingSourceSet(sourceSets.getByName("inContainer"))
//        forTestTask(inContainerTask)
//    }
//    binaries {
//        named("test") {
//            buildArgs.add("-O0")
//        }
//    }
    binaries.all {
        resources.autodetect()
    }
}
