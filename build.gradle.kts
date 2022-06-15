plugins {
    java
    checkstyle
    id("com.diffplug.spotless") version "6.7.2"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

checkstyle {
    // Checkstyle versions 7.x, 8.x, and 9.x are supported by JRE version 8 and above
    toolVersion = "9.3"
    configDirectory.set(File(rootDir, "config/checkstyle"))
    configFile = configDirectory.get().file("google_checks.xml").asFile
}

spotless {
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

dependencies {

    implementation("org.checkerframework:checker-qual:3.22.1")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    testImplementation("org.postgresql:postgresql:42.+")
    testImplementation("mysql:mysql-connector-java:8.0.+")
    testImplementation("com.zaxxer:HikariCP:4.+") // version 4.+ is compatible with Java 8
    testImplementation("org.springframework.boot:spring-boot-starter-jdbc:2.7.+")
    testImplementation("org.mockito:mockito-inline:4.+")
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