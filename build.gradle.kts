import org.jdbcProxyDriver.buildtools.JavaCommentPreprocessorTask
import com.github.vlsi.gradle.properties.dsl.props
import com.github.vlsi.gradle.dsl.configureEach

plugins {
    java
    checkstyle
    jacoco
    id("com.diffplug.spotless") version "6.7.2"
    id("com.github.spotbugs") version "5.0.8"
    id("com.github.vlsi.gradle-extensions") version "1.82"
    id("com.github.vlsi.ide") version "1.82"
    id("com.github.vlsi.stage-vote-release") version "1.82"
}

val String.v: String get() = rootProject.extra["$this.version"] as String
val buildVersion = "jdbc-proxy-driver".v + releaseParams.snapshotSuffix

tasks.check {
    dependsOn("jacocoTestCoverageVerification")
}

repositories {
    mavenCentral()
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

allprojects {
    version = buildVersion

    tasks {
        configureEach<JavaCommentPreprocessorTask> {
            val re = Regex("^(\\d+)\\.(\\d+)(?:\\.(\\d+))?.*")

            val version = project.version.toString()
            val matchResult = re.find(version) ?: throw GradleException("Unable to parse major.minor.patch version parts from project.version '$version'")
            val (major, minor, patch) = matchResult.destructured

            variables.apply {
                put("version", version)
                put("version.major", major)
                put("version.minor", minor)
                put("version.patch", patch.ifBlank { "0" })
            }
        }
    }
}

dependencies {

    implementation("org.checkerframework:checker-qual:3.22.2")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.8.2")
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