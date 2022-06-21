import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    implementation("com.igormaznitsa:jcp:7.0.2")
}

tasks.withType<KotlinCompile> {
    sourceCompatibility = "unused"
    targetCompatibility = "unused"
    kotlinOptions {
        jvmTarget = "1.8"
    }
}
