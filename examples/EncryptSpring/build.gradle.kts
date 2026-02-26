plugins {
    java
    id("org.springframework.boot") version "3.2.0"
    id("io.spring.dependency-management") version "1.1.4"
}

group = "com.example"
version = "1.0.0"

java {
    sourceCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("software.amazon.jdbc:aws-advanced-jdbc-wrapper:3.2.1-SNAPSHOT")
    implementation("software.amazon.awssdk:kms:2.42.4")
    implementation("com.github.jsqlparser:jsqlparser:4.5")
    runtimeOnly("org.postgresql:postgresql")
}
