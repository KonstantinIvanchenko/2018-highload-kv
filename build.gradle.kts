// See https://gradle.org and https://github.com/gradle/kotlin-dsl

import org.gradle.jvm.tasks.Jar

// Apply the java plugin to add support for Java
plugins {
    java
    application
}

repositories {
    jcenter()
}

dependencies {
    // Our beloved one-nio
    compile("ru.odnoklassniki:one-nio:1.0.2")

    // Annotations for better code documentation
    compile("com.intellij:annotations:12.0")

    // JUnit 5
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.3.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.3.1")

    // Guava for tests
    testCompile("com.google.guava:guava:23.1-jre")
}

val fatJar = task("fatJar", type = Jar::class) {
    baseName = "${project.name}-fat"
    manifest {
        attributes["Implementation-Title"] = "highload-kv-jar"
        attributes["Implementation-Version"] = 0.1//version
        attributes["Main-Class"] = "ru.mail.polis.cluster"
    }
    //from(configurations.runtime.map({ if (it.isDirectory) it else zipTree(it) }))
    from(configurations["compile"].map{ if (it.isDirectory) it else zipTree(it) })
    with(tasks["jar"] as CopySpec)
}

tasks {
    "test"(Test::class) {
        maxHeapSize = "128m"//"128m"//"1g"
        useJUnitPlatform()
    }
    "build"{
        dependsOn(fatJar)
    }
}

application {
    // Define the main class for the application
    mainClassName = "ru.mail.polis.Cluster"

    // And limit Xmx
    applicationDefaultJvmArgs = listOf("-Xmx128m")//listOf("-Xmx128m")
}

