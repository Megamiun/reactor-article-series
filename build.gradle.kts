plugins {
    kotlin("jvm") version "1.8.10"
}

group = "br.com.gabryel"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(platform("io.projectreactor:reactor-bom:2022.0.8"))

    implementation(kotlin("stdlib"))
    implementation("io.projectreactor:reactor-core")
}

tasks {
    val sample0Basics by creating(JavaExec::class) {
        group = "samples"
        description = "Run Article 0 - Basics"
        classpath = sourceSets["main"].runtimeClasspath
        mainClass.set("br.com.gabryel.reactorarticle._0_basics.SamplesKt")

        dependsOn(assemble)
    }
}