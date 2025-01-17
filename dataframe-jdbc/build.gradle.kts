plugins {
    with(libs.plugins) {
        alias(kotlin.jvm)
        alias(publisher)
        alias(kover)
//        alias(ktlint)
        alias(jupyter.api)
        alias(binary.compatibility.validator)
    }
}

group = "org.jetbrains.kotlinx"

val jupyterApiTCRepo: String by project

repositories {
    mavenCentral()
    maven(jupyterApiTCRepo)
}

dependencies {
    api(project(":core"))
    implementation(libs.mariadb)
    implementation(libs.kotlinLogging)
    testImplementation(libs.sqlite)
    testImplementation(libs.postgresql)
    testImplementation(libs.mysql)
    testImplementation(libs.h2db)
    testImplementation(libs.mssql)
    testImplementation(libs.junit)
    testImplementation(libs.sl4jsimple)
    testImplementation(libs.jts)
    testImplementation(libs.kotestAssertions) {
        exclude("org.jetbrains.kotlin", "kotlin-stdlib-jdk8")
    }
}

kotlinPublications {
    publication {
        publicationName = "dataframeJDBC"
        artifactId = project.name
        description = "JDBC support for Kotlin Dataframe"
        packageName = artifactId
    }
}

kotlin {
    explicitApi()
}
