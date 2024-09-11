import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    `kotlin-dsl`
    `java-gradle-plugin`
    `maven-publish`
    alias(libs.plugins.plugin.publish)
    alias(libs.plugins.ktlint)
}

repositories {
    mavenCentral()
    maven(url = "https://jitpack.io")
    google()
}

group = "org.jetbrains.kotlinx.dataframe"

dependencies {
    api(libs.kotlin.reflect)
    implementation(project(":core"))
    implementation(project(":dataframe-arrow"))
    implementation(project(":dataframe-openapi"))
    implementation(project(":dataframe-excel"))
    implementation(project(":dataframe-jdbc"))

    implementation(libs.kotlin.gradle.plugin.api)
    implementation(libs.kotlin.gradle.plugin)
    implementation(libs.serialization.core)
    implementation(libs.serialization.json)
    implementation(libs.ksp.gradle)
    implementation(libs.ksp.api)

    testImplementation(libs.junit)
    testImplementation(libs.kotestAssertions)
    testImplementation(libs.android.gradle.api)
    testImplementation(libs.android.gradle)
    testImplementation(libs.ktor.server.netty)
    testImplementation(libs.h2db)
    testImplementation(gradleApi())
}

tasks.withType<ProcessResources> {
    filesMatching("**/plugin.properties") {
        filter {
            it.replace("%PREPROCESSOR_VERSION%", "$version")
        }
    }
}

tasks.withType<ProcessResources> {
    filesMatching("**/df.properties") {
        filter {
            it.replace(
                "%DATAFRAME_JAR%",
                project(":core").configurations
                    .getByName("instrumentedJars")
                    .artifacts.single()
                    .file.absolutePath
                    .replace(File.separatorChar, '/'),
            )
        }
    }
}

gradlePlugin {
    // These settings are set for the whole plugin bundle
    website = "https://github.com/Kotlin/dataframe"
    vcsUrl = "https://github.com/Kotlin/dataframe"

    plugins {
        create("schemaGeneratorPlugin") {
            id = "org.jetbrains.kotlinx.dataframe"
            implementationClass = "org.jetbrains.dataframe.gradle.ConvenienceSchemaGeneratorPlugin"
            displayName = "Kotlin Dataframe gradle plugin"
            description = "Gradle plugin providing task for inferring data schemas from your CSV or JSON data"
            tags = listOf("dataframe", "kotlin")
        }
        create("deprecatedSchemaGeneratorPlugin") {
            id = "org.jetbrains.kotlin.plugin.dataframe"
            implementationClass = "org.jetbrains.dataframe.gradle.DeprecatingSchemaGeneratorPlugin"
            displayName = "Kotlin Dataframe gradle plugin"
            description =
                "The plugin was moved to 'org.jetbrains.kotlinx.dataframe'. Gradle plugin providing task for inferring data schemas from your CSV or JSON data"
            tags = listOf("dataframe", "kotlin")
        }
    }
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    compilerOptions.jvmTarget = JvmTarget.JVM_1_8
}

tasks.withType<JavaCompile>().all {
    sourceCompatibility = JavaVersion.VERSION_1_8.toString()
    targetCompatibility = JavaVersion.VERSION_1_8.toString()
}

sourceSets {
    val main by getting
    val test by getting
    val testRuntimeClasspath by configurations
    create("integrationTest") {
        kotlin.srcDir("src/integrationTest/kotlin")
        compileClasspath += main.output + test.output + testRuntimeClasspath
        runtimeClasspath += output + compileClasspath + test.runtimeClasspath
    }
}

val integrationTestConfiguration by configurations.creating {
    extendsFrom(configurations.testImplementation.get())
}

val integrationTestTask = task<Test>("integrationTest") {
    dependsOn(":plugins:symbol-processor:publishToMavenLocal")
    dependsOn(":dataframe-arrow:publishToMavenLocal")
    dependsOn(":dataframe-excel:publishToMavenLocal")
    dependsOn(":dataframe-jdbc:publishToMavenLocal")
    dependsOn(":dataframe-openapi:publishToMavenLocal")
    dependsOn(":publishApiPublicationToMavenLocal")
    dependsOn(":dataframe-arrow:publishDataframeArrowPublicationToMavenLocal")
    dependsOn(":dataframe-excel:publishDataframeExcelPublicationToMavenLocal")
    dependsOn(":dataframe-jdbc:publishDataframeJDBCPublicationToMavenLocal")
    dependsOn(":dataframe-openapi:publishDataframeOpenApiPublicationToMavenLocal")
    dependsOn(":plugins:symbol-processor:publishMavenPublicationToMavenLocal")
    dependsOn(":core:publishCorePublicationToMavenLocal")
    description = "Runs integration tests."
    group = "verification"
    maxParallelForks = (Runtime.getRuntime().availableProcessors() / 2).coerceAtLeast(1)

    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    shouldRunAfter("test")
}

tasks.check { dependsOn(integrationTestTask) }
