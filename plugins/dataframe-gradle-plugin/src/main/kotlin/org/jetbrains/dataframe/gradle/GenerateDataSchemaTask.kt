package org.jetbrains.dataframe.gradle

import org.gradle.api.DefaultTask
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.dataframe.impl.codeGen.CodeGenerator
import java.io.File
import java.io.IOException
import java.net.URL
import java.nio.file.Paths
import com.beust.klaxon.KlaxonException
import org.gradle.api.provider.Provider
import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.dataframe.impl.codeGen.CodeGenResult
import org.jetbrains.kotlinx.dataframe.api.schema
import org.jetbrains.kotlinx.dataframe.codeGen.MarkerVisibility
import org.jetbrains.kotlinx.dataframe.io.SupportedFormats
import org.jetbrains.kotlinx.dataframe.io.readCSV
import org.jetbrains.kotlinx.dataframe.io.readJson
import java.io.FileNotFoundException

abstract class GenerateDataSchemaTask : DefaultTask() {

    @get:Input
    abstract val data: Property<Any>

    @get:Input
    abstract val csvOptions: Property<CsvOptions>

    @get:Input
    abstract val src: Property<File>

    @get:Input
    abstract val interfaceName: Property<String>

    @get:Input
    abstract val packageName: Property<String>

    @get:Input
    abstract val schemaVisibility: Property<DataSchemaVisibility>

    @Suppress("LeakingThis")
    @get:OutputFile
    val dataSchema: Provider<File> = packageName.zip(interfaceName) { packageName, interfaceName ->
        val packagePath = packageName.replace('.', File.separatorChar)
        Paths.get(src.get().absolutePath, packagePath, "$interfaceName.Generated.kt").toFile()
    }

    @TaskAction
    fun generate() {
        val df = readDataFrame(data.get(), csvOptions.get())
        val codeGenerator = CodeGenerator.create(useFqNames = false)
        val codeGenResult = codeGenerator.generate(
            schema = df.schema(),
            name = interfaceName.get(),
            fields = true,
            extensionProperties = false,
            isOpen = true,
            visibility = when (schemaVisibility.get()) {
                DataSchemaVisibility.INTERNAL -> MarkerVisibility.INTERNAL
                DataSchemaVisibility.IMPLICIT_PUBLIC -> MarkerVisibility.IMPLICIT_PUBLIC
                DataSchemaVisibility.EXPLICIT_PUBLIC -> MarkerVisibility.EXPLICIT_PUBLIC
            }
        )
        val escapedPackageName = escapePackageName(packageName.get())

        val dataSchema = dataSchema.get()
        dataSchema.writeText(buildSourceFileContent(escapedPackageName, codeGenResult))
    }

    private fun escapePackageName(packageName: String): String {
        // See RegexExpectationsTest
        return if (packageName.isNotEmpty()) {
            packageName.split(NameChecker.PACKAGE_IDENTIFIER_DELIMITER)
                .joinToString(".") { part -> "`$part`" }
        } else {
            packageName
        }
    }

    private fun readDataFrame(data: Any, csvOptions: CsvOptions): AnyFrame {
        fun guessFormat(url: String): SupportedFormats? = when {
            url.endsWith(".csv") -> SupportedFormats.CSV
            url.endsWith(".json") -> SupportedFormats.JSON
            else -> null
        }
        fun readCSV(url: URL) = DataFrame.readCSV(url, delimiter = csvOptions.delimiter)
        val url = urlOf(data)
        return try {
            when (guessFormat(url.path)) {
                SupportedFormats.CSV -> readCSV(url)
                SupportedFormats.JSON -> DataFrame.readJson(url)
                else -> try {
                    readCSV(url)
                } catch (e: Exception) {
                    DataFrame.readJson(url)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is KlaxonException, is IndexOutOfBoundsException, is IOException -> throw InvalidDataException(e)
                is FileNotFoundException, is IllegalArgumentException -> throw MissingDataException(e)
                else -> throw e
            }
        }
    }

    private fun urlOf(data: Any): URL {
        fun isURL(fileOrUrl: String): Boolean = listOf("http:", "https:", "ftp:").any { fileOrUrl.startsWith(it) }

        return when (data) {
            is File -> data.toURI()
            is URL -> data.toURI()
            is String -> when {
                isURL(data) -> URL(data).toURI()
                else -> project.file(data).toURI()
            }
            else -> throw IllegalArgumentException("data for schema \"${interfaceName.get()}\" must be File, URL or String")
        }.toURL()
    }

    private fun buildSourceFileContent(escapedPackageName: String, codeGenResult: CodeGenResult): String {
        return buildString {
            appendLine(
                """
                @file:Suppress(
                    "RemoveRedundantBackticks", 
                    "RemoveRedundantQualifierName", 
                    "unused", "ObjectPropertyName", 
                    "UNCHECKED_CAST", "PropertyName",
                    "ClassName", "UnusedImport"
                )
                """.trimIndent()
            )

            if (escapedPackageName.isNotEmpty()) {
                appendLine("package $escapedPackageName")
                appendLine()
            }
            appendLine("import org.jetbrains.kotlinx.dataframe.annotations.*")
            appendLine("import org.jetbrains.kotlinx.dataframe.ColumnsContainer")
            appendLine("import org.jetbrains.kotlinx.dataframe.DataColumn")
            appendLine("import org.jetbrains.kotlinx.dataframe.DataFrame")
            appendLine("import org.jetbrains.kotlinx.dataframe.DataRow")
            appendLine("import org.jetbrains.kotlinx.dataframe.columns.ColumnGroup")
            appendLine()
            appendLine("// GENERATED. DO NOT EDIT MANUALLY")
            appendLine(codeGenResult.code.declarations)
        }
    }
}

class MissingDataException(cause: Exception) : Exception(cause)
class InvalidDataException(cause: Exception) : Exception(cause)
