package org.jetbrains.kotlinx.dataframe.jupyter

import org.jetbrains.dataframe.impl.codeGen.CodeGenerator
import org.jetbrains.dataframe.impl.codeGen.ReplCodeGenerator
import org.jetbrains.kotlinx.dataframe.AnyCol
import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.AnyRow
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.DataRow
import org.jetbrains.kotlinx.dataframe.annotations.DataSchema
import org.jetbrains.kotlinx.dataframe.api.Convert
import org.jetbrains.kotlinx.dataframe.api.FormatClause
import org.jetbrains.kotlinx.dataframe.api.FormattedFrame
import org.jetbrains.kotlinx.dataframe.api.Gather
import org.jetbrains.kotlinx.dataframe.api.GroupBy
import org.jetbrains.kotlinx.dataframe.api.GroupClause
import org.jetbrains.kotlinx.dataframe.api.InsertClause
import org.jetbrains.kotlinx.dataframe.api.Merge
import org.jetbrains.kotlinx.dataframe.api.MoveClause
import org.jetbrains.kotlinx.dataframe.api.Pivot
import org.jetbrains.kotlinx.dataframe.api.PivotGroupBy
import org.jetbrains.kotlinx.dataframe.api.ReducedGroupBy
import org.jetbrains.kotlinx.dataframe.api.ReducedPivot
import org.jetbrains.kotlinx.dataframe.api.ReducedPivotGroupBy
import org.jetbrains.kotlinx.dataframe.api.RenameClause
import org.jetbrains.kotlinx.dataframe.api.ReplaceClause
import org.jetbrains.kotlinx.dataframe.api.Split
import org.jetbrains.kotlinx.dataframe.api.SplitWithTransform
import org.jetbrains.kotlinx.dataframe.api.Update
import org.jetbrains.kotlinx.dataframe.api.asColumnGroup
import org.jetbrains.kotlinx.dataframe.api.asDataFrame
import org.jetbrains.kotlinx.dataframe.api.columnsCount
import org.jetbrains.kotlinx.dataframe.api.isColumnGroup
import org.jetbrains.kotlinx.dataframe.api.name
import org.jetbrains.kotlinx.dataframe.codeGen.CodeConverter
import org.jetbrains.kotlinx.dataframe.codeGen.CodeWithConverter
import org.jetbrains.kotlinx.dataframe.columns.ColumnGroup
import org.jetbrains.kotlinx.dataframe.columns.ColumnReference
import org.jetbrains.kotlinx.dataframe.dataTypes.IFRAME
import org.jetbrains.kotlinx.dataframe.dataTypes.IMG
import org.jetbrains.kotlinx.dataframe.impl.codeGen.CodeGenerationReadResult
import org.jetbrains.kotlinx.dataframe.impl.codeGen.urlCodeGenReader
import org.jetbrains.kotlinx.dataframe.impl.createStarProjectedType
import org.jetbrains.kotlinx.dataframe.impl.renderType
import org.jetbrains.kotlinx.dataframe.io.DataFrameHtmlData
import org.jetbrains.kotlinx.dataframe.io.SupportedCodeGenerationFormat
import org.jetbrains.kotlinx.dataframe.io.supportedFormats
import org.jetbrains.kotlinx.jupyter.api.*
import org.jetbrains.kotlinx.jupyter.api.libraries.*
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.isSubtypeOf

/** Users will get an error if their Kotlin Jupyter kernel is older than this version. */
private const val MIN_KERNEL_VERSION = "0.11.0.198"

internal val newDataSchemas = mutableListOf<KClass<*>>()

internal class Integration(
    private val notebook: Notebook,
    private val options: MutableMap<String, String?>,
) : JupyterIntegration() {

    val version = options["v"]

    private fun KotlinKernelHost.execute(codeWithConverter: CodeWithConverter<*>, argument: String): VariableName? {
        val code = codeWithConverter.with(argument)
        return if (code.isNotBlank()) {
            val result = execute(code)
            if (codeWithConverter.hasConverter) {
                result.name
            } else null
        } else null
    }

    private fun KotlinKernelHost.execute(
        codeWithConverter: CodeWithConverter<*>,
        property: KProperty<*>,
        type: KType,
    ): VariableName? {
        val variableName = "(${property.name}${if (property.returnType.isMarkedNullable) "!!" else ""} as $type)"
        return execute(codeWithConverter, variableName)
    }

    private fun KotlinKernelHost.updateImportDataSchemaVariable(
        importDataSchema: ImportDataSchema,
        property: KProperty<*>,
    ): VariableName? {
        val formats = supportedFormats.filterIsInstance<SupportedCodeGenerationFormat>()
        val name = property.name + "DataSchema"
        return when (
            val codeGenResult = CodeGenerator.urlCodeGenReader(importDataSchema.url, name, formats, true)
        ) {
            is CodeGenerationReadResult.Success -> {
                val readDfMethod = codeGenResult.getReadDfMethod(importDataSchema.url.toExternalForm())
                val code = readDfMethod.additionalImports.joinToString("\n") +
                    "\n" +
                    codeGenResult.code

                execute(code)
                execute("""DISPLAY("Data schema successfully imported as ${property.name}: $name")""")

                name
            }

            is CodeGenerationReadResult.Error -> {
                execute("""DISPLAY("Failed to read data schema from ${importDataSchema.url}: ${codeGenResult.reason}")""")
                null
            }
        }
    }

    private fun KotlinKernelHost.updateAnyFrameVariable(
        df: AnyFrame,
        property: KProperty<*>,
        codeGen: ReplCodeGenerator,
    ): VariableName? = execute(
        codeWithConverter = codeGen.process(df, property),
        property = property,
        type = DataFrame::class.createStarProjectedType(false),

    )

    private fun KotlinKernelHost.updateAnyRowVariable(
        row: AnyRow,
        property: KProperty<*>,
        codeGen: ReplCodeGenerator,
    ): VariableName? = execute(
        codeWithConverter = codeGen.process(row, property),
        property = property,
        type = DataRow::class.createStarProjectedType(false),
    )

    private fun KotlinKernelHost.updateColumnGroupVariable(
        col: ColumnGroup<*>,
        property: KProperty<*>,
        codeGen: ReplCodeGenerator,
    ): VariableName? = execute(
        codeWithConverter = codeGen.process(col.asDataFrame(), property),
        property = property,
        type = ColumnGroup::class.createStarProjectedType(false),
    )

    private fun KotlinKernelHost.updateAnyColVariable(
        col: AnyCol,
        property: KProperty<*>,
        codeGen: ReplCodeGenerator,
    ): VariableName? = if (col.isColumnGroup()) {
        val codeWithConverter = codeGen.process(col.asColumnGroup().asDataFrame(), property).let { c ->
            CodeWithConverter(c.declarations, converter = CodeConverter { c.converter("$it.asColumnGroup()") })
        }
        execute(
            codeWithConverter = codeWithConverter,
            property = property,
            type = DataColumn::class.createStarProjectedType(false),
        )
    } else {
        null
    }

    override fun Builder.onLoaded() {
        if (version != null) {
            dependencies(
                "org.jetbrains.kotlinx:dataframe-excel:$version",
                "org.jetbrains.kotlinx:dataframe-jdbc:$version",
                "org.jetbrains.kotlinx:dataframe-arrow:$version",
                "org.jetbrains.kotlinx:dataframe-openapi:$version",
            )
        }

        try {
            setMinimalKernelVersion(MIN_KERNEL_VERSION)
        } catch (_: NoSuchMethodError) { // will be thrown when a version < 0.11.0.198
            throw IllegalStateException(
                getKernelUpdateMessage(notebook.kernelVersion, MIN_KERNEL_VERSION, notebook.jupyterClientType)
            )
        }
        val codeGen = ReplCodeGenerator.create()
        val config = JupyterConfiguration()

        if (notebook.jupyterClientType == JupyterClientType.KOTLIN_NOTEBOOK) {
            config.display.isolatedOutputs = true
        }

        onLoaded {
            declare("dataFrameConfig" to config)
        }

        resources {
            if (!config.display.isolatedOutputs) {
                js("DataFrame") {
                    if (config.display.localTesting) {
                        classPath("init.js")
                    } else {
                        // Update this commit when new version of init.js is pushed
                        val initJsSha = "3db46ccccaa1291c0627307d64133317f545e6ae"
                        url("https://cdn.jsdelivr.net/gh/Kotlin/dataframe@$initJsSha/core/src/main/resources/init.js")
                    }
                }

                css("DataFrameTable") { classPath("table.css") }
            }
        }

        with(JupyterHtmlRenderer(config.display, this)) {
            render<DisableRowsLimitWrapper>(
                { "DataRow: index = ${it.value.rowsCount()}, columnsCount = ${it.value.columnsCount()}" },
                applyRowsLimit = false
            )

            render<GroupClause<*, *>>({ "Group" })
            render<MoveClause<*, *>>({ "Move" })
            render<RenameClause<*, *>>({ "Rename" })
            render<ReplaceClause<*, *>>({ "Replace" })
            render<InsertClause<*>>({ "Insert" })
            render<FormatClause<*, *>>({ "Format" })

            render<DataFrameHtmlData> {
                // Our integration declares script and css definition. But in Kotlin Notebook outputs are isolated in IFrames
                // That's why we include them directly in the output
                if (notebook.jupyterClientType == JupyterClientType.KOTLIN_NOTEBOOK) {
                    it.withTableDefinitions().toJupyterHtmlData().toIFrame(notebook.currentColorScheme)
                } else {
                    it.toJupyterHtmlData().toSimpleHtml(notebook.currentColorScheme)
                }
            }

            render<AnyRow>(
                { "DataRow: index = ${it.index()}, columnsCount = ${it.columnsCount()}" },
            )
            render<ColumnGroup<*>>(
                { """ColumnGroup: name = "${it.name}", rowsCount = ${it.rowsCount()}, columnsCount = ${it.columnsCount()}""" },
            )
            render<AnyCol>(
                { """DataColumn: name = "${it.name}", type = ${renderType(it.type())}, size = ${it.size()}""" },
            )
            render<AnyFrame>(
                { "DataFrame: rowsCount = ${it.rowsCount()}, columnsCount = ${it.columnsCount()}" }
            )
            render<FormattedFrame<*>>(
                { "DataFrame: rowsCount = ${it.df.rowsCount()}, columnsCount = ${it.df.columnsCount()}" },
                modifyConfig = { getDisplayConfiguration(it) },
            )
            render<GroupBy<*, *>>({ "GroupBy" })
            render<ReducedGroupBy<*, *>>({ "ReducedGroupBy" })
            render<Pivot<*>>({ "Pivot" })
            render<ReducedPivot<*>>({ "ReducedPivot" })
            render<PivotGroupBy<*>>({ "PivotGroupBy" })
            render<ReducedPivotGroupBy<*>>({ "ReducedPivotGroupBy" })
            render<SplitWithTransform<*, *, *>>({ "Split" })
            render<Split<*, *>>({ "Split" })
            render<Merge<*, *, *>>({ "Merge" })
            render<Gather<*, *, *, *>>({ "Gather" })
            render<IMG> { HTML(it.toString()) }
            render<IFRAME> { HTML(it.toString()) }
            render<Update<*, *>>({ "Update" })
            render<Convert<*, *>>({ "Convert" })
        }

        import("org.jetbrains.kotlinx.dataframe.api.*")
        import("org.jetbrains.kotlinx.dataframe.*")
        import("org.jetbrains.kotlinx.dataframe.annotations.*")
        import("org.jetbrains.kotlinx.dataframe.io.*")
        import("org.jetbrains.kotlinx.dataframe.columns.*")
        import("org.jetbrains.kotlinx.dataframe.jupyter.ImportDataSchema")
        import("org.jetbrains.kotlinx.dataframe.jupyter.importDataSchema")
        import("org.jetbrains.kotlinx.dataframe.jupyter.KotlinNotebookPluginUtils")
        import("java.net.URL")
        import("java.io.File")
        import("kotlinx.datetime.Instant")
        import("kotlinx.datetime.LocalDateTime")
        import("kotlinx.datetime.LocalDate")
        import("org.jetbrains.kotlinx.dataframe.dataTypes.*")
        import("org.jetbrains.kotlinx.dataframe.impl.codeGen.urlCodeGenReader")

        addTypeConverter(object : FieldHandler {
            override val execution = FieldHandlerFactory.createUpdateExecution<Any> { instance, property ->
                when (instance) {
                    is AnyCol -> updateAnyColVariable(instance, property, codeGen)
                    is ColumnGroup<*> -> updateColumnGroupVariable(instance, property, codeGen)
                    is AnyRow -> updateAnyRowVariable(instance, property, codeGen)
                    is AnyFrame -> updateAnyFrameVariable(instance, property, codeGen)
                    is ImportDataSchema -> updateImportDataSchemaVariable(instance, property)
                    is GroupBy<*, *> -> execute(codeGen.process(instance), property, GroupBy::class.createStarProjectedType(false))
                    else -> error("${instance::class} should not be handled by Dataframe field handler")
                }
            }
            override fun accepts(value: Any?, property: KProperty<*>): Boolean {
                return value is AnyCol ||
                    value is ColumnGroup<*> ||
                    value is AnyRow ||
                    value is AnyFrame ||
                    value is ImportDataSchema ||
                    value is GroupBy<*, *>
            }
        })

        fun KotlinKernelHost.addDataSchemas(classes: List<KClass<*>>) {
            val code = classes.joinToString("\n") {
                codeGen.process(it)
            }.trim()

            if (code.isNotEmpty()) {
                execute(code)
            }
        }

        onClassAnnotation<DataSchema> { addDataSchemas(it) }

        beforeCellExecution {
            if (newDataSchemas.isNotEmpty()) {
                addDataSchemas(newDataSchemas)
                newDataSchemas.clear()
            }
        }

        val internalTypes = listOf(
            ColumnReference::class,
        ).map { it.createStarProjectedType(true) }

        markVariableInternal { property ->
            // TODO: add more conditions to include all generated properties and other internal stuff
            //  that should not be shown to user in Jupyter variables view
            internalTypes.any { property.returnType.isSubtypeOf(it) }
        }

        onColorSchemeChange {
            config.display.useDarkColorScheme = (it == ColorScheme.DARK)
        }
    }
}

public fun KotlinKernelHost.useSchemas(schemaClasses: Iterable<KClass<*>>) {
    newDataSchemas.addAll(schemaClasses)
}

public fun KotlinKernelHost.useSchemas(vararg schemaClasses: KClass<*>): Unit = useSchemas(schemaClasses.asIterable())

public inline fun <reified T> KotlinKernelHost.useSchema(): Unit = useSchemas(T::class)
