package org.jetbrains.kotlinx.dataframe.jupyter

import org.jetbrains.dataframe.impl.codeGen.ReplCodeGenerator
import org.jetbrains.kotlinx.dataframe.AnyCol
import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.AnyRow
import org.jetbrains.kotlinx.dataframe.annotations.DataSchema
import org.jetbrains.kotlinx.dataframe.api.FormattedFrame
import org.jetbrains.kotlinx.dataframe.api.Gather
import org.jetbrains.kotlinx.dataframe.api.GroupBy
import org.jetbrains.kotlinx.dataframe.api.Merge
import org.jetbrains.kotlinx.dataframe.api.Pivot
import org.jetbrains.kotlinx.dataframe.api.PivotGroupBy
import org.jetbrains.kotlinx.dataframe.api.ReducedPivot
import org.jetbrains.kotlinx.dataframe.api.ReducedPivotGroupBy
import org.jetbrains.kotlinx.dataframe.api.Split
import org.jetbrains.kotlinx.dataframe.api.SplitWithTransform
import org.jetbrains.kotlinx.dataframe.api.frames
import org.jetbrains.kotlinx.dataframe.api.into
import org.jetbrains.kotlinx.dataframe.api.toDataFrame
import org.jetbrains.kotlinx.dataframe.api.values
import org.jetbrains.kotlinx.dataframe.codeGen.CodeWithConverter
import org.jetbrains.kotlinx.dataframe.columns.ColumnReference
import org.jetbrains.kotlinx.dataframe.dataFrameOf
import org.jetbrains.kotlinx.dataframe.dataTypes.IMG
import org.jetbrains.kotlinx.dataframe.impl.createStarProjectedType
import org.jetbrains.kotlinx.dataframe.io.HtmlData
import org.jetbrains.kotlinx.dataframe.ncol
import org.jetbrains.kotlinx.dataframe.nrow
import org.jetbrains.kotlinx.dataframe.size
import org.jetbrains.kotlinx.dataframe.stubs.DataFrameToListNamedStub
import org.jetbrains.kotlinx.dataframe.stubs.DataFrameToListTypedStub
import org.jetbrains.kotlinx.jupyter.api.HTML
import org.jetbrains.kotlinx.jupyter.api.KotlinKernelHost
import org.jetbrains.kotlinx.jupyter.api.VariableName
import org.jetbrains.kotlinx.jupyter.api.declare
import org.jetbrains.kotlinx.jupyter.api.libraries.JupyterIntegration
import org.jetbrains.kotlinx.jupyter.api.libraries.resources
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.full.isSubtypeOf

internal val newDataSchemas = mutableListOf<KClass<*>>()

internal class Integration : JupyterIntegration() {

    override fun Builder.onLoaded() {
        val codeGen = ReplCodeGenerator.create()
        val config = JupyterConfiguration()

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
                        val initJsSha = "012be928db1deacaafbab5c94b96a297d8272f12"
                        url("https://cdn.jsdelivr.net/gh/Kotlin/dataframe@$initJsSha/src/main/resources/init.js")
                    }
                }

                css("DataFrameTable") { classPath("table.css") }
                css("DataFrameFormatting") { classPath("formatting.css") }
            }
        }

        with(JupyterHtmlRenderer(config.display, this)) {
            render<HtmlData> { it.toJupyter() }
            render<AnyFrame> ({ it })
            render<FormattedFrame<*>>({ it.df }, modifyConfig = { getDisplayConfiguration(it) })
            render<AnyRow>({ it.toDataFrame() }, { "DataRow [${it.ncol}]" })
            render<AnyCol>({ dataFrameOf(it) }, { "DataColumn [${it.nrow}]" })
            render<GroupBy<*, *>>({ it.toDataFrame() }, { "GroupBy" })
            render<Pivot<*>>({ it.frames().toDataFrame() }, { "Pivot: ${it.ncol} columns" })
            render<ReducedPivot<*>>({ it.values().toDataFrame() }, { "ReducedPivot: ${it.ncol} columns" })
            render<PivotGroupBy<*>>({ it.frames() }, { "PivotGroupBy: ${it.size}" })
            render<ReducedPivotGroupBy<*>>({ it.values() }, { "ReducedPivotGroupBy: ${it.size}" })
            render<SplitWithTransform<*, *, *>> ({ it.into() }, { "Split" })
            render<Split<*, *>> ({ it.toDataFrame() }, { "Split" })
            render<Merge<*, *, *>> ({ it.into("merged") }, { "Merge" })
            render<Gather<*, *, *, *>> ({ it.into("key", "value") }, { "Gather" })
            render<IMG> { HTML("<img src=\"${it.url}\"/>") }
        }

        import("org.jetbrains.kotlinx.dataframe.api.*")
        import("org.jetbrains.kotlinx.dataframe.*")
        import("org.jetbrains.kotlinx.dataframe.annotations.*")
        import("org.jetbrains.kotlinx.dataframe.io.*")
        import("java.net.URL")
        import("org.jetbrains.kotlinx.dataframe.dataTypes.*")

        fun KotlinKernelHost.execute(codeWithConverter: CodeWithConverter, property: KProperty<*>): VariableName? {
            val code = codeWithConverter.with(property.name)
            return if (code.isNotBlank()) {
                val result = execute(code)
                if (codeWithConverter.hasConverter) {
                    result.name
                } else null
            } else null
        }

        updateVariable<AnyFrame> { df, property ->
            execute(codeGen.process(df, property), property)
        }

        updateVariable<AnyRow> { row, property ->
            execute(codeGen.process(row, property), property)
        }

        updateVariable<DataFrameToListNamedStub> { stub, prop ->
            val code = codeGen.process(stub).with(prop.name)
            execute(code).name
        }

        updateVariable<DataFrameToListTypedStub> { stub, prop ->
            val code = codeGen.process(stub).with(prop.name)
            execute(code).name
        }

        fun KotlinKernelHost.addDataSchemas(classes: List<KClass<*>>) {
            val code = classes.map {
                codeGen.process(it)
            }.joinToString("\n").trim()

            if (code.isNotEmpty()) {
                execute(code)
            }
        }

        onClassAnnotation<DataSchema> { addDataSchemas(it) }

        afterCellExecution { snippet, result ->
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
    }
}

public fun KotlinKernelHost.useSchemas(schemaClasses: Iterable<KClass<*>>) {
    newDataSchemas.addAll(schemaClasses)
}

public fun KotlinKernelHost.useSchemas(vararg schemaClasses: KClass<*>): Unit = useSchemas(schemaClasses.asIterable())

public inline fun <reified T> KotlinKernelHost.useSchema(): Unit = useSchemas(T::class)
