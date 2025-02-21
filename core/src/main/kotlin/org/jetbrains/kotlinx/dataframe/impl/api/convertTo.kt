package org.jetbrains.kotlinx.dataframe.impl.api

import io.github.oshai.kotlinlogging.KotlinLogging
import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.AnyRow
import org.jetbrains.kotlinx.dataframe.ColumnsSelector
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.DataRow
import org.jetbrains.kotlinx.dataframe.RowExpression
import org.jetbrains.kotlinx.dataframe.api.ConvertSchemaDsl
import org.jetbrains.kotlinx.dataframe.api.ConverterScope
import org.jetbrains.kotlinx.dataframe.api.ExcessiveColumns
import org.jetbrains.kotlinx.dataframe.api.Infer
import org.jetbrains.kotlinx.dataframe.api.add
import org.jetbrains.kotlinx.dataframe.api.all
import org.jetbrains.kotlinx.dataframe.api.allNulls
import org.jetbrains.kotlinx.dataframe.api.concat
import org.jetbrains.kotlinx.dataframe.api.convertTo
import org.jetbrains.kotlinx.dataframe.api.emptyDataFrame
import org.jetbrains.kotlinx.dataframe.api.isColumnGroup
import org.jetbrains.kotlinx.dataframe.api.isEmpty
import org.jetbrains.kotlinx.dataframe.api.map
import org.jetbrains.kotlinx.dataframe.api.name
import org.jetbrains.kotlinx.dataframe.api.toDataFrame
import org.jetbrains.kotlinx.dataframe.api.update
import org.jetbrains.kotlinx.dataframe.api.with
import org.jetbrains.kotlinx.dataframe.codeGen.MarkersExtractor
import org.jetbrains.kotlinx.dataframe.columns.ColumnGroup
import org.jetbrains.kotlinx.dataframe.columns.ColumnKind
import org.jetbrains.kotlinx.dataframe.columns.ColumnPath
import org.jetbrains.kotlinx.dataframe.columns.FrameColumn
import org.jetbrains.kotlinx.dataframe.columns.UnresolvedColumnsPolicy
import org.jetbrains.kotlinx.dataframe.columns.toColumnSet
import org.jetbrains.kotlinx.dataframe.exceptions.ExcessiveColumnsException
import org.jetbrains.kotlinx.dataframe.exceptions.TypeConversionException
import org.jetbrains.kotlinx.dataframe.impl.emptyPath
import org.jetbrains.kotlinx.dataframe.impl.getColumnPaths
import org.jetbrains.kotlinx.dataframe.impl.schema.createEmptyDataFrame
import org.jetbrains.kotlinx.dataframe.impl.schema.createNullFilledColumn
import org.jetbrains.kotlinx.dataframe.impl.schema.extractSchema
import org.jetbrains.kotlinx.dataframe.impl.schema.render
import org.jetbrains.kotlinx.dataframe.kind
import org.jetbrains.kotlinx.dataframe.schema.ColumnSchema
import org.jetbrains.kotlinx.dataframe.schema.DataFrameSchema
import org.jetbrains.kotlinx.dataframe.size
import kotlin.reflect.KType
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.full.withNullability
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.typeOf

private val logger = KotlinLogging.logger {}

private open class Converter(val transform: ConverterScope.(Any?) -> Any?, val skipNulls: Boolean)

private class Filler(val columns: ColumnsSelector<*, *>, val expr: RowExpression<*, *>)

internal interface ConvertSchemaDslInternal<T> : ConvertSchemaDsl<T> {
    public fun <C> fill(columns: ColumnsSelector<*, C>, expr: RowExpression<*, C>)
}

private class ConvertSchemaDslImpl<T> : ConvertSchemaDslInternal<T> {
    private val converters: MutableMap<Pair<KType, KType>, Converter> = mutableMapOf<Pair<KType, KType>, Converter>()

    val fillers = mutableListOf<Filler>()

    private val flexibleConverters: MutableMap<(KType, ColumnSchema) -> Boolean, Converter> = mutableMapOf()

    @Suppress("UNCHECKED_CAST")
    override fun <A, B> convert(from: KType, to: KType, converter: (A) -> B) {
        converters[from.withNullability(false) to to.withNullability(false)] =
            Converter({ converter(it as A) }, !from.isMarkedNullable)
    }

    override fun <C> fill(columns: ColumnsSelector<*, C>, expr: RowExpression<*, C>) {
        fillers.add(Filler(columns, expr))
    }

    override fun convertIf(condition: (KType, ColumnSchema) -> Boolean, converter: ConverterScope.(Any?) -> Any?) {
        flexibleConverters[condition] = Converter(converter, false)
    }

    /**
     * Attempts to find a converter for the given types. First it tries to find an exact match,
     * then it tries to find a flexible match where the first one will be used.
     */
    fun getConverter(fromType: KType, toSchema: ColumnSchema): Converter? =
        converters[fromType.withNullability(false) to toSchema.type.withNullability(false)]
            ?: flexibleConverters
                .entries
                .firstOrNull { (predicate, _) ->
                    predicate(fromType, toSchema)
                }?.value
}

@PublishedApi
internal fun AnyFrame.convertToImpl(
    type: KType,
    allowConversion: Boolean,
    excessiveColumns: ExcessiveColumns,
    body: ConvertSchemaDsl<Any>.() -> Unit = {},
): AnyFrame {
    val dsl = ConvertSchemaDslImpl<Any>()
    dsl.body()

    val missingPaths = mutableSetOf<ColumnPath>()

    fun AnyFrame.convertToSchema(schema: DataFrameSchema, path: ColumnPath): AnyFrame {
        // if current frame is empty
        if (this.isEmpty()) {
            return schema.createEmptyDataFrame()
        }

        val visited = mutableSetOf<String>()
        val newColumns = columns().mapNotNull { originalColumn ->
            val targetSchema = schema.columns[originalColumn.name()]
            if (targetSchema == null) {
                when (excessiveColumns) {
                    ExcessiveColumns.Fail -> throw ExcessiveColumnsException(listOf(originalColumn.name))
                    ExcessiveColumns.Keep -> originalColumn
                    ExcessiveColumns.Remove -> null
                }
            } else {
                visited.add(originalColumn.name())
                val currentSchema = originalColumn.extractSchema()
                when {
                    targetSchema == currentSchema -> originalColumn

                    !allowConversion -> {
                        val originalSchema = mapOf(originalColumn.name to currentSchema)
                            .render(0, StringBuilder(), "\t")

                        val targetSchema = mapOf(originalColumn.name to targetSchema)
                            .render(0, StringBuilder(), "\t")

                        throw IllegalArgumentException(
                            "Column has schema:\n $originalSchema\n that differs from target schema:\n $targetSchema",
                        )
                    }

                    else -> {
                        val columnPath = path + originalColumn.name

                        // try to perform any user-specified conversions first
                        val from = originalColumn.type()
                        val to = targetSchema.type
                        var converter = dsl.getConverter(from, targetSchema)

                        // special case for Char columns; check if we have any converters for String -> target
                        // if so, we can convert Char -> String -> target
                        if (converter == null && from.isSubtypeOf(typeOf<Char?>())) {
                            val stringConverter = dsl.getConverter(
                                fromType = typeOf<String>().withNullability(from.isMarkedNullable),
                                toSchema = targetSchema,
                            )
                            if (stringConverter != null) {
                                converter = Converter(
                                    transform = { stringConverter.transform(this, (it as Char?)?.toString()) },
                                    skipNulls = stringConverter.skipNulls,
                                )
                            }
                        }

                        val convertedColumn = if (converter != null) {
                            val nullsAllowed = to.isMarkedNullable
                            originalColumn.map(to, Infer.Nulls) {
                                val result =
                                    if (it != null || !converter.skipNulls) {
                                        converter.transform(ConverterScope(from, targetSchema), it)
                                    } else {
                                        it
                                    }

                                if (!nullsAllowed && result == null) {
                                    throw TypeConversionException(
                                        value = it,
                                        from = from,
                                        to = to,
                                        column = originalColumn.path(),
                                    )
                                }

                                result
                            }
                        } else {
                            null
                        }

                        when (targetSchema.kind) {
                            ColumnKind.Value ->
                                convertedColumn ?: originalColumn.convertTo(to)

                            ColumnKind.Group -> {
                                val column = when {
                                    convertedColumn != null -> convertedColumn

                                    // Value column of DataRows (if it ever occurs) can be converted to a group column
                                    originalColumn.kind == ColumnKind.Value &&
                                        originalColumn.all { it is DataRow<*> } ->
                                        DataColumn.createColumnGroup(
                                            name = originalColumn.name,
                                            df = originalColumn.values()
                                                .let { it as Iterable<DataRow<*>> }
                                                .toDataFrame(),
                                        ) as DataColumn<*>

                                    // Value column of nulls can be converted to an empty group column
                                    originalColumn.kind == ColumnKind.Value && originalColumn.allNulls() ->
                                        DataColumn.createColumnGroup(
                                            name = originalColumn.name,
                                            df = DataFrame.empty(nrow = originalColumn.size),
                                        ) as DataColumn<*>

                                    else -> originalColumn
                                }
                                require(column.isColumnGroup()) {
                                    "Column `${column.name}` is ${column.kind} and can not be converted to `ColumnGroup`"
                                }
                                DataColumn.createColumnGroup(
                                    name = column.name(),
                                    df = column.convertToSchema(
                                        schema = (targetSchema as ColumnSchema.Group).schema,
                                        path = columnPath,
                                    ),
                                )
                            }

                            ColumnKind.Frame -> {
                                val column = convertedColumn ?: originalColumn
                                val frameSchema = (targetSchema as ColumnSchema.Frame).schema

                                val frames = when (column.kind) {
                                    ColumnKind.Frame ->
                                        (column as FrameColumn<*>).values()

                                    ColumnKind.Value -> {
                                        require(
                                            column.all {
                                                it == null ||
                                                    it is AnyFrame ||
                                                    (it is List<*> && it.all { it is AnyRow? })
                                            },
                                        ) {
                                            "Column `${column.name}` is ValueColumn and contains objects that can not be converted into `DataFrame`"
                                        }
                                        column.values().map {
                                            when (it) {
                                                null -> emptyDataFrame()
                                                is AnyFrame -> it
                                                else -> (it as List<AnyRow?>).concat()
                                            }
                                        }
                                    }

                                    ColumnKind.Group -> {
                                        (column as ColumnGroup<*>).values().map { it.toDataFrame() }
                                    }
                                }

                                val convertedFrames = frames.map { it.convertToSchema(frameSchema, columnPath) }

                                DataColumn.createFrameColumn(
                                    name = column.name(),
                                    groups = convertedFrames,
                                    schema = lazy { frameSchema },
                                )
                            }
                        }
                    }
                }
            }
        }.toMutableList()

        // when the target is nullable but the source does not contain a column,
        // fill it in with nulls / empty dataframes
        val size = this.size.nrow
        schema.columns.forEach { (name, targetColumn) ->
            if (name !in visited) {
                try {
                    newColumns += targetColumn.createNullFilledColumn(name, size)
                } catch (e: IllegalStateException) {
                    logger.debug(e) { "" }
                    // if this could not be done automatically, they need to be filled manually
                    missingPaths.add(path + name)
                }
            }
        }
        return newColumns.toDataFrame()
    }

    val clazz = type.jvmErasure
    val marker = MarkersExtractor.get(clazz)
    var result = convertToSchema(marker.schema, emptyPath())

    /*
     * Here we handle all registered fillers of the user.
     * Fillers are registered in the DSL like:
     * ```kt
     * df.convertTo<Target> {
     *   fill { col1 and col2 }.with { something }
     *   fill { col3 }.with { somethingElse }
     * }
     * ```
     * Users can use this to fill up any column that was missing during the conversion.
     * They can also fill up and thus overwrite any existing column here.
     */
    dsl.fillers.forEach { filler ->
        // get all paths from the `fill { col1 and col2 }` part
        val paths = result.getColumnPaths(UnresolvedColumnsPolicy.Create, filler.columns).toSet()

        // split the paths into those that are already in the df and those that are missing
        val (newPaths, existingPaths) = paths.partition { it in missingPaths }

        // first fill cols that are already in the df using the `with {}` part of the dsl
        result = result.update { existingPaths.toColumnSet() }.with { filler.expr(this, this) }

        // then create any missing ones by filling using the `with {}` part of the dsl
        result = newPaths.fold(result) { df, newPath ->
            df.add(newPath, Infer.Type) { filler.expr(this, this) }
        }

        // remove the paths that are now filled
        missingPaths -= paths
    }

    // Inform the user which target columns could not be created in the conversion
    // The user will need to supply extra information for these, like `fill {}` them.
    if (missingPaths.isNotEmpty()) {
        throw IllegalArgumentException(
            "The following columns were not found in DataFrame: ${
                missingPaths.map { it.joinToString() }
            }, and their type was not nullable. Use `fill` to initialize these columns",
        )
    }

    return result
}
