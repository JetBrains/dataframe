package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.AnyBaseCol
import org.jetbrains.kotlinx.dataframe.AnyCol
import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.AnyRow
import org.jetbrains.kotlinx.dataframe.ColumnGroupReference
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.DataRow
import org.jetbrains.kotlinx.dataframe.RowExpression
import org.jetbrains.kotlinx.dataframe.annotations.AccessApiOverload
import org.jetbrains.kotlinx.dataframe.annotations.Interpretable
import org.jetbrains.kotlinx.dataframe.annotations.Refine
import org.jetbrains.kotlinx.dataframe.columns.ColumnAccessor
import org.jetbrains.kotlinx.dataframe.columns.ColumnPath
import org.jetbrains.kotlinx.dataframe.columns.ColumnReference
import org.jetbrains.kotlinx.dataframe.columns.FrameColumn
import org.jetbrains.kotlinx.dataframe.columns.TypeSuggestion
import org.jetbrains.kotlinx.dataframe.exceptions.DuplicateColumnNamesException
import org.jetbrains.kotlinx.dataframe.exceptions.UnequalColumnSizesException
import org.jetbrains.kotlinx.dataframe.impl.ColumnNameGenerator
import org.jetbrains.kotlinx.dataframe.impl.DataFrameImpl
import org.jetbrains.kotlinx.dataframe.impl.UNNAMED_COLUMN_PREFIX
import org.jetbrains.kotlinx.dataframe.impl.api.withValuesImpl
import org.jetbrains.kotlinx.dataframe.impl.asList
import org.jetbrains.kotlinx.dataframe.impl.columnName
import org.jetbrains.kotlinx.dataframe.impl.columns.ColumnAccessorImpl
import org.jetbrains.kotlinx.dataframe.impl.columns.createColumnGuessingType
import org.jetbrains.kotlinx.dataframe.impl.columns.createComputedColumnReference
import org.jetbrains.kotlinx.dataframe.impl.columns.forceResolve
import org.jetbrains.kotlinx.dataframe.impl.columns.unbox
import org.jetbrains.kotlinx.dataframe.size
import kotlin.random.Random
import kotlin.random.nextInt
import kotlin.reflect.KProperty
import kotlin.reflect.full.withNullability
import kotlin.reflect.typeOf

// region create ColumnAccessor

// region column

public fun <T> column(): ColumnDelegate<T> = ColumnDelegate()

public fun <T> column(name: String): ColumnAccessor<T> = ColumnAccessorImpl(name)

public fun <T> column(path: ColumnPath): ColumnAccessor<T> = ColumnAccessorImpl(path)

@AccessApiOverload
public fun <T> column(property: KProperty<T>): ColumnAccessor<T> = ColumnAccessorImpl(property.name)

public fun <T> ColumnGroupReference.column(): ColumnDelegate<T> = ColumnDelegate(this)

public fun <T> ColumnGroupReference.column(name: String): ColumnAccessor<T> = ColumnAccessorImpl(path() + name)

public fun <T> ColumnGroupReference.column(path: ColumnPath): ColumnAccessor<T> = ColumnAccessorImpl(this.path() + path)

@AccessApiOverload
public fun <T> ColumnGroupReference.column(property: KProperty<T>): ColumnAccessor<T> =
    ColumnAccessorImpl(this.path() + property.name)

public inline fun <reified T> column(
    name: String = "",
    infer: Infer = Infer.Nulls,
    noinline expression: RowExpression<Any?, T>,
): ColumnReference<T> = createComputedColumnReference(name, typeOf<T>(), infer, expression)

@Suppress("UNCHECKED_CAST", "UNUSED_PARAMETER")
public inline fun <T, reified C> column(
    df: DataFrame<T>,
    name: String = "",
    infer: Infer = Infer.Nulls,
    noinline expression: RowExpression<T, C>,
): ColumnReference<C> = createComputedColumnReference(name, typeOf<C>(), infer, expression as RowExpression<Any?, C>)

// endregion

// region valueColumn

public fun valueColumn(): ColumnDelegate<Any?> = column()

@JvmName("valueColumnTyped")
public fun <T> valueColumn(): ColumnDelegate<T> = column()

public fun valueColumn(name: String): ColumnAccessor<Any?> = column(name)

@JvmName("valueColumnTyped")
public fun <T> valueColumn(name: String): ColumnAccessor<T> = column(name)

public fun valueColumn(path: ColumnPath): ColumnAccessor<Any?> = column(path)

@JvmName("valueColumnTyped")
public fun <T> valueColumn(path: ColumnPath): ColumnAccessor<T> = column(path)

@AccessApiOverload
public fun <T> valueColumn(property: KProperty<T>): ColumnAccessor<T> = column(property.name)

public fun ColumnGroupReference.valueColumn(): ColumnDelegate<Any?> = ColumnDelegate(this)

@JvmName("valueColumnTyped")
public fun <T> ColumnGroupReference.valueColumn(): ColumnDelegate<T> = ColumnDelegate(this)

public fun ColumnGroupReference.valueColumn(name: String): ColumnAccessor<Any?> = ColumnAccessorImpl(path() + name)

@JvmName("valueColumnTyped")
public fun <T> ColumnGroupReference.valueColumn(name: String): ColumnAccessor<T> = ColumnAccessorImpl(path() + name)

public fun ColumnGroupReference.valueColumn(path: ColumnPath): ColumnAccessor<Any?> =
    ColumnAccessorImpl(this.path() + path)

@JvmName("valueColumnTyped")
public fun <T> ColumnGroupReference.valueColumn(path: ColumnPath): ColumnAccessor<T> =
    ColumnAccessorImpl(this.path() + path)

@AccessApiOverload
public fun <T> ColumnGroupReference.valueColumn(property: KProperty<T>): ColumnAccessor<T> =
    ColumnAccessorImpl(this.path() + property.name)

// endregion

// region columnGroup

public fun columnGroup(): ColumnDelegate<AnyRow> = column()

@JvmName("columnGroupTyped")
public fun <T> columnGroup(): ColumnDelegate<DataRow<T>> = column()

public fun columnGroup(name: String): ColumnAccessor<AnyRow> = column(name)

@JvmName("columnGroupTyped")
public fun <T> columnGroup(name: String): ColumnAccessor<DataRow<T>> = column(name)

public fun columnGroup(path: ColumnPath): ColumnAccessor<AnyRow> = column(path)

@JvmName("columnGroupTyped")
public fun <T> columnGroup(path: ColumnPath): ColumnAccessor<DataRow<T>> = column(path)

@JvmName("columnGroupDataRowKProperty")
@AccessApiOverload
public fun <T> columnGroup(property: KProperty<DataRow<T>>): ColumnAccessor<DataRow<T>> = column(property)

@AccessApiOverload
public fun <T> columnGroup(property: KProperty<T>): ColumnAccessor<DataRow<T>> = column(property.name)

public fun ColumnGroupReference.columnGroup(): ColumnDelegate<AnyRow> = ColumnDelegate(this)

@JvmName("columnGroupTyped")
public fun <T> ColumnGroupReference.columnGroup(): ColumnDelegate<DataRow<T>> = ColumnDelegate(this)

public fun ColumnGroupReference.columnGroup(name: String): ColumnAccessor<AnyRow> = ColumnAccessorImpl(path() + name)

@JvmName("columnGroupTyped")
public fun <T> ColumnGroupReference.columnGroup(name: String): ColumnAccessor<DataRow<T>> =
    ColumnAccessorImpl(path() + name)

public fun ColumnGroupReference.columnGroup(path: ColumnPath): ColumnAccessor<AnyRow> =
    ColumnAccessorImpl(this.path() + path)

@JvmName("columnGroupTyped")
public fun <T> ColumnGroupReference.columnGroup(path: ColumnPath): ColumnAccessor<DataRow<T>> =
    ColumnAccessorImpl(this.path() + path)

@JvmName("columnGroupDataRowKProperty")
@AccessApiOverload
public fun <T> ColumnGroupReference.columnGroup(property: KProperty<DataRow<T>>): ColumnAccessor<DataRow<T>> =
    ColumnAccessorImpl(this.path() + property.name)

@AccessApiOverload
public fun <T> ColumnGroupReference.columnGroup(property: KProperty<T>): ColumnAccessor<DataRow<T>> =
    ColumnAccessorImpl(this.path() + property.name)

// endregion

// region frameColumn

public fun frameColumn(): ColumnDelegate<AnyFrame> = column()

@JvmName("frameColumnTyped")
public fun <T> frameColumn(): ColumnDelegate<DataFrame<T>> = column()

public fun frameColumn(name: String): ColumnAccessor<AnyFrame> = column(name)

@JvmName("frameColumnTyped")
public fun <T> frameColumn(name: String): ColumnAccessor<DataFrame<T>> = column(name)

public fun frameColumn(path: ColumnPath): ColumnAccessor<AnyFrame> = column(path)

@JvmName("frameColumnTyped")
public fun <T> frameColumn(path: ColumnPath): ColumnAccessor<DataFrame<T>> = column(path)

@JvmName("frameColumnDataFrameKProperty")
@AccessApiOverload
public fun <T> frameColumn(property: KProperty<DataFrame<T>>): ColumnAccessor<DataFrame<T>> = column(property)

@AccessApiOverload
public fun <T> frameColumn(property: KProperty<List<T>>): ColumnAccessor<DataFrame<T>> = column(property.name)

public fun ColumnGroupReference.frameColumn(): ColumnDelegate<AnyFrame> = ColumnDelegate(this)

@JvmName("frameColumnTyped")
public fun <T> ColumnGroupReference.frameColumn(): ColumnDelegate<DataFrame<T>> = ColumnDelegate(this)

public fun ColumnGroupReference.frameColumn(name: String): ColumnAccessor<AnyFrame> = ColumnAccessorImpl(path() + name)

@JvmName("frameColumnTyped")
public fun <T> ColumnGroupReference.frameColumn(name: String): ColumnAccessor<DataFrame<T>> =
    ColumnAccessorImpl(path() + name)

public fun ColumnGroupReference.frameColumn(path: ColumnPath): ColumnAccessor<AnyFrame> =
    ColumnAccessorImpl(this.path() + path)

@JvmName("frameColumnTyped")
public fun <T> ColumnGroupReference.frameColumn(path: ColumnPath): ColumnAccessor<DataFrame<T>> =
    ColumnAccessorImpl(this.path() + path)

@JvmName("frameColumnDataFrameKProperty")
@AccessApiOverload
public fun <T> ColumnGroupReference.frameColumn(property: KProperty<DataFrame<T>>): ColumnAccessor<DataFrame<T>> =
    ColumnAccessorImpl(this.path() + property.name)

@AccessApiOverload
public fun <T> ColumnGroupReference.frameColumn(property: KProperty<List<T>>): ColumnAccessor<DataFrame<T>> =
    ColumnAccessorImpl(this.path() + property.name)

// endregion

public class ColumnDelegate<T>(private val parent: ColumnGroupReference? = null) {
    public operator fun getValue(thisRef: Any?, property: KProperty<*>): ColumnAccessor<T> = named(property.columnName)

    public infix fun named(name: String): ColumnAccessor<T> =
        parent?.let { ColumnAccessorImpl(it.path() + name) } ?: ColumnAccessorImpl(name)
}

// endregion

// region create DataColumn

public inline fun <reified T> columnOf(vararg values: T): DataColumn<T> =
    createColumnGuessingType(
        values = values.asIterable(),
        suggestedType = TypeSuggestion.InferWithUpperbound(typeOf<T>()),
        listifyValues = false,
        allColsMakesColGroup = true,
    ).forceResolve()

public fun columnOf(vararg values: AnyBaseCol): DataColumn<AnyRow> = columnOf(values.asIterable()).forceResolve()

public fun <T> columnOf(vararg frames: DataFrame<T>): FrameColumn<T> = columnOf(frames.asIterable()).forceResolve()

public fun columnOf(columns: Iterable<AnyBaseCol>): DataColumn<AnyRow> =
    DataColumn.createColumnGroup(
        name = "",
        df = dataFrameOf(columns),
    )
        .asDataColumn()
        .forceResolve()

public fun <T> columnOf(frames: Iterable<DataFrame<T>>): FrameColumn<T> =
    DataColumn.createFrameColumn(
        name = "",
        groups = frames.toList(),
    ).forceResolve()

public inline fun <reified T> column(values: Iterable<T>): DataColumn<T> =
    createColumnGuessingType(
        values = values,
        suggestedType = TypeSuggestion.Use(typeOf<T>()),
        allColsMakesColGroup = true,
    ).forceResolve()

// endregion

// region create DataFrame

/**
 * Creates new [DataFrame] with given [columns]
 *
 * All named columns must have unique names. For columns with empty names unique column names are generated: "untitled", "untitiled1", "untitled2" etc.
 *
 * All columns must have equal sizes.
 *
 * @throws [DuplicateColumnNamesException] if column names are not unique
 * @throws [UnequalColumnSizesException] if column size are not equal
 * @param columns columns for [DataFrame]
 */
public fun dataFrameOf(columns: Iterable<AnyBaseCol>): DataFrame<*> {
    val cols = columns.map { it.unbox() }
    val nrow = if (cols.isEmpty()) 0 else cols[0].size
    return DataFrameImpl<Unit>(cols, nrow)
}

public fun dataFrameOf(vararg header: ColumnReference<*>): DataFrameBuilder = DataFrameBuilder(header.map { it.name() })

public fun dataFrameOf(vararg columns: AnyBaseCol): DataFrame<*> = dataFrameOf(columns.asIterable())

@Interpretable("DataFrameOf0")
public fun dataFrameOf(vararg header: String): DataFrameBuilder = dataFrameOf(header.toList())

public inline fun <reified C> dataFrameOf(
    vararg header: String,
    crossinline fill: (String) -> Iterable<C>,
): DataFrame<*> = dataFrameOf(header.asIterable()).invoke(fill)

public fun dataFrameOf(header: Iterable<String>): DataFrameBuilder = DataFrameBuilder(header.asList())

@Refine
@Interpretable("DataFrameOf3")
public fun dataFrameOf(vararg columns: Pair<String, List<Any?>>): DataFrame<*> =
    columns.map { it.second.toColumn(it.first, Infer.Type) }.toDataFrame()

public fun dataFrameOf(header: Iterable<String>, values: Iterable<Any?>): DataFrame<*> =
    dataFrameOf(header).withValues(values)

public inline fun <T, reified C> dataFrameOf(header: Iterable<T>, fill: (T) -> Iterable<C>): DataFrame<*> =
    header.map { value ->
        DataColumn.createByInference(
            name = value.toString(),
            values = fill(value).asList(),
            suggestedType = TypeSuggestion.InferWithUpperbound(typeOf<C>()),
        )
    }.toDataFrame()

public fun dataFrameOf(header: CharProgression): DataFrameBuilder = dataFrameOf(header.map { it.toString() })

public class DataFrameBuilder(private val header: List<String>) {

    public operator fun invoke(vararg columns: AnyCol): DataFrame<*> = invoke(columns.asIterable())

    public operator fun invoke(columns: Iterable<AnyCol>): DataFrame<*> {
        val cols = columns.asList()
        require(cols.size == header.size) { "Number of columns differs from number of column names" }
        return cols.mapIndexed { i, col ->
            col.rename(header[i])
        }.toDataFrame()
    }

    @Refine
    @Interpretable("DataFrameBuilderInvoke0")
    public operator fun invoke(vararg values: Any?): DataFrame<*> = withValues(values.asIterable())

    @JvmName("invoke1")
    internal fun withValues(values: Iterable<Any?>): DataFrame<*> =
        withValuesImpl(header, values.asList()).map { (name, values) ->
            DataColumn.createByInference(name, values)
        }.toDataFrame()

    public operator fun invoke(args: Sequence<Any?>): DataFrame<*> = invoke(*args.toList().toTypedArray())

    public fun withColumns(columnBuilder: (String) -> AnyCol): DataFrame<*> =
        header
            .map { columnBuilder(it) named it } // create a columns and make sure to rename them to the given header
            .toDataFrame()

    public inline operator fun <reified T> invoke(crossinline valuesBuilder: (String) -> Iterable<T>): DataFrame<*> =
        withColumns { name ->
            DataColumn.createByInference(
                name = name,
                values = valuesBuilder(name).asList(),
                suggestedType = TypeSuggestion.InferWithUpperbound(typeOf<T>()),
            )
        }

    public inline fun <reified C> fill(nrow: Int, value: C): DataFrame<*> =
        withColumns { name ->
            DataColumn.createValueColumn(
                name = name,
                values = List(nrow) { value },
                type = typeOf<C>().withNullability(value == null),
            )
        }

    public fun fill(nrow: Int, dataFrame: AnyFrame): DataFrame<*> =
        withColumns { name ->
            DataColumn.createFrameColumn(
                name = name,
                groups = List(nrow) { dataFrame },
                schema = lazy { dataFrame.schema() },
            )
        }

    public inline fun <reified C> nulls(nrow: Int): DataFrame<*> = fill<C?>(nrow, null)

    public inline fun <reified C> fillIndexed(nrow: Int, crossinline init: (Int, String) -> C): DataFrame<*> =
        withColumns { name ->
            DataColumn.createByInference(
                name = name,
                values = List(nrow) { init(it, name) },
            )
        }

    public inline fun <reified C> fill(nrow: Int, crossinline init: (Int) -> C): DataFrame<*> =
        withColumns { name ->
            DataColumn.createByInference(
                name = name,
                values = List(nrow, init),
            )
        }

    private inline fun <reified C : Any> fillNotNull(nrow: Int, crossinline init: (Int) -> C) =
        withColumns { name ->
            DataColumn.createValueColumn(
                name = name,
                values = List(nrow, init),
                type = typeOf<C>(),
            )
        }

    public fun randomInt(nrow: Int): DataFrame<*> = fillNotNull(nrow) { Random.nextInt() }

    public fun randomInt(nrow: Int, range: IntRange): DataFrame<*> = fillNotNull(nrow) { Random.nextInt(range) }

    public fun randomDouble(nrow: Int): DataFrame<*> = fillNotNull(nrow) { Random.nextDouble() }

    public fun randomDouble(nrow: Int, range: ClosedRange<Double>): DataFrame<*> =
        fillNotNull(nrow) { Random.nextDouble(range.start, range.endInclusive) }

    public fun randomFloat(nrow: Int): DataFrame<*> = fillNotNull(nrow) { Random.nextFloat() }

    public fun randomLong(nrow: Int): DataFrame<*> = fillNotNull(nrow) { Random.nextLong() }

    public fun randomLong(nrow: Int, range: ClosedRange<Long>): DataFrame<*> =
        fillNotNull(nrow) { Random.nextLong(range.start, range.endInclusive) }

    public fun randomBoolean(nrow: Int): DataFrame<*> = fillNotNull(nrow) { Random.nextBoolean() }
}

/**
 * Helper class for implementing operations when column names can be potentially duplicated.
 * For example, operations involving multiple dataframes, computed columns or parsing some third-party data
 */
public class DynamicDataFrameBuilder {
    private var cols: MutableList<AnyCol> = mutableListOf()
    private val generator = ColumnNameGenerator()

    public fun add(col: AnyCol): String {
        val uniqueName = if (col.name().isEmpty()) {
            generator.addUnique(UNNAMED_COLUMN_PREFIX)
        } else {
            generator.addUnique(col.name())
        }
        val renamed = if (uniqueName != col.name()) {
            col.rename(uniqueName)
        } else {
            col
        }
        cols.add(renamed)
        return uniqueName
    }

    public fun toDataFrame(): DataFrame<*> = dataFrameOf(cols)
}

/**
 * Returns [DataFrame] with no rows and no columns.
 *
 * To create [DataFrame] with empty columns or empty rows see [DataFrame.empty]
 *
 * @param T schema marker for [DataFrame]
 */
public fun <T> emptyDataFrame(): DataFrame<T> = DataFrame.empty().cast()

// endregion

// region create ColumnPath

public fun pathOf(vararg columnNames: String): ColumnPath = ColumnPath(columnNames.asList())

// endregion
