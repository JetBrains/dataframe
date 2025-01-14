package org.jetbrains.kotlinx.dataframe

import org.jetbrains.kotlinx.dataframe.aggregation.Aggregatable
import org.jetbrains.kotlinx.dataframe.aggregation.AggregateGroupedBody
import org.jetbrains.kotlinx.dataframe.annotations.AccessApiOverload
import org.jetbrains.kotlinx.dataframe.annotations.HasSchema
import org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl
import org.jetbrains.kotlinx.dataframe.api.add
import org.jetbrains.kotlinx.dataframe.api.cast
import org.jetbrains.kotlinx.dataframe.api.getRows
import org.jetbrains.kotlinx.dataframe.api.indices
import org.jetbrains.kotlinx.dataframe.api.rows
import org.jetbrains.kotlinx.dataframe.api.select
import org.jetbrains.kotlinx.dataframe.api.toDataFrame
import org.jetbrains.kotlinx.dataframe.columns.UnresolvedColumnsPolicy
import org.jetbrains.kotlinx.dataframe.columns.toColumnSet
import org.jetbrains.kotlinx.dataframe.impl.DataFrameImpl
import org.jetbrains.kotlinx.dataframe.impl.DataFrameSize
import org.jetbrains.kotlinx.dataframe.impl.getColumnsImpl
import org.jetbrains.kotlinx.dataframe.impl.headPlusArray
import org.jetbrains.kotlinx.dataframe.impl.headPlusIterable
import org.jetbrains.kotlinx.dataframe.impl.schema.createEmptyDataFrame
import org.jetbrains.kotlinx.dataframe.impl.schema.createEmptyDataFrameOf
import org.jetbrains.kotlinx.dataframe.schema.DataFrameSchema
import kotlin.reflect.KType

/**
 * Readonly interface for an ordered list of [columns][DataColumn].
 *
 * Columns in `DataFrame` have distinct non-empty [names][DataColumn.name] and equal [sizes][DataColumn.size].
 *
 * @param T Schema marker. It identifies column schema and is used to generate schema-specific extension properties for typed data access. It is covariant, so `DataFrame<A>` is assignable to variable of type `DataFrame<B>` if `A` is a subtype of `B`.
 */
@HasSchema(schemaArg = 0)
@org.jetbrains.annotations.Debug.Renderer(
    text = "\"DataFrame \" + org.jetbrains.kotlinx.dataframe.DataFrameKt.size(this)",
    childrenArray = "this.columns1().toArray()",
)
public interface DataFrame<out T> :
    Aggregatable<T>,
    ColumnsContainer<T> {

    public companion object {
        public val Empty: AnyFrame = DataFrameImpl<Unit>(emptyList(), 0)

        public fun empty(nrow: Int = 0): AnyFrame = if (nrow == 0) Empty else DataFrameImpl<Unit>(emptyList(), nrow)

        /**
         * Creates a DataFrame with empty columns (rows = 0).
         * Can be used as a "null object" in aggregation operations, operations that work on columns (select, reorder, ...)
         *
         */
        public inline fun <reified T> emptyOf(): DataFrame<T> = createEmptyDataFrameOf(T::class).cast()

        /**
         * Creates a DataFrame with empty columns (rows = 0).
         * Can be used as a "null object" in aggregation operations, operations that work on columns (select, reorder, ...)
         */
        public fun empty(schema: DataFrameSchema): AnyFrame = schema.createEmptyDataFrame()
    }

    // region columns

    public fun columnNames(): List<String>

    public fun columnTypes(): List<KType>

    // endregion

    // region rows

    public fun rowsCount(): Int

    public operator fun iterator(): Iterator<DataRow<T>> = rows().iterator()

    public fun rowsList(): List<DataRow<T>> = rows().toList()

    // endregion

    public fun <R> aggregate(body: AggregateGroupedBody<T, R>): DataRow<T>

    // region get columns

    /**
     * Returns a list of columns selected by [columns], a [ColumnsSelectionDsl].
     *
     * NOTE: This doesn't work in [ColumnsSelectionDsl], use [ColumnsSelectionDsl.cols] to select columns by predicate.
     */
    override fun <C> get(columns: ColumnsSelector<T, C>): List<DataColumn<C>> =
        getColumnsImpl(UnresolvedColumnsPolicy.Fail, columns)

    // endregion

    // region get rows

    public operator fun get(index: Int): DataRow<T>

    public operator fun get(indices: Iterable<Int>): DataFrame<T> = getRows(indices)

    public operator fun get(range: IntRange): DataFrame<T> = getRows(range)

    public operator fun get(first: IntRange, vararg ranges: IntRange): DataFrame<T> =
        getRows(headPlusArray(first, ranges).asSequence().flatMap { it.asSequence() }.asIterable())

    public operator fun get(firstIndex: Int, vararg otherIndices: Int): DataFrame<T> =
        get(headPlusIterable(firstIndex, otherIndices.asIterable()))

    // endregion

    // region plus columns

    public operator fun plus(col: AnyBaseCol): DataFrame<T> = add(col)

    public operator fun plus(cols: Iterable<AnyBaseCol>): DataFrame<T> = (columns() + cols).toDataFrame().cast()

    // endregion
}

// region get columns

/**
 * Returns a list of columns selected by [columns], a [ColumnsSelectionDsl].
 */
public operator fun <T, C> DataFrame<T>.get(columns: ColumnsSelector<T, C>): List<DataColumn<C>> = this.get(columns)

@AccessApiOverload
public operator fun <T> DataFrame<T>.get(first: AnyColumnReference, vararg other: AnyColumnReference): DataFrame<T> =
    select { (listOf(first) + other).toColumnSet() }

public operator fun <T> DataFrame<T>.get(first: String, vararg other: String): DataFrame<T> =
    select { (listOf(first) + other).toColumnSet() }

public operator fun <T> DataFrame<T>.get(columnRange: ClosedRange<String>): DataFrame<T> =
    select { columnRange.start..columnRange.endInclusive }

// endregion

internal val ColumnsContainer<*>.ncol get() = columnsCount()
internal val AnyFrame.nrow get() = rowsCount()
internal val AnyFrame.indices get() = indices()
internal val AnyFrame.size: DataFrameSize get() = size()

public fun AnyFrame.size(): DataFrameSize = DataFrameSize(ncol, nrow)
