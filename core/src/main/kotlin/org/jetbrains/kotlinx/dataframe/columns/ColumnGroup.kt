package org.jetbrains.kotlinx.dataframe.columns

import org.jetbrains.kotlinx.dataframe.AnyCol
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.DataRow
import org.jetbrains.kotlinx.dataframe.annotations.DataSchema
import org.jetbrains.kotlinx.dataframe.annotations.HasSchema
import org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl
import org.jetbrains.kotlinx.dataframe.api.asColumnGroup
import org.jetbrains.kotlinx.dataframe.api.columnGroup
import kotlin.reflect.KProperty

/**
 * Group of columns. Used to create column hierarchy in [DataFrame].
 *
 * ColumnGroup is a mix of [DataFrame] and [DataColumn] that supports all [DataFrame] operations but also has [column name][name] and [column type][type].
 * It derives not from [DataColumn], but from [BaseColumn] to avoid API clashes between [DataFrame] and [DataColumn].
 *
 * ColumnGroup interface can be returned by:
 * - extension property generated for [DataSchema]
 * - [ColumnAccessor] created by [columnGroup] delegate
 * - explicit cast using [asColumnGroup]
 *
 * Can be instantiated by [DataColumn.createColumnGroup].
 *
 * @param T Schema marker. See [DataFrame] for details.
 */
@HasSchema(schemaArg = 0)
@org.jetbrains.annotations.Debug.Renderer(text = "\" ColumnGroup \"")
public interface ColumnGroup<out T> :
    BaseColumn<DataRow<T>>,
    DataFrame<T> {

    /**
     * Gets the rows at given indices.
     *
     * NOTE: This doesn't work in the [ColumnsSelectionDsl], use [ColumnsSelectionDsl.cols] to select columns by index.
     */
    override fun get(indices: Iterable<Int>): ColumnGroup<T>

    override fun get(columnName: String): AnyCol

    override fun kind(): ColumnKind = ColumnKind.Group

    override fun distinct(): ColumnGroup<T>

    /**
     * Gets the rows at given indices.
     *
     * NOTE: This doesn't work in the [ColumnsSelectionDsl], use [ColumnsSelectionDsl.cols] to select columns by index.
     */
    override fun get(firstIndex: Int, vararg otherIndices: Int): ColumnGroup<T>

    /**
     * Gets the rows at given range of indices.
     *
     * NOTE: This doesn't work in the [ColumnsSelectionDsl], use [ColumnsSelectionDsl.cols] to select columns by range.
     */
    override fun get(range: IntRange): ColumnGroup<T>

    override fun rename(newName: String): ColumnGroup<T>

    override operator fun getValue(thisRef: Any?, property: KProperty<*>): ColumnGroup<T> =
        super.getValue(thisRef, property) as ColumnGroup<T>
}
