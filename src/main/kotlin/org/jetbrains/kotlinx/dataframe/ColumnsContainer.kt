package org.jetbrains.kotlinx.dataframe

import org.jetbrains.kotlinx.dataframe.api.cast
import org.jetbrains.kotlinx.dataframe.api.castFrameColumn
import org.jetbrains.kotlinx.dataframe.api.getColumn
import org.jetbrains.kotlinx.dataframe.api.name
import org.jetbrains.kotlinx.dataframe.columns.ColumnGroup
import org.jetbrains.kotlinx.dataframe.columns.ColumnPath
import org.jetbrains.kotlinx.dataframe.columns.ColumnReference
import org.jetbrains.kotlinx.dataframe.columns.ColumnWithPath
import org.jetbrains.kotlinx.dataframe.columns.FrameColumn
import org.jetbrains.kotlinx.dataframe.impl.columnName
import org.jetbrains.kotlinx.dataframe.impl.columns.asColumnGroup
import org.jetbrains.kotlinx.dataframe.impl.columns.asFrameColumn
import kotlin.reflect.KProperty

public interface ColumnsContainer<out T> {

    public fun columns(): List<AnyCol>
    public fun columnsCount(): Int

    // region getColumnOrNull

    public fun getColumnOrNull(name: String): AnyCol?
    public fun getColumnOrNull(index: Int): AnyCol?
    public fun <R> getColumnOrNull(column: ColumnReference<R>): DataColumn<R>?
    public fun getColumnOrNull(path: ColumnPath): AnyCol?
    public fun <R> getColumnOrNull(column: ColumnSelector<T, R>): DataColumn<R>?

    // endregion

    // region get

    public operator fun get(columnName: String): AnyCol = getColumn(columnName)
    public operator fun get(columnPath: ColumnPath): AnyCol = getColumn(columnPath)

    public operator fun <R> get(column: DataColumn<R>): DataColumn<R> = getColumn(column.name()).cast()

    public operator fun <R> get(column: ColumnReference<R>): DataColumn<R> = getColumn(column)
    public operator fun <R> get(column: ColumnReference<DataRow<R>>): ColumnGroup<R> = getColumn(column)
    public operator fun <R> get(column: ColumnReference<DataFrame<R>>): FrameColumn<R> = getColumn(column)

    public operator fun <R> get(column: KProperty<R>): DataColumn<R> = get(column.columnName).cast()
    public operator fun <R> get(column: KProperty<DataRow<R>>): ColumnGroup<R> = get(column.columnName).asColumnGroup().cast()
    public operator fun <R> get(column: KProperty<DataFrame<R>>): FrameColumn<R> = get(column.columnName).asFrameColumn().castFrameColumn()

    public operator fun <C> get(columns: ColumnsSelector<T, C>): List<DataColumn<C>>
    public operator fun <C> get(column: ColumnSelector<T, C>): DataColumn<C> = get(column as ColumnsSelector<T, C>).single()

    // endregion

    public fun <R> resolve(reference: ColumnReference<R>): ColumnWithPath<R>?

    public fun asColumnGroup(name: String = ""): ColumnGroup<*>
    public fun asColumnGroup(column: ColumnGroupAccessor): ColumnGroup<*> = asColumnGroup(column.name)
}
