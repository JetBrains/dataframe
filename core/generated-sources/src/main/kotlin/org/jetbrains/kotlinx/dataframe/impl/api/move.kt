package org.jetbrains.kotlinx.dataframe.impl.api

import org.jetbrains.kotlinx.dataframe.AnyColumnReference
import org.jetbrains.kotlinx.dataframe.ColumnSelector
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl
import org.jetbrains.kotlinx.dataframe.api.MoveClause
import org.jetbrains.kotlinx.dataframe.api.cast
import org.jetbrains.kotlinx.dataframe.api.getColumn
import org.jetbrains.kotlinx.dataframe.api.getColumnGroup
import org.jetbrains.kotlinx.dataframe.api.getColumnWithPath
import org.jetbrains.kotlinx.dataframe.api.toDataFrame
import org.jetbrains.kotlinx.dataframe.columns.ColumnWithPath
import org.jetbrains.kotlinx.dataframe.columns.UnresolvedColumnsPolicy
import org.jetbrains.kotlinx.dataframe.impl.DataFrameReceiver
import org.jetbrains.kotlinx.dataframe.impl.columns.toColumnWithPath
import org.jetbrains.kotlinx.dataframe.impl.columns.tree.ColumnPosition
import org.jetbrains.kotlinx.dataframe.impl.columns.tree.getOrPut

// TODO: support 'before' mode
internal fun <T, C> MoveClause<T, C>.afterOrBefore(column: ColumnSelector<T, *>, isAfter: Boolean): DataFrame<T> {
    val removeResult = df.removeImpl(columns = columns)

    val targetPath = df.getColumnWithPath(column).path
    val removeRoot = removeResult.removedColumns.first().getRoot()

    val refNode = removeRoot.getOrPut(targetPath) {
        val parentPath = it.dropLast(1)
        val parent = if (parentPath.isEmpty()) df else df.getColumnGroup(parentPath)
        val index = parent.getColumnIndex(it.last())
        val col = df.getColumn(index)
        ColumnPosition(index, false, col)
    }

    val parentPath = targetPath.dropLast(1)
    val toInsert = removeResult.removedColumns.map {
        val path = parentPath + it.name
        ColumnToInsert(path, it.toColumnWithPath<C>().data, refNode)
    }
    return removeResult.df.insertImpl(toInsert)
}

internal fun <T, C> MoveClause<T, C>.moveImpl(
    under: Boolean = false,
    newPathExpression: ColumnsSelectionDsl<T>.(ColumnWithPath<C>) -> AnyColumnReference,
): DataFrame<T> {
    val receiver = object : DataFrameReceiver<T>(df, UnresolvedColumnsPolicy.Fail), ColumnsSelectionDsl<T> {}
    val removeResult = df.removeImpl(columns = columns)
    val columnsToInsert = removeResult.removedColumns.map {
        val col = it.toColumnWithPath<C>()
        var path = newPathExpression(receiver, col).path()
        if (under) path += col.name()
        ColumnToInsert(path, col.data, it)
    }
    return removeResult.df.insertImpl(columnsToInsert)
}

internal fun <T, C> MoveClause<T, C>.moveTo(columnIndex: Int): DataFrame<T> {
    val removed = df.removeImpl(columns = columns)
    val remainingColumns = removed.df.columns()
    val targetIndex = if (columnIndex > remainingColumns.size) remainingColumns.size else columnIndex
    val newColumnList =
        remainingColumns.subList(0, targetIndex) +
            removed.removedColumns.map { it.data.column as DataColumn<C> } +
            if (targetIndex < remainingColumns.size) {
                remainingColumns.subList(targetIndex, remainingColumns.size)
            } else {
                emptyList()
            }
    return newColumnList.toDataFrame().cast()
}
