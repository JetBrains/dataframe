package org.jetbrains.dataframe.impl

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.api.columns.DataColumn
import org.jetbrains.dataframe.api.columns.ColumnWithPath
import org.jetbrains.dataframe.io.renderToString
import java.lang.IllegalArgumentException
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.jvm.jvmErasure

internal open class DataFrameImpl<T>(var columns: List<AnyCol>) : DataFrame<T> {

    private val nrow: Int = columns.firstOrNull()?.size ?: 0

    override fun nrow() = nrow

    init {

        val invalidSizeColumns = columns.filter { it.size != nrow() }
        if (invalidSizeColumns.size > 0)
            throw IllegalArgumentException("Invalid column sizes: ${invalidSizeColumns}") // TODO

        val columnNames = columns.groupBy { it.name() }.filter { it.value.size > 1 }.map { it.key }
        if (columnNames.size > 0)
            throw IllegalArgumentException("Duplicate column names: ${columnNames}. All column names: ${columnNames()}")
    }


    private val columnsMap by lazy { columns.withIndex().associateBy({ it.value.name() }, { it.index }).toMutableMap() }

    override fun rows() = object : Iterable<DataRow<T>> {
        override fun iterator() =

                object : Iterator<DataRow<T>> {
                    var curRow = 0

                    override fun hasNext(): Boolean = curRow < nrow()

                    override fun next() = get(curRow++)!!
                }
    }


    override fun getColumnIndex(columnName: String) = columnsMap[columnName] ?: -1

    override fun equals(other: Any?): Boolean {
        val df = other as? AnyFrame ?: return false
        return columns == df.columns()
    }

    override fun hashCode() = columns.hashCode()

    override fun toString() = renderToString()

    override fun append(vararg values: Any?): DataFrame<T> {
        assert(values.size % ncol() != 0) { "Invalid number of arguments. Multiple of ${ncol()} is expected, but actual was: ${values.size}" }
        return values.mapIndexed { i, v ->
            val col = columns()[i]
            if (v != null)
            // Note: type arguments for a new value are not validated here because they are erased
                assert(v.javaClass.kotlin.isSubclassOf(col.type.jvmErasure))
            col.withValues(col.values + listOf(v), col.hasNulls || v == null)
        }.asDataFrame<T>()
    }

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<DataRow<T>>? {
        return DataColumn.createGroup("", this).addPath(emptyList())
    }

    override fun set(columnName: String, value: AnyCol) {

        if(value.size != nrow())
            throw IllegalArgumentException("Invalid column size for column '$columnName'. Expected: ${nrow()}, actual: ${value.size}")

        val renamed = value.rename(columnName)
        val index = getColumnIndex(columnName)
        val newCols = if(index == -1) columns + renamed else columns.mapIndexed { i, col -> if(i == index) renamed else col }
        columnsMap[columnName] = if(index == -1) ncol() else index
        columns = newCols
    }

    override fun columns() = columns
}