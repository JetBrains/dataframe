package org.jetbrains.kotlinx.dataframe.impl.api

import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.AnyRow
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.RowValueFilter
import org.jetbrains.kotlinx.dataframe.Selector
import org.jetbrains.kotlinx.dataframe.api.AddDataRow
import org.jetbrains.kotlinx.dataframe.api.AddDataRowImpl
import org.jetbrains.kotlinx.dataframe.api.UpdateClause
import org.jetbrains.kotlinx.dataframe.api.cast
import org.jetbrains.kotlinx.dataframe.api.indices
import org.jetbrains.kotlinx.dataframe.api.name
import org.jetbrains.kotlinx.dataframe.api.replace
import org.jetbrains.kotlinx.dataframe.api.toDataFrame
import org.jetbrains.kotlinx.dataframe.api.with
import org.jetbrains.kotlinx.dataframe.columns.ColumnGroup
import org.jetbrains.kotlinx.dataframe.columns.FrameColumn
import org.jetbrains.kotlinx.dataframe.columns.size
import org.jetbrains.kotlinx.dataframe.impl.createDataCollector
import org.jetbrains.kotlinx.dataframe.type
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.withNullability
import kotlin.reflect.jvm.jvmErasure

@PublishedApi
internal fun <T, C> UpdateClause<T, C>.updateImpl(expression: (AddDataRow<T>, DataColumn<C>, C) -> C?): DataFrame<T> = df.replace(columns).with { it.updateImpl(df, filter, expression) }

internal fun <T, C> UpdateClause<T, C>.updateWithValuePerColumnImpl(selector: Selector<DataColumn<C>, C>) = df.replace(columns).with {
    val value = selector(it, it)
    val convertedValue = value?.convertTo(it.type()) as C
    it.updateImpl(df, filter) { _, _, _ -> convertedValue }
}

internal fun <T, C> DataColumn<C>.updateImpl(
    df: DataFrame<T>,
    filter: RowValueFilter<T, C>?,
    expression: (AddDataRow<T>, DataColumn<C>, C) -> C?
): DataColumn<C> {
    val collector = createDataCollector<C>(size, type)
    val src = this
    if (filter == null) {
        df.indices().forEach { rowIndex ->
            val row = AddDataRowImpl(rowIndex, df, collector.values)
            collector.add(expression(row, src, src[rowIndex]))
        }
    } else {
        df.indices().forEach { rowIndex ->
            val row = AddDataRowImpl(rowIndex, df, collector.values)
            val currentValue = row[src]
            val newValue =
                if (filter.invoke(row, currentValue)) expression(row, src, currentValue) else currentValue
            collector.add(newValue)
        }
    }
    return collector.toColumn(src.name).cast()
}

/**
 * Replaces all values in column asserting that new values are compatible with current column kind
 */
internal fun <T> DataColumn<T>.updateWith(values: List<T>): DataColumn<T> = when (this) {
    is FrameColumn<*> -> {
        values.forEach {
            require(it is AnyFrame) { "Can not add value '$it' to FrameColumn" }
        }
        val groups = (values as List<AnyFrame>)
        DataColumn.createFrameColumn(name, groups) as DataColumn<T>
    }
    is ColumnGroup<*> -> {
        this.columns().mapIndexed { colIndex, col ->
            val newValues = values.map {
                when (it) {
                    null -> null
                    is List<*> -> it[colIndex]
                    is AnyRow -> it.tryGet(col.name)
                    else -> require(false) { "Can not add value '$it' to MapColumn" }
                }
            }
            col.updateWith(newValues)
        }.toDataFrame().let { DataColumn.createColumnGroup(name, it) } as DataColumn<T>
    }
    else -> {
        var nulls = false
        val kclass = type.jvmErasure
        values.forEach {
            when (it) {
                null -> nulls = true
                else -> {
                    require(it.javaClass.kotlin.isSubclassOf(kclass)) { "Can not add value '$it' to column '$name' of type $type" }
                }
            }
        }
        DataColumn.createValueColumn(name, values, type.withNullability(nulls))
    }
}
