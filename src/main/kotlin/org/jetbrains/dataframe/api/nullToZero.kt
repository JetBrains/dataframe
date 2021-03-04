package org.jetbrains.dataframe

import org.jetbrains.dataframe.columns.ColumnReference
import org.jetbrains.dataframe.columns.ColumnSet
import java.math.BigDecimal
import kotlin.reflect.KType
import kotlin.reflect.jvm.jvmErasure

fun <T> DataFrame<T>.nullToZero(selector: ColumnsSelector<T, Number?>): DataFrame<T> {
    val cols = getColumnsWithPaths(selector).groupBy { it.type }

    return cols.asIterable().fold(this) { df, group ->
        df.nullColumnToZero(group.key, group.value)
    }
}

fun <T> DataFrame<T>.nullToZero(vararg cols: String) = nullToZero { cols.toColumns() as ColumnSet<Number?> }
fun <T> DataFrame<T>.nullToZero(vararg cols: ColumnReference<Number?>) = nullToZero { cols.toColumns() }
fun <T> DataFrame<T>.nullToZero(cols: Iterable<ColumnReference<Number?>>) = nullToZero { cols.toColumnSet() }

internal fun <T> DataFrame<T>.nullColumnToZero(type: KType, cols: Iterable<ColumnReference<Number?>>) =
        when (type.jvmErasure) {
            Double::class -> fillNulls(cols).with { .0 }
            Int::class -> fillNulls(cols).with { 0 }
            Long::class -> fillNulls(cols).with { 0L }
            BigDecimal::class -> fillNulls(cols).with { BigDecimal.ZERO }
            else -> throw IllegalArgumentException()
        }
