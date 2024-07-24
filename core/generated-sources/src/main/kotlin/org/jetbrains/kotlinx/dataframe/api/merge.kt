package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.AnyRow
import org.jetbrains.kotlinx.dataframe.ColumnsSelector
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.DataRow
import org.jetbrains.kotlinx.dataframe.columns.ColumnAccessor
import org.jetbrains.kotlinx.dataframe.columns.ColumnPath
import org.jetbrains.kotlinx.dataframe.columns.ColumnReference
import org.jetbrains.kotlinx.dataframe.columns.toColumnSet
import org.jetbrains.kotlinx.dataframe.impl.api.removeImpl
import org.jetbrains.kotlinx.dataframe.impl.api.withRowCellImpl
import org.jetbrains.kotlinx.dataframe.impl.nameGenerator
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.typeOf

public fun <T, C> DataFrame<T>.merge(selector: ColumnsSelector<T, C>): Merge<T, C, List<C>> =
    Merge(this, selector, false, { it }, typeOf<Any?>(), Infer.Type)

public fun <T> DataFrame<T>.merge(vararg columns: String): Merge<T, Any?, List<Any?>> = merge { columns.toColumnSet() }

public inline fun <T, reified C> DataFrame<T>.merge(vararg columns: ColumnReference<C>): Merge<T, C, List<C>> =
    merge { columns.toColumnSet() }

public inline fun <T, reified C> DataFrame<T>.merge(vararg columns: KProperty<C>): Merge<T, C, List<C>> =
    merge { columns.toColumnSet() }

public data class Merge<T, C, R>(
    @PublishedApi
    internal val df: DataFrame<T>,
    @PublishedApi
    internal val selector: ColumnsSelector<T, C>,
    @PublishedApi
    internal val notNull: Boolean,
    @PublishedApi
    internal val transform: DataRow<T>.(List<C>) -> R,
    @PublishedApi
    internal val resultType: KType,
    @PublishedApi
    internal val infer: Infer,
)

public fun <T, C, R> Merge<T, C, R>.notNull(): Merge<T, C, R> = copy(notNull = true)

public fun <T, C, R> Merge<T, C, R>.into(columnName: String): DataFrame<T> = into(pathOf(columnName))

public fun <T, C, R> Merge<T, C, R>.into(column: ColumnAccessor<*>): DataFrame<T> = into(column.path())

public fun <T, C, R> Merge<T, C, R>.intoList(): List<R> =
    df.select(selector).rows().map { transform(it, it.values() as List<C>) }

public fun <T, C, R> Merge<T, C, R>.into(path: ColumnPath): DataFrame<T> {
    // If target path exists, merge into temp path
    val mergePath = if (df.getColumnOrNull(path) != null) pathOf(nameGenerator().addUnique("temp")) else path

    // move columns into group
    val grouped = df.move(selector).under { mergePath }

    var res = grouped.convert { getColumnGroup(mergePath) }.withRowCellImpl(resultType, infer) {
        val srcRow = df[index()]
        var values = it.values() as List<C>
        if (notNull) {
            values = values.filter {
                it != null && (it !is AnyRow || !it.isEmpty())
            }
        }
        transform(srcRow, values)
    }
    if (mergePath != path) {
        // target path existed before merge, but
        // it may have already been removed
        res = res
            .removeImpl(allowMissingColumns = true) { path }
            .df
            .move(mergePath).into { path }
    }
    return res
}

public fun <T, C, R> Merge<T, C, R>.asStrings(): Merge<T, C, String> = by(", ")

public fun <T, C, R> Merge<T, C, R>.by(
    separator: CharSequence = ", ",
    prefix: CharSequence = "",
    postfix: CharSequence = "",
    limit: Int = -1,
    truncated: CharSequence = "...",
): Merge<T, C, String> =
    Merge(
        df = df,
        selector = selector,
        notNull = notNull,
        transform = {
            it.joinToString(
                separator = separator,
                prefix = prefix,
                postfix = postfix,
                limit = limit,
                truncated = truncated,
            )
        },
        resultType = typeOf<String>(),
        infer = Infer.Nulls,
    )

public inline fun <T, C, R, reified V> Merge<T, C, R>.by(
    infer: Infer = Infer.Nulls,
    crossinline transform: DataRow<T>.(R) -> V,
): Merge<T, C, V> = Merge(df, selector, notNull, { transform(this@by.transform(this, it)) }, typeOf<V>(), infer)
