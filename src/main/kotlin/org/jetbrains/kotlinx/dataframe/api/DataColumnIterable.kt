package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.AnyCol
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.Predicate
import org.jetbrains.kotlinx.dataframe.columns.size
import org.jetbrains.kotlinx.dataframe.columns.values
import org.jetbrains.kotlinx.dataframe.impl.columns.guessColumnType
import org.jetbrains.kotlinx.dataframe.impl.createDataCollector
import org.jetbrains.kotlinx.dataframe.impl.getType
import org.jetbrains.kotlinx.dataframe.indices
import kotlin.reflect.KType

public fun <T> DataColumn<T>.asIterable(): Iterable<T> = values()
public fun <T> DataColumn<T>.asSequence(): Sequence<T> = asIterable().asSequence()

public fun <T> DataColumn<T>.all(predicate: Predicate<T>): Boolean = values.all(predicate)

public fun <T> DataColumn<T>.drop(predicate: Predicate<T>): DataColumn<T> = filter { !predicate(it) }

public fun <T> DataColumn<T>.filter(predicate: Predicate<T>): DataColumn<T> = indices.filter {
    predicate(get(it))
}.let { get(it) }

public fun <T> DataColumn<T>.forEach(action: (T) -> Unit): Unit = values.forEach(action)

public fun <T> DataColumn<T>.forEachIndexed(action: (Int, T) -> Unit): Unit = values.forEachIndexed(action)

public fun <T> DataColumn<T>.groupBy(cols: Iterable<AnyCol>): GroupBy<*, *> =
    (cols + this).toDataFrame().groupBy { cols(0 until ncol() - 1) }

public fun <T> DataColumn<T>.groupBy(vararg cols: AnyCol): GroupBy<*, *> = groupBy(cols.toList())

public fun <T, R> DataColumn<T>.map(transform: (T) -> R): DataColumn<R> {
    val collector = createDataCollector(size)
    values.forEach { collector.add(transform(it)) }
    return collector.toColumn(name).cast()
}

public fun <T, R> DataColumn<T?>.mapNotNull(transform: (T) -> R): DataColumn<R> {
    val collector = createDataCollector(size)
    values.forEach {
        if (it == null) collector.add(null)
        else collector.add(transform(it))
    }
    return collector.toColumn(name).cast()
}

public inline fun <T, reified R> DataColumn<T>.mapInline(crossinline transform: (T) -> R): DataColumn<R> {
    val newValues = Array(size()) { transform(get(it)) }.asList()
    val resType = getType<R>()
    return guessColumnType(
        name(),
        newValues,
        suggestedType = resType,
        suggestedTypeIsUpperBound = false,
        nullable = if(!resType.isMarkedNullable) false else null
    )
}

public fun <T, R> DataColumn<T>.map(type: KType?, transform: (T) -> R): DataColumn<R> {
    if (type == null) return map(transform)
    val collector = createDataCollector<R>(size, type)
    values.forEach { collector.add(transform(it)) }
    return collector.toColumn(name) as DataColumn<R>
}

public fun <T> DataColumn<T>.first(): T = get(0)
public fun <T> DataColumn<T>.firstOrNull(): T? = if (size > 0) first() else null
public fun <T> DataColumn<T>.first(predicate: (T) -> Boolean): T = values.first(predicate)
public fun <T> DataColumn<T>.firstOrNull(predicate: (T) -> Boolean): T? = values.firstOrNull(predicate)
public fun <T> DataColumn<T>.last(): T = get(size - 1)
public fun <T> DataColumn<T>.lastOrNull(): T? = if (size > 0) last() else null
public fun <C> DataColumn<C>.allNulls(): Boolean = size == 0 || all { it == null }
public fun <C> DataColumn<C>.single(): C = values.single()

// region take/drop

public fun <T> DataColumn<T>.dropLast(n: Int = 1): DataColumn<T> = take(size - n)
public fun <T> DataColumn<T>.takeLast(n: Int): DataColumn<T> = drop(size - n)
public fun <T> DataColumn<T>.drop(n: Int): DataColumn<T> = when {
    n == 0 -> this
    n >= size -> get(emptyList())
    else -> get(n until size)
}

public fun <T> DataColumn<T>.take(n: Int): DataColumn<T> = when {
    n == 0 -> get(emptyList())
    n >= size -> this
    else -> get(0 until n)
}

// endregion
