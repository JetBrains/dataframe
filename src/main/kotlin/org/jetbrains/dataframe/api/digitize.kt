package org.jetbrains.dataframe

import org.jetbrains.dataframe.api.columns.DataCol
import kotlin.reflect.KClass

fun DataCol<Double>.digitize(vararg bins: Int, right: Boolean = false) = digitize(bins.toList(), Double::class, right)

fun <T: Comparable<T>> DataCol<T>.digitize(vararg bins: T, right: Boolean = false) = digitize(bins.toList(), right)

fun <T: Comparable<T>> DataCol<T>.digitize(bins: List<Int>, kclass: KClass<T>, right: Boolean = false): DataCol<Int> = digitize(bins.toList().map{convert(it, kclass)}, right)

fun <T: Comparable<T>> DataCol<T>.digitize(bins: List<T>, right: Boolean = false): DataCol<Int> {

    // TODO: use binary search
    // TODO: support descending order
    val predicate: (T, T) -> Boolean = if (right) { value, bin -> value <= bin } else { value, bin -> value < bin }

    return map { value ->
        val index = bins.indexOfFirst { predicate(value, it) }
        if (index == -1) bins.size
        else index
    }
}