package org.jetbrains.dataframe

import org.jetbrains.dataframe.api.columns.DataCol
import java.math.BigDecimal
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.jvm.jvmErasure

inline fun <reified T : Comparable<T>> Iterable<T>.median(): Double {
    val sorted = sorted()
    val size = sorted.size
    val index = size / 2
    return when (T::class) {
        Double::class -> if (size % 2 == 0) (sorted[index - 1] as Double + sorted[index] as Double) / 2.0 else sorted[index] as Double
        Int::class -> if (size % 2 == 0) (sorted[index - 1] as Int + sorted[index] as Int) / 2.0 else (sorted[index] as Int).toDouble()
        Long::class -> if (size % 2 == 0) (sorted[index - 1] as Long + sorted[index] as Long) / 2.0 else (sorted[index] as Long).toDouble()
        else -> throw IllegalArgumentException()
    }
}

class Counter(var value: Int = 0){
    operator fun inc(): Counter {
        value++
        return this
    }
}

fun <T> Iterable<T>.computeSize(counter: Counter) = map {
    counter.inc()
    it
}

internal fun Int.zeroToOne() = if(this == 0) 1 else this

fun <T: Number> Iterable<T>.sum(clazz: KClass<T>) = when (clazz) {
    Double::class -> (this as Iterable<Double>).sum() as T
    Int::class -> (this as Iterable<Int>).sum() as T
    Long::class -> (this as Iterable<Long>).sum() as T
    BigDecimal::class -> (this as Iterable<BigDecimal>).sum() as T
    else -> throw IllegalArgumentException()
}

fun Iterable<BigDecimal>.sum(): BigDecimal {
    var sum: BigDecimal = BigDecimal.ZERO
    for (element in this) {
        sum += element
    }
    return sum
}

inline fun <reified T : Number> sum(list: Iterable<T>): T = list.sum(T::class)

fun <T: Number> DataCol<T>.sum() = values.sum(type.jvmErasure as KClass<T>)

inline fun <T, reified D : Comparable<D>> DataFrame<T>.median(col: ColumnDef<D?>): Double = get(col).median()
inline fun <T, reified D : Comparable<D>> DataFrame<T>.median(crossinline selector: RowSelector<T, D?>): Double = rows().asSequence().map { selector(it, it) }.filterNotNull().asIterable().median()
inline fun <T, reified D : Comparable<D>> DataFrame<T>.median(col: KProperty<D?>): Double = get(col).median()
