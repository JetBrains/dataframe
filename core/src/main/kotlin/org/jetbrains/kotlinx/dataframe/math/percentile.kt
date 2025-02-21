package org.jetbrains.kotlinx.dataframe.math

import org.jetbrains.kotlinx.dataframe.impl.asList
import java.math.BigDecimal
import java.math.BigInteger
import kotlin.reflect.KType
import kotlin.reflect.typeOf

@PublishedApi
internal inline fun <reified T : Comparable<T>> Iterable<T?>.percentile(
    percentile: Double,
    type: KType = typeOf<T>(),
): T? {
    require(percentile in 0.0..100.0) { "Percentile must be in range [0, 100]" }

    @Suppress("UNCHECKED_CAST")
    val list = if (type.isMarkedNullable) filterNotNull() else (this as Iterable<T>).asList()
    val size = list.size
    if (size == 0) return null

    val index = (percentile / 100.0 * (size - 1)).toInt()
    val fraction = (percentile / 100.0 * (size - 1)) - index

    // median handle for even sized list (legacy logic)
    if (percentile == 50.0 && size % 2 == 0) {
        val lower = list.quickSelect(index)
        val upper = list.quickSelect(index + 1)

        return when (type.classifier) {
            Double::class -> ((lower as Double + upper as Double) / 2.0) as T
            Float::class -> ((lower as Float + upper as Float) / 2.0f) as T
            Int::class -> ((lower as Int + upper as Int) / 2) as T
            Short::class -> ((lower as Short + upper as Short) / 2).toShort() as T
            Long::class -> ((lower as Long + upper as Long) / 2L) as T
            Byte::class -> ((lower as Byte + upper as Byte) / 2).toByte() as T
            BigDecimal::class -> ((lower as BigDecimal + upper as BigDecimal) / 2.toBigDecimal()) as T
            BigInteger::class -> ((lower as BigInteger + upper as BigInteger) / 2.toBigInteger()) as T
            else -> lower
        }
    }

    if (fraction == 0.0) {
        return list.quickSelect(index)
    }

    val lower = list.quickSelect(index)
    val upper = list.quickSelect(index + 1)

    return when (type.classifier) {
        Double::class -> ((lower as Double) + (upper as Double - lower as Double) * fraction) as T

        Float::class -> ((lower as Float) + (upper as Float - lower as Float) * fraction) as T

        Int::class -> ((lower as Int) + (upper as Int - lower as Int) * fraction).toInt() as T

        Short::class -> ((lower as Short) + (upper as Short - lower as Short) * fraction).toInt().toShort() as T

        Long::class -> ((lower as Long) + (upper as Long - lower as Long) * fraction).toLong() as T

        Byte::class -> ((lower as Byte) + (upper as Byte - lower as Byte) * fraction).toInt().toByte() as T

        BigDecimal::class -> (
            (lower as BigDecimal) +
                (upper as BigDecimal - lower as BigDecimal) * fraction.toBigDecimal()
        ) as T

        BigInteger::class -> (
            (lower as BigInteger) +
                (
                    (upper as BigInteger - lower as BigInteger) * fraction.toBigDecimal()
                        .toBigInteger()
                )
        ) as T

        else -> lower
    }
}

@PublishedApi
internal fun <T : Comparable<T>> List<T>.quickSelect(k: Int): T {
    if (k < 0 || k >= size) throw IndexOutOfBoundsException("k = $k, size = $size")

    var list = this
    var temp = mutableListOf<T>()
    var less = mutableListOf<T>()
    var k = k
    var greater = mutableListOf<T>()
    while (list.size > 1) {
        var equal = 0
        val x = list.random()
        greater.clear()
        less.clear()
        for (v in list) {
            val comp = v.compareTo(x)
            when {
                comp < 0 -> less.add(v)
                comp > 0 -> greater.add(v)
                else -> equal++
            }
        }
        when {
            k < less.size -> {
                list = less
                less = temp
                temp = list
            }

            k < less.size + equal -> {
                return x
            }

            else -> {
                list = greater
                greater = temp
                temp = list
                k -= less.size + equal
            }
        }
    }
    return list[0]
}
