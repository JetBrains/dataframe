package org.jetbrains.kotlinx.dataframe.math

import org.jetbrains.kotlinx.dataframe.api.skipNA_default
import org.jetbrains.kotlinx.dataframe.impl.renderType
import java.math.BigDecimal
import java.math.BigInteger
import kotlin.reflect.KType
import kotlin.reflect.full.withNullability

@PublishedApi
internal fun <T : Number> Iterable<T>.mean(type: KType, skipNA: Boolean = skipNA_default): Double =
    asSequence().mean(type, skipNA)

@Suppress("UNCHECKED_CAST")
internal fun <T : Number> Sequence<T>.mean(type: KType, skipNA: Boolean = skipNA_default): Double {
    if (type.isMarkedNullable) {
        return filterNotNull().mean(type.withNullability(false), skipNA)
    }
    return when (type.classifier) {
        Double::class -> (this as Sequence<Double>).mean(skipNA)

        Float::class -> (this as Sequence<Float>).mean(skipNA)

        Int::class -> (this as Sequence<Int>).map { it.toDouble() }.mean(false)

        // for integer values NA is not possible
        Short::class -> (this as Sequence<Short>).map { it.toDouble() }.mean(false)

        Byte::class -> (this as Sequence<Byte>).map { it.toDouble() }.mean(false)

        Long::class -> (this as Sequence<Long>).map { it.toDouble() }.mean(false)

        BigInteger::class -> (this as Sequence<BigInteger>).map { it.toDouble() }.mean(false)

        BigDecimal::class -> (this as Sequence<BigDecimal>).map { it.toDouble() }.mean(skipNA)

        Number::class -> (this as Sequence<Number>).map { it.toDouble() }.mean(skipNA)

        // this means the sequence is empty
        Nothing::class -> Double.NaN

        else -> throw IllegalArgumentException("Unable to compute the mean for type ${renderType(type)}")
    }
}

internal fun Sequence<Double>.mean(skipNA: Boolean = skipNA_default): Double {
    var count = 0
    var sum: Double = 0.toDouble()
    for (element in this) {
        if (element.isNaN()) {
            if (skipNA) {
                continue
            } else {
                return Double.NaN
            }
        }
        sum += element
        count++
    }
    return if (count > 0) sum / count else Double.NaN
}

@JvmName("meanFloat")
internal fun Sequence<Float>.mean(skipNA: Boolean = skipNA_default): Double {
    var count = 0
    var sum: Double = 0.toDouble()
    for (element in this) {
        if (element.isNaN()) {
            if (skipNA) {
                continue
            } else {
                return Double.NaN
            }
        }
        sum += element
        count++
    }
    return if (count > 0) sum / count else Double.NaN
}

@JvmName("doubleMean")
internal fun Iterable<Double>.mean(skipNA: Boolean = skipNA_default): Double = asSequence().mean(skipNA)

@JvmName("floatMean")
internal fun Iterable<Float>.mean(skipNA: Boolean = skipNA_default): Double = asSequence().mean(skipNA)

@JvmName("intMean")
internal fun Iterable<Int>.mean(): Double =
    if (this is Collection) {
        if (size > 0) sumOf { it.toDouble() } / size else Double.NaN
    } else {
        var count = 0
        val sum = sumOf {
            count++
            it.toDouble()
        }
        if (count > 0) sum / count else Double.NaN
    }

@JvmName("shortMean")
internal fun Iterable<Short>.mean(): Double =
    if (this is Collection) {
        if (size > 0) sumOf { it.toDouble() } / size else Double.NaN
    } else {
        var count = 0
        val sum = sumOf {
            count++
            it.toDouble()
        }
        if (count > 0) sum / count else Double.NaN
    }

@JvmName("byteMean")
internal fun Iterable<Byte>.mean(): Double =
    if (this is Collection) {
        if (size > 0) sumOf { it.toDouble() } / size else Double.NaN
    } else {
        var count = 0
        val sum = sumOf {
            count++
            it.toDouble()
        }
        if (count > 0) sum / count else Double.NaN
    }

@JvmName("longMean")
internal fun Iterable<Long>.mean(): Double =
    if (this is Collection) {
        if (size > 0) sumOf { it.toDouble() } / size else Double.NaN
    } else {
        var count = 0
        val sum = sumOf {
            count++
            it.toDouble()
        }
        if (count > 0) sum / count else Double.NaN
    }

// TODO result is Double, but should be BigDecimal, Issue #558
@JvmName("bigIntegerMean")
internal fun Iterable<BigInteger>.mean(): Double =
    if (this is Collection) {
        if (size > 0) sumOf { it.toDouble() } / size else Double.NaN
    } else {
        var count = 0
        val sum = sumOf {
            count++
            it.toDouble()
        }
        if (count > 0) sum / count else Double.NaN
    }

// TODO result is Double, but should be BigDecimal, Issue #558
@JvmName("bigDecimalMean")
internal fun Iterable<BigDecimal>.mean(): Double =
    if (this is Collection) {
        if (size > 0) sum().toDouble() / size else Double.NaN
    } else {
        var count = 0
        val sum = sumOf {
            count++
            it.toDouble()
        }
        if (count > 0) sum / count else Double.NaN
    }
