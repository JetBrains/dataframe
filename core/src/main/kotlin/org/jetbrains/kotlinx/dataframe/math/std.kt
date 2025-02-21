package org.jetbrains.kotlinx.dataframe.math

import org.jetbrains.kotlinx.dataframe.api.ddof_default
import org.jetbrains.kotlinx.dataframe.api.skipNA_default
import org.jetbrains.kotlinx.dataframe.impl.renderType
import java.math.BigDecimal
import java.math.BigInteger
import kotlin.reflect.KType
import kotlin.reflect.full.withNullability

@Suppress("UNCHECKED_CAST")
@PublishedApi
internal fun <T : Number> Iterable<T?>.std(
    type: KType,
    skipNA: Boolean = skipNA_default,
    ddof: Int = ddof_default,
): Double {
    if (type.isMarkedNullable) {
        return when {
            skipNA -> filterNotNull().std(type = type.withNullability(false), skipNA = true, ddof = ddof)
            contains(null) -> Double.NaN
            else -> std(type = type.withNullability(false), skipNA = false, ddof = ddof)
        }
    }
    return when (type.classifier) {
        Double::class -> (this as Iterable<Double>).std(skipNA, ddof)
        Float::class -> (this as Iterable<Float>).std(skipNA, ddof)
        Int::class, Short::class, Byte::class -> (this as Iterable<Int>).std(ddof)
        Long::class -> (this as Iterable<Long>).std(ddof)
        BigDecimal::class -> (this as Iterable<BigDecimal>).std(ddof)
        BigInteger::class -> (this as Iterable<BigInteger>).std(ddof)
        Number::class -> (this as Iterable<Number>).map { it.toDouble() }.std(skipNA, ddof)
        Nothing::class -> Double.NaN
        else -> throw IllegalArgumentException("Unable to compute the std for type ${renderType(type)}")
    }
}

@JvmName("doubleStd")
internal fun Iterable<Double>.std(skipNA: Boolean = skipNA_default, ddof: Int = ddof_default): Double =
    varianceAndMean(skipNA)?.std(ddof) ?: Double.NaN

@JvmName("floatStd")
internal fun Iterable<Float>.std(skipNA: Boolean = skipNA_default, ddof: Int = ddof_default): Double =
    varianceAndMean(skipNA)?.std(ddof) ?: Double.NaN

@JvmName("intStd")
internal fun Iterable<Int>.std(ddof: Int = ddof_default): Double = varianceAndMean().std(ddof)

@JvmName("longStd")
internal fun Iterable<Long>.std(ddof: Int = ddof_default): Double = varianceAndMean().std(ddof)

@JvmName("bigDecimalStd")
internal fun Iterable<BigDecimal>.std(ddof: Int = ddof_default): Double = varianceAndMean().std(ddof)

@JvmName("bigIntegerStd")
internal fun Iterable<BigInteger>.std(ddof: Int = ddof_default): Double = varianceAndMean().std(ddof)
