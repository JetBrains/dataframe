@file:Suppress("DuplicatedCode")

package org.jetbrains.kotlinx.dataframe.math

import org.jetbrains.kotlinx.dataframe.api.skipNA_default
import java.math.BigDecimal
import java.math.BigInteger
import kotlin.math.sqrt

internal data class BasicStats(val count: Int, val mean: Double, val variance: Double) {

    fun std(ddof: Int): Double {
        if (count <= ddof) return Double.NaN
        return sqrt(variance / (count - ddof))
    }
}

@JvmName("doubleVarianceAndMean")
internal fun Iterable<Double>.varianceAndMean(skipNA: Boolean = skipNA_default): BasicStats? {
    var count = 0
    var sum = .0
    for (element in this) {
        if (element.isNaN()) {
            if (skipNA) {
                continue
            } else {
                return null
            }
        }
        sum += element
        count++
    }
    val mean = sum / count
    var variance = .0
    for (element in this) {
        if (element.isNaN()) continue
        val diff = element - mean
        variance += diff * diff
    }
    return BasicStats(count, mean, variance)
}

@JvmName("floatVarianceAndMean")
internal fun Iterable<Float>.varianceAndMean(skipNA: Boolean = skipNA_default): BasicStats? {
    var count = 0
    var sum = .0
    for (element in this) {
        if (element.isNaN()) {
            if (skipNA) {
                continue
            } else {
                return null
            }
        }
        sum += element
        count++
    }
    val mean = sum / count
    var variance = .0
    for (element in this) {
        if (element.isNaN()) continue
        val diff = element - mean
        variance += diff * diff
    }
    return BasicStats(count, mean, variance)
}

@JvmName("intVarianceAndMean")
internal fun Iterable<Int>.varianceAndMean(): BasicStats {
    var count = 0
    var sum = .0
    for (element in this) {
        sum += element
        count++
    }
    val mean = sum / count
    var variance = .0
    for (element in this) {
        val diff = element - mean
        variance += diff * diff
    }
    return BasicStats(count, mean, variance)
}

@JvmName("longVarianceAndMean")
internal fun Iterable<Long>.varianceAndMean(): BasicStats {
    var count = 0
    var sum = .0
    for (element in this) {
        sum += element
        count++
    }
    val mean = sum / count
    var variance = .0
    for (element in this) {
        val diff = element - mean
        variance += diff * diff
    }
    return BasicStats(count, mean, variance)
}

@JvmName("bigDecimalVarianceAndMean")
internal fun Iterable<BigDecimal>.varianceAndMean(): BasicStats {
    var count = 0
    var sum = BigDecimal.ZERO
    for (element in this) {
        sum += element
        count++
    }
    val mean = sum.div(count.toBigDecimal())
    var variance = BigDecimal.ZERO
    for (element in this) {
        val diff = element - mean
        variance += diff * diff
    }
    return BasicStats(count, mean.toDouble(), variance.toDouble())
}

@JvmName("bigIntegerVarianceAndMean")
internal fun Iterable<BigInteger>.varianceAndMean(): BasicStats {
    var count = 0
    var sum = BigInteger.ZERO
    for (element in this) {
        sum += element
        count++
    }
    val mean = sum.toDouble() / count
    var variance = .0
    for (element in this) {
        val diff = element.toDouble() - mean
        variance += diff * diff
    }
    return BasicStats(count, mean, variance)
}
