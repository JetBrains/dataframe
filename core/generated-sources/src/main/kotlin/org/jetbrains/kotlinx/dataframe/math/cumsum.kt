package org.jetbrains.kotlinx.dataframe.math

import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.api.isNA
import org.jetbrains.kotlinx.dataframe.api.map
import java.math.BigDecimal

internal val defaultCumSumSkipNA: Boolean = true

@JvmName("doubleCumsum")
internal fun DataColumn<Double>.cumSum(skipNA: Boolean = defaultCumSumSkipNA): DataColumn<Double> {
    var sum = .0
    return map {
        if (skipNA && it.isNaN()) {
            it
        } else {
            sum += it
            sum
        }
    }
}

@JvmName("cumsumDoubleNullable")
internal fun DataColumn<Double?>.cumSum(skipNA: Boolean = defaultCumSumSkipNA): DataColumn<Double> {
    var sum = .0
    return map {
        if (skipNA && it.isNA) {
            Double.NaN
        } else {
            sum += it ?: Double.NaN
            sum
        }
    }
}

@JvmName("floatCumsum")
internal fun DataColumn<Float>.cumSum(skipNA: Boolean = defaultCumSumSkipNA): DataColumn<Float> {
    var sum = .0f
    return map {
        if (skipNA && it.isNaN()) {
            it
        } else {
            sum += it
            sum
        }
    }
}

internal fun DataColumn<Float?>.cumSum(skipNA: Boolean = defaultCumSumSkipNA): DataColumn<Float> {
    var sum = .0f
    return map {
        if (skipNA && it.isNA) {
            Float.NaN
        } else {
            sum += it ?: Float.NaN
            sum
        }
    }
}

@JvmName("intCumsum")
internal fun DataColumn<Int>.cumSum(): DataColumn<Int> {
    var sum = 0
    return map {
        sum += it
        sum
    }
}

@JvmName("intCumsum")
internal fun DataColumn<Int?>.cumSum(skipNA: Boolean = defaultCumSumSkipNA): DataColumn<Int?> {
    var sum = 0
    var fillNull = false
    return map {
        when {
            it == null -> {
                if (!skipNA) fillNull = true
                null
            }

            fillNull -> null

            else -> {
                sum += it
                sum
            }
        }
    }
}

@JvmName("longCumsum")
internal fun DataColumn<Long>.cumSum(): DataColumn<Long> {
    var sum = 0L
    return map {
        sum += it
        sum
    }
}

@JvmName("cumsumLongNullable")
internal fun DataColumn<Long?>.cumSum(skipNA: Boolean = defaultCumSumSkipNA): DataColumn<Long?> {
    var sum = 0L
    var fillNull = false
    return map {
        when {
            it == null -> {
                if (!skipNA) fillNull = true
                null
            }

            fillNull -> null

            else -> {
                sum += it
                sum
            }
        }
    }
}

@JvmName("bigDecimalCumsum")
internal fun DataColumn<BigDecimal>.cumSum(): DataColumn<BigDecimal> {
    var sum = BigDecimal.ZERO
    return map {
        sum += it
        sum
    }
}

@JvmName("cumsumBigDecimalNullable")
internal fun DataColumn<BigDecimal?>.cumSum(skipNA: Boolean = defaultCumSumSkipNA): DataColumn<BigDecimal?> {
    var sum = BigDecimal.ZERO
    var fillNull = false
    return map {
        when {
            it == null -> {
                if (!skipNA) fillNull = true
                null
            }

            fillNull -> null

            else -> {
                sum += it
                sum
            }
        }
    }
}
