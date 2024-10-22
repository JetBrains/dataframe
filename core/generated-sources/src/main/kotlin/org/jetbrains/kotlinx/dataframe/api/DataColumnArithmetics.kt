package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.AnyCol
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.Predicate
import org.jetbrains.kotlinx.dataframe.columns.ColumnReference
import java.math.BigDecimal

public operator fun DataColumn<Boolean>.not(): DataColumn<Boolean> = map { !it }

@JvmName("notBooleanNullable")
public operator fun DataColumn<Boolean?>.not(): DataColumn<Boolean?> = map { it?.not() }

public operator fun ColumnReference<Boolean>.not(): ColumnReference<Boolean> = map { !it }

@JvmName("notBooleanNullable")
public operator fun ColumnReference<Boolean?>.not(): ColumnReference<Boolean?> = map { it?.not() }

public operator fun DataColumn<Int>.plus(value: Int): DataColumn<Int> = map { it + value }

public operator fun DataColumn<Int>.minus(value: Int): DataColumn<Int> = map { it - value }

public operator fun Int.plus(column: DataColumn<Int>): DataColumn<Int> = column.map { this + it }

public operator fun Int.minus(column: DataColumn<Int>): DataColumn<Int> = column.map { this - it }

public operator fun DataColumn<Int>.unaryMinus(): DataColumn<Int> = map { -it }

public operator fun DataColumn<Int>.times(value: Int): DataColumn<Int> = map { it * value }

public operator fun DataColumn<Int>.div(value: Int): DataColumn<Int> = map { it / value }

public operator fun Int.div(column: DataColumn<Int>): DataColumn<Int> = column.map { this / it }

public operator fun AnyCol.plus(str: String): DataColumn<String> = map { it.toString() + str }

public operator fun ColumnReference<Int>.plus(value: Int): ColumnReference<Int> = map { it + value }

public operator fun ColumnReference<Int>.minus(value: Int): ColumnReference<Int> = map { it - value }

public operator fun Int.plus(column: ColumnReference<Int>): ColumnReference<Int> = column.map { this + it }

public operator fun Int.minus(column: ColumnReference<Int>): ColumnReference<Int> = column.map { this - it }

public operator fun ColumnReference<Int>.unaryMinus(): ColumnReference<Int> = map { -it }

public operator fun ColumnReference<Int>.times(value: Int): ColumnReference<Int> = map { it * value }

public operator fun ColumnReference<Int>.div(value: Int): ColumnReference<Int> = map { it / value }

public operator fun Int.div(column: ColumnReference<Int>): ColumnReference<Int> = column.map { this / it }

public operator fun ColumnReference<Any?>.plus(str: String): ColumnReference<String> = map { it.toString() + str }

@JvmName("plusIntNullable")
public operator fun DataColumn<Int?>.plus(value: Int): DataColumn<Int?> = map { it?.plus(value) }

@JvmName("minusIntNullable")
public operator fun DataColumn<Int?>.minus(value: Int): DataColumn<Int?> = map { it?.minus(value) }

@JvmName("plusNullable")
public operator fun Int.plus(column: DataColumn<Int?>): DataColumn<Int?> = column.map { it?.plus(this) }

@JvmName("minusNullable")
public operator fun Int.minus(column: DataColumn<Int?>): DataColumn<Int?> = column.map { it?.let { this - it } }

@JvmName("unaryMinusIntNullable")
public operator fun DataColumn<Int?>.unaryMinus(): DataColumn<Int?> = map { it?.unaryMinus() }

@JvmName("timesIntNullable")
public operator fun DataColumn<Int?>.times(value: Int): DataColumn<Int?> = map { it?.times(value) }

@JvmName("divIntNullable")
public operator fun DataColumn<Int?>.div(value: Int): DataColumn<Int?> = map { it?.div(value) }

@JvmName("divNullable")
public operator fun Int.div(column: DataColumn<Int?>): DataColumn<Int?> = column.map { it?.let { this / it } }

@JvmName("plusInt")
public operator fun DataColumn<Int>.plus(value: Double): DataColumn<Double> = map { it + value }

@JvmName("minusInt")
public operator fun DataColumn<Int>.minus(value: Double): DataColumn<Double> = map { it - value }

@JvmName("doublePlus")
public operator fun Double.plus(column: DataColumn<Int>): DataColumn<Double> = column.map { this + it }

@JvmName("doubleMinus")
public operator fun Double.minus(column: DataColumn<Int>): DataColumn<Double> = column.map { this - it }

@JvmName("timesInt")
public operator fun DataColumn<Int>.times(value: Double): DataColumn<Double> = map { it * value }

@JvmName("divInt")
public operator fun DataColumn<Int>.div(value: Double): DataColumn<Double> = map { it / value }

@JvmName("doubleDiv")
public operator fun Double.div(column: DataColumn<Int>): DataColumn<Double> = column.map { this / it }

@JvmName("plusDouble")
public operator fun DataColumn<Double>.plus(value: Int): DataColumn<Double> = map { it + value }

@JvmName("minusDouble")
public operator fun DataColumn<Double>.minus(value: Int): DataColumn<Double> = map { it - value }

@JvmName("intPlus")
public operator fun Int.plus(column: DataColumn<Double>): DataColumn<Double> = column.map { this + it }

@JvmName("intMinus")
public operator fun Int.minus(column: DataColumn<Double>): DataColumn<Double> = column.map { this - it }

@JvmName("timesDouble")
public operator fun DataColumn<Double>.times(value: Int): DataColumn<Double> = map { it * value }

@JvmName("divDouble")
public operator fun DataColumn<Double>.div(value: Int): DataColumn<Double> = map { it / value }

@JvmName("intDiv")
public operator fun Int.div(column: DataColumn<Double>): DataColumn<Double> = column.map { this / it }

public operator fun DataColumn<Double>.plus(value: Double): DataColumn<Double> = map { it + value }

public operator fun DataColumn<Double>.minus(value: Double): DataColumn<Double> = map { it - value }

public operator fun Double.plus(column: DataColumn<Double>): DataColumn<Double> = column.map { this + it }

public operator fun Double.minus(column: DataColumn<Double>): DataColumn<Double> = column.map { this - it }

@JvmName("unaryMinusDouble")
public operator fun DataColumn<Double>.unaryMinus(): DataColumn<Double> = map { -it }

public operator fun DataColumn<Double>.times(value: Double): DataColumn<Double> = map { it * value }

public operator fun DataColumn<Double>.div(value: Double): DataColumn<Double> = map { it / value }

public operator fun Double.div(column: DataColumn<Double>): DataColumn<Double> = column.map { this / it }

public operator fun DataColumn<Long>.plus(value: Long): DataColumn<Long> = map { it + value }

public operator fun DataColumn<Long>.minus(value: Long): DataColumn<Long> = map { it - value }

public operator fun Long.plus(column: DataColumn<Long>): DataColumn<Long> = column.map { this + it }

public operator fun Long.minus(column: DataColumn<Long>): DataColumn<Long> = column.map { this - it }

@JvmName("unaryMinusLong")
public operator fun DataColumn<Long>.unaryMinus(): DataColumn<Long> = map { -it }

public operator fun DataColumn<Long>.times(value: Long): DataColumn<Long> = map { it * value }

public operator fun DataColumn<Long>.div(value: Long): DataColumn<Long> = map { it / value }

public operator fun Long.div(column: DataColumn<Long>): DataColumn<Long> = column.map { this / it }

public operator fun DataColumn<BigDecimal>.plus(value: BigDecimal): DataColumn<BigDecimal> = map { it + value }

public operator fun DataColumn<BigDecimal>.minus(value: BigDecimal): DataColumn<BigDecimal> = map { it - value }

public operator fun BigDecimal.plus(column: DataColumn<BigDecimal>): DataColumn<BigDecimal> = column.map { this + it }

public operator fun BigDecimal.minus(column: DataColumn<BigDecimal>): DataColumn<BigDecimal> = column.map { this - it }

@JvmName("unaryMinusBigDecimal")
public operator fun DataColumn<BigDecimal>.unaryMinus(): DataColumn<BigDecimal> = map { -it }

public operator fun DataColumn<BigDecimal>.times(value: BigDecimal): DataColumn<BigDecimal> = map { it * value }

public operator fun DataColumn<BigDecimal>.div(value: BigDecimal): DataColumn<BigDecimal> = map { it / value }

public operator fun BigDecimal.div(column: DataColumn<BigDecimal>): DataColumn<BigDecimal> = column.map { this / it }

public infix fun <T> DataColumn<T>.eq(value: T): DataColumn<Boolean> = isMatching { it == value }

public infix fun <T> DataColumn<T>.neq(value: T): DataColumn<Boolean> = isMatching { it != value }

public infix fun <T : Comparable<T>> DataColumn<T>.gt(value: T): DataColumn<Boolean> = isMatching { it > value }

public infix fun <T : Comparable<T>> DataColumn<T>.lt(value: T): DataColumn<Boolean> = isMatching { it < value }

internal fun <T> DataColumn<T>.isMatching(predicate: Predicate<T>): DataColumn<Boolean> = map { predicate(it) }
