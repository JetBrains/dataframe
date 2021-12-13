package org.jetbrains.kotlinx.dataframe.impl

import org.jetbrains.kotlinx.dataframe.ColumnsSelector
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.Predicate
import org.jetbrains.kotlinx.dataframe.annotations.ColumnName
import org.jetbrains.kotlinx.dataframe.api.cast
import org.jetbrains.kotlinx.dataframe.columns.ColumnPath
import org.jetbrains.kotlinx.dataframe.columns.ColumnWithPath
import org.jetbrains.kotlinx.dataframe.columns.UnresolvedColumnsPolicy
import org.jetbrains.kotlinx.dataframe.impl.columns.resolve
import org.jetbrains.kotlinx.dataframe.impl.columns.toColumns
import org.jetbrains.kotlinx.dataframe.nrow
import java.math.BigDecimal
import java.math.BigInteger
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.KTypeProjection
import kotlin.reflect.full.createType
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.full.starProjectedType
import kotlin.reflect.full.withNullability
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.typeOf

internal infix fun <T> (Predicate<T>?).and(other: Predicate<T>): Predicate<T> = if (this == null) other else { it: T -> this(it) && other(it) }

internal fun <T> T.toIterable(getNext: (T) -> T?) = Iterable<T> {
    object : Iterator<T> {

        var current: T? = null
        var beforeStart = true
        var next: T? = null

        override fun hasNext(): Boolean {
            if (beforeStart) return true
            if (next == null) next = getNext(current!!)
            return next != null
        }

        override fun next(): T {
            if (beforeStart) {
                current = this@toIterable
                beforeStart = false
                return current!!
            }
            current = next ?: getNext(current!!)
            next = null
            return current!!
        }
    }
}

internal fun <T> List<T>.removeAt(index: Int) = subList(0, index) + subList(index + 1, size)

internal inline fun <reified T : Any> Int.cast() = convert(this, T::class)

internal fun <T : Any> convert(src: Int, tartypeOf: KClass<T>): T = when (tartypeOf) {
    Double::class -> src.toDouble() as T
    Long::class -> src.toLong() as T
    Float::class -> src.toFloat() as T
    BigDecimal::class -> src.toBigDecimal() as T
    else -> throw NotImplementedError("Casting int to $tartypeOf is not supported")
}

internal fun BooleanArray.toIndices(): List<Int> {
    val res = ArrayList<Int>(size)
    for (i in 0 until size)
        if (this[i]) res.add(i)
    return res
}

internal fun List<Boolean>.toIndices(): List<Int> {
    val res = ArrayList<Int>(size)
    for (i in 0 until size)
        if (this[i]) res.add(i)
    return res
}

internal fun <T> Iterable<T>.equalsByElement(other: Iterable<T>): Boolean {
    val iterator1 = iterator()
    val iterator2 = other.iterator()
    while (iterator1.hasNext() && iterator2.hasNext()) {
        if (iterator1.next() != iterator2.next()) return false
    }
    if (iterator1.hasNext() || iterator2.hasNext()) return false
    return true
}

internal fun <T> Iterable<T>.rollingHash(): Int {
    val i = iterator()
    var hash = 0
    while (i.hasNext())
        hash = 31 * hash + (i.next()?.hashCode() ?: 5)
    return hash
}

public fun <T> Iterable<T>.asList(): List<T> = when (this) {
    is List<T> -> this
    else -> this.toList()
}

@PublishedApi
internal fun <T> Iterable<T>.anyNull(): Boolean = any { it == null }

@PublishedApi
internal fun emptyPath(): ColumnPath = ColumnPath(emptyList())

@PublishedApi
internal fun <T : Number> KClass<T>.zero(): T = when (this) {
    Int::class -> 0 as T
    Byte::class -> 0.toByte() as T
    Short::class -> 0.toShort() as T
    Long::class -> 0.toLong() as T
    Double::class -> 0.toDouble() as T
    Float::class -> 0.toFloat() as T
    BigDecimal::class -> BigDecimal.ZERO as T
    BigInteger::class -> BigInteger.ZERO as T
    Number::class -> 0 as T
    else -> TODO()
}

internal fun <T> catchSilent(body: () -> T): T? = try { body() } catch (_: Throwable) { null }

internal fun Iterable<KClass<*>>.commonType(nullable: Boolean, upperBound: KType? = null) = commonParents(this).createType(nullable, upperBound)

internal fun Iterable<KType?>.commonType(): KType {
    val distinct = distinct()
    val nullable = distinct.any { it?.isMarkedNullable ?: true }
    return when {
        distinct.isEmpty() || distinct.contains(null) -> Any::class.createStarProjectedType(nullable)
        distinct.size == 1 -> distinct[0]!!
        else -> {
            val kclass = commonParent(distinct.map { it!!.jvmErasure }) ?: return typeOf<Any>()
            val projections = distinct.map { it!!.projectUpTo(kclass).replaceTypeParameters() }
            require(projections.all { it.jvmErasure == kclass })
            val arguments = kclass.typeParameters.mapIndexed { i, p ->
                val type = projections.map { it.arguments[i].type }.commonType()
                KTypeProjection.invariant(type)
            }
            kclass.createType(arguments, nullable)
        }
    }
}

internal fun <T, C> DataFrame<T>.getColumnsImpl(
    unresolvedColumnsPolicy: UnresolvedColumnsPolicy,
    selector: ColumnsSelector<T, C>
): List<DataColumn<C>> = getColumnsWithPaths(unresolvedColumnsPolicy, selector).map { it.data }

internal fun <T, C> DataFrame<T>.getColumnsWithPaths(
    unresolvedColumnsPolicy: UnresolvedColumnsPolicy,
    selector: ColumnsSelector<T, C>
): List<ColumnWithPath<C>> = selector.toColumns().resolve(this, unresolvedColumnsPolicy)

internal fun <T, C> DataFrame<T>.getColumnPaths(
    unresolvedColumnsPolicy: UnresolvedColumnsPolicy,
    selector: ColumnsSelector<T, C>
): List<ColumnPath> = getColumnsWithPaths(unresolvedColumnsPolicy, selector).map { it.path }

internal fun <C : Comparable<C>> Sequence<C?>.indexOfMin(): Int {
    val iterator = iterator()
    if (!iterator.hasNext()) return -1
    var value = iterator.next()
    var index = 0
    while (value == null) {
        if (!iterator.hasNext()) return -1
        value = iterator.next()
        index++
    }
    var min: C = value
    var minIndex = index
    if (!iterator.hasNext()) return minIndex
    do {
        val v = iterator.next()
        index++
        if (v != null && min > v) {
            min = v
            minIndex = index
        }
    } while (iterator.hasNext())
    return minIndex
}

internal fun <C : Comparable<C>> Sequence<C?>.indexOfMax(): Int {
    val iterator = iterator()
    if (!iterator.hasNext()) return -1
    var value = iterator.next()
    var index = 0
    while (value == null) {
        if (!iterator.hasNext()) return -1
        value = iterator.next()
        index++
    }
    var max: C = value
    var maxIndex = index
    if (!iterator.hasNext()) return maxIndex
    do {
        val v = iterator.next()
        index++
        if (v != null && max < v) {
            max = v
            maxIndex = index
        }
    } while (iterator.hasNext())
    return maxIndex
}

internal fun KClass<*>.createStarProjectedType(nullable: Boolean): KType =
    this.starProjectedType.let { if (nullable) it.withNullability(true) else it }

internal fun KType.isSubtypeWithNullabilityOf(type: KType) = this.isSubtypeOf(type) && (!this.isMarkedNullable || type.isMarkedNullable)

@PublishedApi
internal inline fun <reified C> headPlusArray(head: C, cols: Array<out C>): Array<C> =
    (listOf(head) + cols.toList()).toTypedArray()

@PublishedApi
internal inline fun <reified C> headPlusIterable(head: C, cols: Iterable<C>): Iterable<C> =
    (listOf(head) + cols.asIterable())

internal fun <T> DataFrame<T>.splitByIndices(
    startIndices: Sequence<Int>
): Sequence<DataFrame<T>> {
    return (startIndices + nrow).zipWithNext { start, endExclusive ->
        if (start == endExclusive) DataFrame.empty().cast()
        else get(start until endExclusive)
    }
}

internal fun <T> List<T>.splitByIndices(startIndices: Sequence<Int>): Sequence<List<T>> {
    return (startIndices + size).zipWithNext { start, endExclusive ->
        subList(start, endExclusive)
    }
}

internal fun <T> T.asNullable() = this as T?
internal fun <T> List<T>.last(count: Int) = subList(size - count, size)
internal fun <T : Comparable<T>> T.between(left: T, right: T, includeBoundaries: Boolean = true): Boolean =
    if (includeBoundaries) this in left..right
    else this > left && this < right

private const val DELIMITERS = "[_\\s]"
internal val DELIMITERS_REGEX = DELIMITERS.toRegex()
internal val DELIMITED_STRING_REGEX: Regex = ".+$DELIMITERS.+".toRegex()

internal fun String.toCamelCaseByDelimiters(delimiters: Regex): String {
    return split(delimiters).joinToCamelCaseString()
}

internal fun List<String>.joinToCamelCaseString(): String {
    return joinToString(separator = "") { it.replaceFirstChar { it.uppercaseChar() } }
        .replaceFirstChar { it.lowercaseChar() }
}

@PublishedApi
internal val <T> KProperty<T>.columnName: String get() = findAnnotation<ColumnName>()?.name ?: name
