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
import org.jetbrains.kotlinx.dataframe.impl.columns.toColumnSet
import org.jetbrains.kotlinx.dataframe.nrow
import java.math.BigDecimal
import java.math.BigInteger
import kotlin.reflect.KCallable
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.KTypeProjection
import kotlin.reflect.KVariance
import kotlin.reflect.full.createType
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.full.starProjectedType
import kotlin.reflect.full.valueParameters
import kotlin.reflect.full.withNullability
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.typeOf

internal infix fun <T> (Predicate<T>?).and(other: Predicate<T>): Predicate<T> =
    if (this == null) other else { it: T -> this(it) && other(it) }

internal fun <T> T.toIterable(getNext: (T) -> T?) =
    Iterable<T> {
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

// TODO remove in favor of column convert logic, Issue #971
internal fun <T : Any> convert(src: Int, tartypeOf: KClass<T>): T =
    when (tartypeOf) {
        Double::class -> src.toDouble() as T
        Long::class -> src.toLong() as T
        Float::class -> src.toFloat() as T
        BigDecimal::class -> src.toBigDecimal() as T
        else -> throw NotImplementedError("Casting int to $tartypeOf is not supported")
    }

internal fun BooleanArray.getTrueIndices(): List<Int> {
    val res = ArrayList<Int>(size)
    for (i in indices) {
        if (this[i]) res.add(i)
    }
    return res
}

internal fun List<Boolean>.getTrueIndices(): List<Int> {
    val res = ArrayList<Int>(size)
    for (i in indices) {
        if (this[i]) res.add(i)
    }
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
    while (i.hasNext()) {
        hash = 31 * hash + (i.next()?.hashCode() ?: 5)
    }
    return hash
}

public fun <T> Iterable<T>.asList(): List<T> =
    when (this) {
        is List<T> -> this
        else -> this.toList()
    }

@PublishedApi
internal fun <T> Iterable<T>.anyNull(): Boolean = any { it == null }

@PublishedApi
internal fun emptyPath(): ColumnPath = ColumnPath(emptyList())

@PublishedApi
internal fun <T : Number> KClass<T>.zeroOrNull(): T? =
    when (this) {
        Int::class -> 0 as T
        Byte::class -> 0.toByte() as T
        Short::class -> 0.toShort() as T
        Long::class -> 0.toLong() as T
        Double::class -> 0.toDouble() as T
        Float::class -> 0.toFloat() as T
        BigDecimal::class -> BigDecimal.ZERO as T
        BigInteger::class -> BigInteger.ZERO as T
        Number::class -> 0 as? T
        else -> null
    }

@PublishedApi
internal fun <T : Number> KClass<T>.zero(): T =
    zeroOrNull() ?: throw NotImplementedError("Zero value for $this is not supported")

internal fun <T> catchSilent(body: () -> T): T? =
    try {
        body()
    } catch (_: Throwable) {
        null
    }

internal fun Iterable<KClass<*>>.commonType(nullable: Boolean, upperBound: KType? = null) =
    commonParents(this).createType(nullable, upperBound)

/**
 * Returns the common supertype of the given types.
 *
 * @param useStar if true, `*` will be used to fill in generic type parameters instead of `Any?`
 * (for invariant/out variance) or `Nothing` (for in variance)
 *
 * @see Iterable.commonTypeListifyValues
 */
internal fun Iterable<KType?>.commonType(useStar: Boolean = true): KType {
    val distinct = distinct()
    val nullable = distinct.any { it?.isMarkedNullable ?: true }
    return when {
        distinct.isEmpty() || distinct.contains(null) -> typeOf<Any>().withNullability(nullable)

        distinct.size == 1 -> distinct.single()!!

        else -> {
            // common parent class of all KTypes
            val kClass = commonParent(distinct.map { it!!.jvmErasure })
                ?: return typeOf<Any>().withNullability(nullable)

            // all KTypes projected to the common parent class with filled-in generic type parameters (no <T>, but <UpperBound>)
            val projections = distinct
                .map { it!!.projectUpTo(kClass).replaceGenericTypeParametersWithUpperbound() }
            require(projections.all { it.jvmErasure == kClass })

            // make new type arguments for the common parent class
            val arguments = List(kClass.typeParameters.size) { i ->
                val typeParameter = kClass.typeParameters[i]
                val projectionTypes = projections
                    .map { it.arguments[i].type }
                    .filterNot { it in distinct } // avoid infinite recursion

                when {
                    projectionTypes.isEmpty() && typeParameter.variance == KVariance.IN -> {
                        if (useStar) {
                            KTypeProjection.STAR
                        } else {
                            KTypeProjection.invariant(nothingType(false))
                        }
                    }

                    else -> {
                        val commonType = projectionTypes.commonType(useStar)
                        if (commonType == typeOf<Any?>() && useStar) {
                            KTypeProjection.STAR
                        } else {
                            KTypeProjection(typeParameter.variance, commonType)
                        }
                    }
                }
            }
            kClass.createType(arguments, nullable)
        }
    }
}

internal fun <T, C> DataFrame<T>.getColumnsImpl(
    unresolvedColumnsPolicy: UnresolvedColumnsPolicy,
    selector: ColumnsSelector<T, C>,
): List<DataColumn<C>> = getColumnsWithPaths(unresolvedColumnsPolicy, selector).map { it.data }

internal fun <T, C> DataFrame<T>.getColumnsWithPaths(
    unresolvedColumnsPolicy: UnresolvedColumnsPolicy,
    selector: ColumnsSelector<T, C>,
): List<ColumnWithPath<C>> = selector.toColumnSet().resolve(this, unresolvedColumnsPolicy)

internal fun <T, C> DataFrame<T>.getColumnPaths(
    unresolvedColumnsPolicy: UnresolvedColumnsPolicy,
    selector: ColumnsSelector<T, C>,
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
    if (this == Nothing::class) {
        nothingType(nullable) // would be Void otherwise
    } else {
        this.starProjectedType.let { if (nullable) it.withNullability(true) else it }
    }

internal fun KType.isSubtypeWithNullabilityOf(type: KType) =
    this.isSubtypeOf(type) && (!this.isMarkedNullable || type.isMarkedNullable)

@PublishedApi
internal fun headPlusArray(head: Byte, cols: ByteArray): ByteArray = byteArrayOf(head) + cols

@PublishedApi
internal fun headPlusArray(head: Short, cols: ShortArray): ShortArray = shortArrayOf(head) + cols

@PublishedApi
internal fun headPlusArray(head: Int, cols: IntArray): IntArray = intArrayOf(head) + cols

@PublishedApi
internal fun headPlusArray(head: Long, cols: LongArray): LongArray = longArrayOf(head) + cols

@PublishedApi
internal fun headPlusArray(head: Float, cols: FloatArray): FloatArray = floatArrayOf(head) + cols

@PublishedApi
internal fun headPlusArray(head: Double, cols: DoubleArray): DoubleArray = doubleArrayOf(head) + cols

@PublishedApi
internal fun headPlusArray(head: Boolean, cols: BooleanArray): BooleanArray = booleanArrayOf(head) + cols

@PublishedApi
internal fun headPlusArray(head: Char, cols: CharArray): CharArray = charArrayOf(head) + cols

@PublishedApi
internal inline fun <reified C> headPlusArray(head: C, cols: Array<out C>): Array<C> =
    (listOf(head) + cols.toList()).toTypedArray()

@PublishedApi
internal inline fun <reified C> headPlusIterable(head: C, cols: Iterable<C>): Iterable<C> =
    (listOf(head) + cols.asIterable())

internal fun <T> DataFrame<T>.splitByIndices(startIndices: Sequence<Int>): Sequence<DataFrame<T>> =
    (startIndices + nrow).zipWithNext { start, endExclusive ->
        if (start == endExclusive) {
            DataFrame.empty().cast()
        } else {
            get(start until endExclusive)
        }
    }

internal fun <T> List<T>.splitByIndices(startIndices: Sequence<Int>): Sequence<List<T>> =
    (startIndices + size).zipWithNext { start, endExclusive ->
        subList(start, endExclusive)
    }

internal fun <T> T.asNullable() = this as T?

internal fun <T> List<T>.last(count: Int) = subList(size - count, size)

internal fun <T : Comparable<T>> T.between(left: T, right: T, includeBoundaries: Boolean = true): Boolean =
    if (includeBoundaries) {
        this in left..right
    } else {
        this > left && this < right
    }

// Single regex to split words by non-alphanumeric characters, camelCase, and numbers

internal val CAMEL_DEFAULT_DELIMITERS_REGEX =
    "[^a-zA-Z0-9]+|(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|(?<=\\d)(?=[a-zA-Z])|(?<=[a-zA-Z])(?=\\d)".toRegex()

/**
 * Converts a string into lowerCamelCase using [delimiters].
 *
 * - Splits words using a unified regex that handles delimiters.
 * - If the string does not contain any letters or numbers, it remains unchanged.
 * - Places underscore ("_") between numbers.
 * - The first word remains in lowercase, and subsequent words are capitalized.
 *
 * Default behavior (with [CAMEL_DEFAULT_DELIMITERS_REGEX]):
 *
 * ```
 * "hello_world" -> "helloWorld"
 * "HelloWorld" -> "helloWorld"
 * "json.parser.Config" -> "jsonParserConfig"
 * "my.var_name test" -> "myVarNameTest"
 * "thirdColumn" -> "thirdColumn"
 * "someHTMLParser" -> "someHtmlParser"
 * "RESTApi" -> "restApi"
 * "OAuth2Token" -> "oAuth2Token"
 * "GraphQLQuery" -> "graphQlQuery"
 * "TCP_3_PROTOCOL" -> "tcp3Protocol"
 * "123hello_world456" -> "123HelloWorld456"
 * "API_Response_2023" -> "apiResponse2023"
 * "UPPER_case-LOWER" -> "upperCaseLower"
 * "12parse34CamelCase" -> "12Parse34CamelCase"
 * "snake_case_example" -> "snakeCaseExample"
 * "dot.separated.words" -> "dotSeparatedWords"
 * "kebab-case-example" -> "kebabCaseExample"
 * "MIXED_Case_with_123Numbers" -> "mixedCaseWith123Numbers"
 * "___!!!___" -> "___!!!___"
 * "1000.2000.3000" -> "1000_2000_3000"
 * "UPPERCASE" -> "uppercase"
 * "alreadyCamelCased" -> "alreadyCamelCased"
 * "justNumbers123" -> "justNumbers123"
 * "Just_Special\$Chars!!" -> "justSpecialChars"
 * "singleword" -> "singleword"
 * "word_with_underscores_and-dashes" -> "wordWithUnderscoresAndDashes"
 * "10-20-aa" -> "10_20Aa"
 * ```
 *
 * @return trhe formatted string in lowerCamelCase.
 */
public fun String.toCamelCaseByDelimiters(delimiters: Regex = CAMEL_DEFAULT_DELIMITERS_REGEX): String =
    if (!this.any { it.isLetter() || it.isDigit() }) {
        this // If the string has no letters, return it unchanged
    } else {
        split(delimiters)
            .filter { it.isNotBlank() }
            .map { it.lowercase() }
            .joinNumbers("_")
            .joinToCamelCaseString()
    }

/**
 * Joins consecutive numbers in a list with the given [separator].
 */
private fun List<String>.joinNumbers(separator: CharSequence): List<String> {
    val result = mutableListOf<String>()
    var i = 0

    while (i < this.size) {
        val current = this[i]
        if (current.all { it.isDigit() }) { // Check if the current element is a number
            val numberGroup = mutableListOf(current)
            while (i + 1 < this.size && this[i + 1].all { it.isDigit() }) {
                numberGroup.add(this[i + 1])
                i++
            }
            result.add(numberGroup.joinToString(separator)) // Join consecutive numbers with "_"
        } else {
            result.add(current)
        }
        i++
    }
    return result
}

/**
 * Joins a list of words into lowerCamelCase format.
 * - The first word is converted to lowercase.
 * - Subsequent words start with an uppercase letter.
 */
private fun List<String>.joinToCamelCaseString(): String =
    mapIndexed { index, word ->
        if (index == 0) word.lowercase() else word.replaceFirstChar { it.uppercaseChar() }
    }.joinToString("")

internal val CAMEL_LETTERS_REGEX = "(?<=[a-zA-Z])[A-Z]".toRegex()

internal fun String.toSnakeCase(): String =
    if ("[A-Z_]+".toRegex().matches(this)) {
        this
    } else {
        CAMEL_LETTERS_REGEX
            .replace(this) { "_${it.value}" }
            .replace(" ", "_")
            .lowercase()
    }

/** @include [KCallable.isGetterLike] */
internal fun KFunction<*>.isGetterLike(): Boolean =
    (name.startsWith("get") || name.startsWith("is")) &&
        valueParameters.isEmpty() &&
        typeParameters.isEmpty()

/** @include [KCallable.isGetterLike] */
internal fun KProperty<*>.isGetterLike(): Boolean = true

/**
 * Returns `true` if this callable is a getter-like function.
 *
 * A callable is considered getter-like if it is either a property getter,
 * or it's a function with no (type) parameters that starts with "get"/"is".
 */
internal fun KCallable<*>.isGetterLike(): Boolean =
    when (this) {
        is KProperty<*> -> isGetterLike()
        is KFunction<*> -> isGetterLike()
        else -> false
    }

/** @include [KCallable.getterName] */
internal val KFunction<*>.getterName: String
    get() = name
        .removePrefix("get")
        .removePrefix("is")
        .replaceFirstChar { it.lowercase() }

/** @include [KCallable.getterName] */
internal val KProperty<*>.getterName: String
    get() = name

/**
 * Returns the getter name for this callable.
 * The name of the callable is returned with proper getter-trimming if it's a [KFunction].
 */
internal val KCallable<*>.getterName: String
    get() = when (this) {
        is KFunction<*> -> getterName
        is KProperty<*> -> getterName
        else -> name
    }

/** @include [KCallable.columnName] */
@PublishedApi
internal val KFunction<*>.columnName: String
    get() = findAnnotation<ColumnName>()?.name ?: getterName

/** @include [KCallable.columnName] */
@PublishedApi
internal val KProperty<*>.columnName: String
    get() = findAnnotation<ColumnName>()?.name ?: getterName

/**
 * Returns the column name for this callable.
 * If the callable contains the [ColumnName] annotation, its [ColumnName.name] is returned.
 * Otherwise, the name of the callable is returned with proper getter-trimming if it's a [KFunction].
 */
@PublishedApi
internal val KCallable<*>.columnName: String
    get() = when (this) {
        is KFunction<*> -> columnName
        is KProperty<*> -> columnName
        else -> findAnnotation<ColumnName>()?.name ?: getterName
    }
