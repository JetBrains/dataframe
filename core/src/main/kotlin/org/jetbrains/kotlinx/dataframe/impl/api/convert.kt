package org.jetbrains.kotlinx.dataframe.impl.api

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.atStartOfDayIn
import kotlinx.datetime.atTime
import kotlinx.datetime.toInstant
import kotlinx.datetime.toJavaInstant
import kotlinx.datetime.toJavaLocalDate
import kotlinx.datetime.toJavaLocalDateTime
import kotlinx.datetime.toJavaLocalTime
import kotlinx.datetime.toKotlinInstant
import kotlinx.datetime.toKotlinLocalDate
import kotlinx.datetime.toKotlinLocalDateTime
import kotlinx.datetime.toKotlinLocalTime
import kotlinx.datetime.toLocalDateTime
import org.jetbrains.kotlinx.dataframe.AnyCol
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.RowColumnExpression
import org.jetbrains.kotlinx.dataframe.RowValueExpression
import org.jetbrains.kotlinx.dataframe.api.Convert
import org.jetbrains.kotlinx.dataframe.api.DataSchemaEnum
import org.jetbrains.kotlinx.dataframe.api.Infer
import org.jetbrains.kotlinx.dataframe.api.ParserOptions
import org.jetbrains.kotlinx.dataframe.api.mapIndexed
import org.jetbrains.kotlinx.dataframe.api.name
import org.jetbrains.kotlinx.dataframe.api.to
import org.jetbrains.kotlinx.dataframe.columns.values
import org.jetbrains.kotlinx.dataframe.dataTypes.IFRAME
import org.jetbrains.kotlinx.dataframe.dataTypes.IMG
import org.jetbrains.kotlinx.dataframe.exceptions.CellConversionException
import org.jetbrains.kotlinx.dataframe.exceptions.ColumnTypeMismatchesColumnValuesException
import org.jetbrains.kotlinx.dataframe.exceptions.TypeConversionException
import org.jetbrains.kotlinx.dataframe.exceptions.TypeConverterNotFoundException
import org.jetbrains.kotlinx.dataframe.impl.columns.DataColumnInternal
import org.jetbrains.kotlinx.dataframe.impl.columns.newColumn
import org.jetbrains.kotlinx.dataframe.impl.createStarProjectedType
import org.jetbrains.kotlinx.dataframe.path
import org.jetbrains.kotlinx.dataframe.type
import java.math.BigDecimal
import java.math.BigInteger
import java.net.URL
import java.util.Locale
import kotlin.math.roundToInt
import kotlin.math.roundToLong
import kotlin.reflect.KType
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.full.withNullability
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.typeOf
import kotlin.text.trim
import kotlin.toBigDecimal
import java.time.Instant as JavaInstant
import java.time.LocalDate as JavaLocalDate
import java.time.LocalDateTime as JavaLocalDateTime
import java.time.LocalTime as JavaLocalTime
import kotlin.toBigDecimal as toBigDecimalKotlin
import kotlin.toBigInteger as toBigIntegerKotlin

@PublishedApi
internal fun <T, C, R> Convert<T, C>.withRowCellImpl(
    type: KType,
    infer: Infer,
    rowConverter: RowValueExpression<T, C, R>,
): DataFrame<T> =
    to { col ->
        try {
            df.newColumn(type, col.name, infer) { rowConverter(it, it[col]) }
        } catch (e: ClassCastException) {
            throw ColumnTypeMismatchesColumnValuesException(col, e)
        } catch (e: NullPointerException) {
            throw ColumnTypeMismatchesColumnValuesException(col, e)
        }
    }

@PublishedApi
internal fun <T, C, R> Convert<T, C>.convertRowColumnImpl(
    type: KType,
    infer: Infer,
    rowConverter: RowColumnExpression<T, C, R>,
): DataFrame<T> = to { col -> df.newColumn(type, col.name, infer) { rowConverter(it, col) } }

/**
 * Specific implementation for [convertToTypeImpl] for [String] -> [Double] conversion
 *
 * This function exists because [convertToTypeImpl] can only retrieve a single parser
 * double has two: one with the given locale (or system default) and one POSIX parser
 */
internal fun DataColumn<String?>.convertToDoubleImpl(
    locale: Locale?,
    nullStrings: Set<String>?,
    useFastDoubleParser: Boolean?,
): DataColumn<Double?> {
    val nullStrings = nullStrings ?: Parsers.nulls
    val useFastDoubleParser = useFastDoubleParser ?: Parsers.useFastDoubleParser

    fun applyParser(parser: (String) -> Double?): DataColumn<Double?> {
        var currentRow = 0
        try {
            return mapIndexed { row, value ->
                currentRow = row
                value?.let {
                    if (it in nullStrings) return@let null

                    parser(value.trim()) ?: throw TypeConversionException(
                        value = value,
                        from = typeOf<String>(),
                        to = typeOf<Double>(),
                        column = path,
                    )
                }
            }
        } catch (e: TypeConversionException) {
            throw CellConversionException(e.value, e.from, e.to, path, currentRow, e)
        }
    }

    return if (locale != null) {
        val explicitParser = Parsers.getDoubleParser(
            locale = locale,
            useFastDoubleParser = useFastDoubleParser,
        )
        applyParser(explicitParser)
    } else {
        try {
            val defaultParser =
                Parsers.getDoubleParser(
                    locale = null,
                    useFastDoubleParser = useFastDoubleParser,
                )
            applyParser(defaultParser)
        } catch (_: TypeConversionException) {
            val posixParser = Parsers.getPosixDoubleParser(
                useFastDoubleParser = useFastDoubleParser,
            )
            applyParser(posixParser)
        }
    }
}

internal fun AnyCol.convertToTypeImpl(to: KType, parserOptions: ParserOptions?): AnyCol {
    val from = type

    val nullsAreAllowed = to.isMarkedNullable

    var nullsFound = false

    fun Any?.checkNulls() =
        when {
            this != null -> this

            nullsAreAllowed -> {
                nullsFound = true
                null
            }

            else -> throw TypeConversionException(null, from, to, path)
        }

    fun applyConverter(converter: TypeConverter): AnyCol {
        var currentRow = 0
        try {
            val values = values.mapIndexed { row, value ->
                currentRow = row
                value?.let { converter(value) }.checkNulls()
            }
            return DataColumn.createValueColumn(name, values, to.withNullability(nullsFound))
        } catch (e: TypeConversionException) {
            throw CellConversionException(e.value, e.from, e.to, path, currentRow, e)
        }
    }

    fun convertPerCell(): AnyCol {
        var currentRow = 0
        try {
            return when (from.classifier) {
                Any::class, Comparable::class, Number::class, java.io.Serializable::class -> {
                    // find converter for every value
                    val values = values.mapIndexed { row, value ->
                        currentRow = row
                        value?.let {
                            val clazz = it.javaClass.kotlin
                            val type = clazz.createStarProjectedType(false)
                            val converter = getConverter(type, to, parserOptions)
                                ?: throw TypeConverterNotFoundException(from, to, path)
                            converter(it)
                        }.checkNulls()
                    }
                    DataColumn.createValueColumn(name, values, to.withNullability(nullsFound))
                }

                else -> throw TypeConverterNotFoundException(from, to, path)
            }
        } catch (e: TypeConversionException) {
            throw CellConversionException(e.value, e.from, e.to, path, currentRow, e)
        }
    }

    if (from == to) return this

    // catch for ColumnGroup and FrameColumn since they don't have changeType,
    // but user converters can still exist
    if (from.isSubtypeOf(to)) {
        try {
            return (this as DataColumnInternal<*>).changeType(to.withNullability(hasNulls()))
        } catch (_: UnsupportedOperationException) {
            //
        }
    }

    return when (val converter = getConverter(from, to, parserOptions)) {
        null -> convertPerCell()
        else -> applyConverter(converter)
    }
}

internal val convertersCache = mutableMapOf<Triple<KType, KType, ParserOptions?>, TypeConverter?>()

internal fun getConverter(from: KType, to: KType, options: ParserOptions? = null): TypeConverter? =
    convertersCache.getOrPut(Triple(from, to, options)) { createConverter(from, to, options) }

internal typealias TypeConverter = (Any) -> Any?

internal fun Any.convertTo(type: KType): Any? {
    val clazz = javaClass.kotlin
    if (clazz.isSubclassOf(type.jvmErasure)) return this
    val from = clazz.createStarProjectedType(false)
    val converter = getConverter(from, type) ?: throw TypeConverterNotFoundException(from, type, null)
    return converter(this)
}

internal inline fun <T> convert(crossinline converter: (T) -> Any?): TypeConverter = { converter(it as T) }

private enum class DummyEnum

internal fun createConverter(from: KType, to: KType, options: ParserOptions? = null): TypeConverter? {
    if (from.arguments.isNotEmpty() || to.arguments.isNotEmpty()) return null
    if (from.isMarkedNullable) {
        val res = createConverter(from.withNullability(false), to, options) ?: return null
        return { res(it) }
    }
    val fromClass = from.jvmErasure
    val toClass = to.jvmErasure

    if (fromClass == toClass) return { it }

    if (toClass.isValue) {
        val constructor =
            toClass.primaryConstructor ?: error("Value type $toClass doesn't have primary constructor")
        val underlyingType = constructor.parameters.single().type
        val converter = getConverter(from, underlyingType)
            ?: throw TypeConverterNotFoundException(from, underlyingType, null)
        return convert<Any> {
            val converted = converter(it)
            if (converted == null && !underlyingType.isMarkedNullable) {
                throw TypeConversionException(it, from, underlyingType, null)
            }
            constructor.call(converted)
        }
    }

    return when {
        fromClass == String::class -> {
            val parser = Parsers[to.withNullability(false)]
            when {
                parser != null -> parser.toConverter(options)

                // convert enums by name (or by `value` if they implement DataSchemaEnum)
                toClass.isSubclassOf(Enum::class) -> convert<String> { string ->
                    if (toClass.isSubclassOf(DataSchemaEnum::class)) {
                        val enumValues = toClass.java.enumConstants as Array<out DataSchemaEnum>
                        enumValues.firstOrNull { it.value == string }
                            ?: java.lang.Enum.valueOf(toClass.java as Class<DummyEnum>, string)
                    } else {
                        java.lang.Enum.valueOf(toClass.java as Class<DummyEnum>, string)
                    }
                }

                else -> null
            }
        }

        toClass == String::class -> convert<Any> {
            when {
                // convert enums to String by `value` if they implement DataSchemaEnum
                fromClass.isSubclassOf(DataSchemaEnum::class) ->
                    (it as? DataSchemaEnum?)?.value ?: it.toString()

                else -> it.toString()
            }
        }

        fromClass.isValue -> {
            val constructor =
                fromClass.primaryConstructor ?: error("Value type $fromClass doesn't have primary constructor")
            val constructorParameter = constructor.parameters.single()
            val underlyingType = constructorParameter.type
            val converter = getConverter(underlyingType, to)
                ?: throw TypeConverterNotFoundException(underlyingType, to, null)
            val property =
                fromClass.memberProperties
                    .single { it.name == constructorParameter.name } as kotlin.reflect.KProperty1<Any, *>
            if (property.visibility != kotlin.reflect.KVisibility.PUBLIC) {
                throw TypeConversionException(
                    "Not public member property in primary constructor of value type",
                    from,
                    to,
                    null,
                )
            }

            convert<Any> {
                property.get(it)?.let {
                    converter(it)
                }
            }
        }

        else -> when (fromClass) {
            Boolean::class -> when (toClass) {
                Float::class -> convert<Boolean> { if (it) 1.0f else 0.0f }

                Double::class -> convert<Boolean> { if (it) 1.0 else 0.0 }

                Int::class -> convert<Boolean> { if (it) 1 else 0 }

                Long::class -> convert<Boolean> { if (it) 1L else 0L }

                Short::class -> convert<Boolean> {
                    val one: Short = 1
                    val zero: Short = 0
                    if (it) one else zero
                }

                Byte::class -> convert<Boolean> {
                    val one: Byte = 1
                    val zero: Byte = 0
                    if (it) one else zero
                }

                BigDecimal::class -> convert<Boolean> { if (it) BigDecimal.ONE else BigDecimal.ZERO }

                BigInteger::class -> convert<Boolean> { if (it) BigInteger.ONE else BigInteger.ZERO }

                else -> null
            }

            Number::class -> when (toClass) {
                Double::class -> convert<Number> { it.toDouble() }
                Int::class -> convert<Number> { it.toInt() }
                Float::class -> convert<Number> { it.toFloat() }
                Byte::class -> convert<Number> { it.toByte() }
                Short::class -> convert<Number> { it.toShort() }
                Long::class -> convert<Number> { it.toLong() }
                Boolean::class -> convert<Number> { it.toDouble() != 0.0 }
                BigDecimal::class -> convert<Number> { it.toBigDecimal() }
                BigInteger::class -> convert<Number> { it.toBigInteger() }
                else -> null
            }

            Char::class -> when (toClass) {
                Int::class -> convert<Char> { it.code }

                else -> // convert char to string and then to target type
                    getConverter(typeOf<String>(), to, options)?.let { stringConverter ->
                        convert<Char> {
                            stringConverter(it.toString())
                        }
                    }
            }

            Int::class -> when (toClass) {
                Char::class -> convert<Int> { it.toChar() }

                Double::class -> convert<Int> { it.toDouble() }

                Float::class -> convert<Int> { it.toFloat() }

                Byte::class -> convert<Int> { it.toByte() }

                Short::class -> convert<Int> { it.toShort() }

                Long::class -> convert<Int> { it.toLong() }

                BigDecimal::class -> convert<Int> { it.toBigDecimal() }

                BigInteger::class -> convert<Int> { it.toBigInteger() }

                Boolean::class -> convert<Int> { it != 0 }

                LocalDateTime::class -> convert<Int> { it.toLong().toLocalDateTime(defaultTimeZone) }

                LocalDate::class -> convert<Int> { it.toLong().toLocalDate(defaultTimeZone) }

                LocalTime::class -> convert<Int> { it.toLong().toLocalTime(defaultTimeZone) }

                Instant::class -> convert<Int> { Instant.fromEpochMilliseconds(it.toLong()) }

                JavaLocalDateTime::class -> convert<Int> {
                    it.toLong().toLocalDateTime(defaultTimeZone).toJavaLocalDateTime()
                }

                JavaLocalDate::class -> convert<Int> { it.toLong().toLocalDate(defaultTimeZone).toJavaLocalDate() }

                JavaLocalTime::class -> convert<Int> { it.toLong().toLocalTime(defaultTimeZone).toJavaLocalTime() }

                JavaInstant::class -> convert<Int> { JavaInstant.ofEpochMilli(it.toLong()) }

                else -> null
            }

            Byte::class -> when (toClass) {
                Double::class -> convert<Byte> { it.toDouble() }

                Float::class -> convert<Byte> { it.toFloat() }

                Int::class -> convert<Byte> { it.toInt() }

                Short::class -> convert<Byte> { it.toShort() }

                Long::class -> convert<Byte> { it.toLong() }

                BigDecimal::class -> convert<Byte> { it.toBigDecimal() }

                BigInteger::class -> convert<Byte> { it.toBigInteger() }

                Boolean::class -> convert<Byte> { it != 0.toByte() }

                LocalDateTime::class -> convert<Byte> { it.toLong().toLocalDateTime(defaultTimeZone) }

                LocalDate::class -> convert<Byte> { it.toLong().toLocalDate(defaultTimeZone) }

                LocalTime::class -> convert<Byte> { it.toLong().toLocalTime(defaultTimeZone) }

                Instant::class -> convert<Byte> { Instant.fromEpochMilliseconds(it.toLong()) }

                JavaLocalDateTime::class -> convert<Byte> {
                    it.toLong().toLocalDateTime(defaultTimeZone).toJavaLocalDateTime()
                }

                JavaLocalDate::class -> convert<Byte> { it.toLong().toLocalDate(defaultTimeZone).toJavaLocalDate() }

                JavaLocalTime::class -> convert<Byte> { it.toLong().toLocalTime(defaultTimeZone).toJavaLocalTime() }

                JavaInstant::class -> convert<Byte> { JavaInstant.ofEpochMilli(it.toLong()) }

                else -> null
            }

            Short::class -> when (toClass) {
                Double::class -> convert<Short> { it.toDouble() }

                Float::class -> convert<Short> { it.toFloat() }

                Int::class -> convert<Short> { it.toInt() }

                Byte::class -> convert<Short> { it.toByte() }

                Long::class -> convert<Short> { it.toLong() }

                BigDecimal::class -> convert<Short> { it.toBigDecimal() }

                BigInteger::class -> convert<Short> { it.toBigInteger() }

                Boolean::class -> convert<Short> { it != 0.toShort() }

                LocalDateTime::class -> convert<Short> { it.toLong().toLocalDateTime(defaultTimeZone) }

                LocalDate::class -> convert<Short> { it.toLong().toLocalDate(defaultTimeZone) }

                LocalTime::class -> convert<Short> { it.toLong().toLocalTime(defaultTimeZone) }

                Instant::class -> convert<Short> { Instant.fromEpochMilliseconds(it.toLong()) }

                JavaLocalDateTime::class -> convert<Short> {
                    it.toLong().toLocalDateTime(defaultTimeZone).toJavaLocalDateTime()
                }

                JavaLocalDate::class -> convert<Short> { it.toLong().toLocalDate(defaultTimeZone).toJavaLocalDate() }

                JavaLocalTime::class -> convert<Short> { it.toLong().toLocalTime(defaultTimeZone).toJavaLocalTime() }

                JavaInstant::class -> convert<Short> { JavaInstant.ofEpochMilli(it.toLong()) }

                else -> null
            }

            Double::class -> when (toClass) {
                Int::class -> convert<Double> { it.roundToInt() }
                Float::class -> convert<Double> { it.toFloat() }
                Long::class -> convert<Double> { it.roundToLong() }
                Short::class -> convert<Double> { it.roundToInt().toShort() }
                Byte::class -> convert<Double> { it.roundToInt().toByte() }
                BigDecimal::class -> convert<Double> { it.toBigDecimal() }
                BigInteger::class -> convert<Double> { it.toBigInteger() }
                Boolean::class -> convert<Double> { it != 0.0 }
                else -> null
            }

            Long::class -> when (toClass) {
                Double::class -> convert<Long> { it.toDouble() }

                Float::class -> convert<Long> { it.toFloat() }

                Byte::class -> convert<Long> { it.toByte() }

                Short::class -> convert<Long> { it.toShort() }

                Int::class -> convert<Long> { it.toInt() }

                BigDecimal::class -> convert<Long> { it.toBigDecimal() }

                BigInteger::class -> convert<Long> { it.toBigInteger() }

                Boolean::class -> convert<Long> { it != 0L }

                LocalDateTime::class -> convert<Long> { it.toLocalDateTime(defaultTimeZone) }

                LocalDate::class -> convert<Long> { it.toLocalDate(defaultTimeZone) }

                LocalTime::class -> convert<Long> { it.toLocalTime(defaultTimeZone) }

                Instant::class -> convert<Long> { Instant.fromEpochMilliseconds(it) }

                JavaLocalDateTime::class -> convert<Long> {
                    it.toLocalDateTime(defaultTimeZone).toJavaLocalDateTime()
                }

                JavaLocalDate::class -> convert<Long> { it.toLocalDate(defaultTimeZone).toJavaLocalDate() }

                JavaLocalTime::class -> convert<Long> { it.toLocalTime(defaultTimeZone).toJavaLocalTime() }

                JavaInstant::class -> convert<Long> { JavaInstant.ofEpochMilli(it) }

                else -> null
            }

            Instant::class -> when (toClass) {
                Long::class -> convert<Instant> { it.toEpochMilliseconds() }

                LocalDateTime::class -> convert<Instant> { it.toLocalDateTime(defaultTimeZone) }

                LocalDate::class -> convert<Instant> { it.toLocalDate(defaultTimeZone) }

                LocalTime::class -> convert<Instant> { it.toLocalTime(defaultTimeZone) }

                JavaLocalDateTime::class -> convert<Instant> {
                    it.toLocalDateTime(defaultTimeZone).toJavaLocalDateTime()
                }

                JavaLocalDate::class -> convert<Instant> { it.toLocalDate(defaultTimeZone).toJavaLocalDate() }

                JavaInstant::class -> convert<Instant> { it.toJavaInstant() }

                JavaLocalTime::class -> convert<Instant> { it.toLocalTime(defaultTimeZone).toJavaLocalTime() }

                else -> null
            }

            JavaInstant::class -> when (toClass) {
                Long::class -> convert<JavaInstant> { it.toEpochMilli() }

                LocalDateTime::class -> convert<JavaInstant> {
                    it.toKotlinInstant().toLocalDateTime(defaultTimeZone)
                }

                LocalDate::class -> convert<JavaInstant> { it.toKotlinInstant().toLocalDate(defaultTimeZone) }

                LocalTime::class -> convert<JavaInstant> { it.toKotlinInstant().toLocalTime(defaultTimeZone) }

                Instant::class -> convert<JavaInstant> { it.toKotlinInstant() }

                JavaLocalDateTime::class -> convert<JavaInstant> {
                    it.toKotlinInstant().toLocalDateTime(defaultTimeZone).toJavaLocalDateTime()
                }

                JavaLocalDate::class -> convert<JavaInstant> {
                    it.toKotlinInstant().toLocalDate(defaultTimeZone).toJavaLocalDate()
                }

                JavaLocalTime::class -> convert<JavaInstant> {
                    it.toKotlinInstant().toLocalTime(defaultTimeZone).toJavaLocalTime()
                }

                else -> null
            }

            Float::class -> when (toClass) {
                Double::class -> convert<Float> { it.toDouble() }
                Long::class -> convert<Float> { it.roundToLong() }
                Int::class -> convert<Float> { it.roundToInt() }
                Byte::class -> convert<Float> { it.roundToInt().toByte() }
                Short::class -> convert<Float> { it.roundToInt().toShort() }
                BigDecimal::class -> convert<Float> { it.toBigDecimal() }
                BigInteger::class -> convert<Float> { it.toBigInteger() }
                Boolean::class -> convert<Float> { it != 0.0F }
                else -> null
            }

            BigDecimal::class -> when (toClass) {
                Double::class -> convert<BigDecimal> { it.toDouble() }
                Int::class -> convert<BigDecimal> { it.toInt() }
                Byte::class -> convert<BigDecimal> { it.toByte() }
                Short::class -> convert<BigDecimal> { it.toShort() }
                Float::class -> convert<BigDecimal> { it.toFloat() }
                Long::class -> convert<BigDecimal> { it.toLong() }
                BigInteger::class -> convert<BigDecimal> { it.toBigInteger() }
                Boolean::class -> convert<BigDecimal> { it != BigDecimal.ZERO }
                else -> null
            }

            BigInteger::class -> when (toClass) {
                Double::class -> convert<BigInteger> { it.toDouble() }
                Int::class -> convert<BigInteger> { it.toInt() }
                Byte::class -> convert<BigInteger> { it.toByte() }
                Short::class -> convert<BigInteger> { it.toShort() }
                Float::class -> convert<BigInteger> { it.toFloat() }
                Long::class -> convert<BigInteger> { it.toLong() }
                BigDecimal::class -> convert<BigInteger> { it.toBigDecimal() }
                Boolean::class -> convert<BigInteger> { it != BigInteger.ZERO }
                else -> null
            }

            LocalDateTime::class -> when (toClass) {
                LocalDate::class -> convert<LocalDateTime> { it.date }
                LocalTime::class -> convert<LocalDateTime> { it.time }
                Instant::class -> convert<LocalDateTime> { it.toInstant(defaultTimeZone) }
                Long::class -> convert<LocalDateTime> { it.toInstant(defaultTimeZone).toEpochMilliseconds() }
                JavaLocalDateTime::class -> convert<LocalDateTime> { it.toJavaLocalDateTime() }
                JavaLocalDate::class -> convert<LocalDateTime> { it.date.toJavaLocalDate() }
                JavaLocalTime::class -> convert<LocalDateTime> { it.toJavaLocalDateTime().toLocalTime() }
                JavaInstant::class -> convert<LocalDateTime> { it.toInstant(defaultTimeZone).toJavaInstant() }
                else -> null
            }

            JavaLocalDateTime::class -> when (toClass) {
                LocalDate::class -> convert<JavaLocalDateTime> { it.toKotlinLocalDateTime().date }

                LocalTime::class -> convert<JavaLocalDateTime> { it.toKotlinLocalDateTime().time }

                LocalDateTime::class -> convert<JavaLocalDateTime> { it.toKotlinLocalDateTime() }

                Instant::class -> convert<JavaLocalDateTime> {
                    it.toKotlinLocalDateTime().toInstant(defaultTimeZone)
                }

                Long::class -> convert<JavaLocalDateTime> {
                    it.toKotlinLocalDateTime().toInstant(defaultTimeZone).toEpochMilliseconds()
                }

                JavaLocalDate::class -> convert<JavaLocalDateTime> { it.toLocalDate() }

                JavaLocalTime::class -> convert<JavaLocalDateTime> { it.toLocalTime() }

                JavaInstant::class -> convert<JavaLocalDateTime> {
                    it.toKotlinLocalDateTime().toInstant(defaultTimeZone).toJavaInstant()
                }

                else -> null
            }

            LocalDate::class -> when (toClass) {
                LocalDateTime::class -> convert<LocalDate> { it.atTime(0, 0) }
                Instant::class -> convert<LocalDate> { it.atStartOfDayIn(defaultTimeZone) }
                Long::class -> convert<LocalDate> { it.atStartOfDayIn(defaultTimeZone).toEpochMilliseconds() }
                JavaLocalDate::class -> convert<LocalDate> { it.toJavaLocalDate() }
                JavaLocalDateTime::class -> convert<LocalDate> { it.atTime(0, 0).toJavaLocalDateTime() }
                JavaInstant::class -> convert<LocalDate> { it.atStartOfDayIn(defaultTimeZone).toJavaInstant() }
                else -> null
            }

            JavaLocalDate::class -> when (toClass) {
                LocalDate::class -> convert<JavaLocalDate> { it.toKotlinLocalDate() }

                LocalDateTime::class -> convert<JavaLocalDate> { it.atTime(0, 0).toKotlinLocalDateTime() }

                Instant::class -> convert<JavaLocalDate> {
                    it.toKotlinLocalDate().atStartOfDayIn(defaultTimeZone)
                }

                Long::class -> convert<JavaLocalDate> {
                    it.toKotlinLocalDate().atStartOfDayIn(defaultTimeZone).toEpochMilliseconds()
                }

                JavaLocalDateTime::class -> convert<JavaLocalDate> { it.atStartOfDay() }

                JavaInstant::class -> convert<JavaLocalDate> {
                    it.toKotlinLocalDate().atStartOfDayIn(defaultTimeZone).toJavaInstant()
                }

                else -> null
            }

            LocalTime::class -> when (toClass) {
                JavaLocalTime::class -> convert<LocalTime> { it.toJavaLocalTime() }
                else -> null
            }

            JavaLocalTime::class -> when (toClass) {
                LocalTime::class -> convert<JavaLocalTime> { it.toKotlinLocalTime() }
                else -> null
            }

            URL::class -> when (toClass) {
                IMG::class -> convert<URL> { IMG(it.toString()) }
                IFRAME::class -> convert<URL> { IFRAME(it.toString()) }
                else -> null
            }

            else -> null
        }
    }
}

internal fun Long.toLocalDateTime(zone: TimeZone = defaultTimeZone) =
    Instant.fromEpochMilliseconds(this).toLocalDateTime(zone)

internal fun Long.toLocalDate(zone: TimeZone = defaultTimeZone) = toLocalDateTime(zone).date

internal fun Long.toLocalTime(zone: TimeZone = defaultTimeZone) = toLocalDateTime(zone).time

internal fun Instant.toLocalDate(zone: TimeZone = defaultTimeZone) = toLocalDateTime(zone).date

internal fun Instant.toLocalTime(zone: TimeZone = defaultTimeZone) = toLocalDateTime(zone).time

internal val defaultTimeZone = TimeZone.currentSystemDefault()

internal fun Number.toBigDecimal(): BigDecimal =
    when (this) {
        is BigDecimal -> this
        is BigInteger -> this.toBigDecimalKotlin()
        is Int -> this.toBigDecimalKotlin()
        is Byte -> this.toInt().toBigDecimalKotlin()
        is Short -> this.toInt().toBigDecimalKotlin()
        is Long -> this.toBigDecimalKotlin()
        is Float -> this.toBigDecimalKotlin()
        is Double -> this.toBigDecimalKotlin()
        else -> BigDecimal(this.toString())
    }

internal fun Number.toBigInteger(): BigInteger =
    when (this) {
        is BigInteger -> this
        is BigDecimal -> this.toBigInteger()
        is Int -> this.toBigIntegerKotlin()
        is Byte -> this.toInt().toBigIntegerKotlin()
        is Short -> this.toInt().toBigIntegerKotlin()
        is Long -> this.toBigIntegerKotlin()
        is Float -> this.roundToInt().toBigIntegerKotlin()
        is Double -> this.roundToLong().toBigIntegerKotlin()
        else -> BigInteger(this.toString())
    }
