package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.ColumnsSelector
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.annotations.AccessApiOverload
import org.jetbrains.kotlinx.dataframe.columns.ColumnReference
import org.jetbrains.kotlinx.dataframe.columns.toColumnSet
import org.jetbrains.kotlinx.dataframe.impl.api.Parsers
import org.jetbrains.kotlinx.dataframe.impl.api.StringParser
import org.jetbrains.kotlinx.dataframe.impl.api.parseImpl
import org.jetbrains.kotlinx.dataframe.impl.api.tryParseImpl
import org.jetbrains.kotlinx.dataframe.impl.io.FastDoubleParser
import org.jetbrains.kotlinx.dataframe.typeClass
import org.jetbrains.kotlinx.dataframe.util.PARSER_OPTIONS
import org.jetbrains.kotlinx.dataframe.util.PARSER_OPTIONS_COPY
import java.time.format.DateTimeFormatter
import java.util.Locale
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.typeOf

/**
 * ### Global Parser Options
 *
 * These options are used to configure how [DataColumns][DataColumn] of type [String] or [String?][String]
 * should be parsed.
 * You can always pass a [ParserOptions] object to functions that perform parsing, like [tryParse], [parse],
 * or even [DataFrame.readCsv][DataFrame.Companion.readCsv] to override these options.
 */
public val DataFrame.Companion.parser: GlobalParserOptions
    get() = Parsers

public fun <T> DataFrame<T>.parse(options: ParserOptions? = null, columns: ColumnsSelector<T, Any?>): DataFrame<T> =
    parseImpl(options, columns)

public fun <T> DataFrame<T>.parse(vararg columns: String, options: ParserOptions? = null): DataFrame<T> =
    parse(options) { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C> DataFrame<T>.parse(vararg columns: ColumnReference<C>, options: ParserOptions? = null): DataFrame<T> =
    parse(options) { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C> DataFrame<T>.parse(vararg columns: KProperty<C>, options: ParserOptions? = null): DataFrame<T> =
    parse(options) { columns.toColumnSet() }

/**
 * Global counterpart of [ParserOptions].
 * Settings changed here will affect the defaults for all parsing operations.
 *
 * The default values are set by [Parsers.resetToDefault].
 */
public interface GlobalParserOptions {

    public fun addDateTimePattern(pattern: String)

    public fun addNullString(str: String)

    /** This function can be called to skip some types. Parsing will be attempted for all other types. */
    public fun addSkipType(type: KType)

    /** Whether to use [FastDoubleParser], defaults to `true`. Please report any issues you encounter. */
    public var useFastDoubleParser: Boolean

    public fun resetToDefault()

    public var locale: Locale

    public val nulls: Set<String>

    public val skipTypes: Set<KType>
}

/**
 * ### Options for parsing [String]`?` columns
 *
 * These options are used to configure how [DataColumn]s of type [String] or [String?][String] should be parsed.
 * They can be passed to [tryParse] and [parse] functions.
 *
 * You can also use the [DataFrame.parser][DataFrame.Companion.parser] property to access and modify
 * the global parser configuration.
 *
 * If any of the arguments in [ParserOptions] are `null` (or [ParserOptions] itself is `null`),
 * the global configuration will be queried.
 *
 * @param locale locale to use for parsing dates and numbers, defaults to the System default locale.
 *   If specified instead of [dateTimeFormatter], it will be used in combination with [dateTimePattern]
 *   to create a [DateTimeFormatter]. Just providing [locale] will not allow you to parse
 *   locale-specific dates!
 * @param dateTimeFormatter a [DateTimeFormatter] to use for parsing dates, if not specified, it will be created
 *   from [dateTimePattern] and [locale]. If neither [dateTimeFormatter] nor [dateTimePattern] are specified,
 *   [DateTimeFormatter.ISO_LOCAL_DATE_TIME] will be used.
 * @param dateTimePattern a pattern to use for parsing dates. If specified instead of [dateTimeFormatter],
 *   it will be used to create a [DateTimeFormatter].
 * @param nullStrings a set of strings that should be treated as `null` values. By default, it's
 *   `["null", "NULL", "NA", "N/A"]`.
 * @param skipTypes a set of types that should be skipped during parsing. Parsing will be attempted for all other types.
 *   By default, it's an empty set. To skip all types except a specified one, use [convertTo] instead.
 * @param useFastDoubleParser whether to use [FastDoubleParser], defaults to `true`. Please report any issues you encounter.
 */
public class ParserOptions(
    public val locale: Locale? = null,
    // TODO, migrate to kotlinx.datetime.format.DateTimeFormat? https://github.com/Kotlin/dataframe/issues/876
    public val dateTimeFormatter: DateTimeFormatter? = null,
    public val dateTimePattern: String? = null,
    public val nullStrings: Set<String>? = null,
    public val skipTypes: Set<KType>? = null,
    public val useFastDoubleParser: Boolean? = null,
) {

    /** For binary compatibility. */
    @Deprecated(
        message = PARSER_OPTIONS,
        level = DeprecationLevel.HIDDEN,
    )
    public constructor(
        locale: Locale? = null,
        dateTimeFormatter: DateTimeFormatter? = null,
        dateTimePattern: String? = null,
        nullStrings: Set<String>? = null,
    ) : this(
        locale = locale,
        dateTimeFormatter = dateTimeFormatter,
        dateTimePattern = dateTimePattern,
        nullStrings = nullStrings,
        skipTypes = null,
        useFastDoubleParser = null,
    )

    /** For binary compatibility. */
    @Deprecated(
        message = PARSER_OPTIONS_COPY,
        level = DeprecationLevel.HIDDEN,
    )
    public fun copy(
        locale: Locale? = this.locale,
        dateTimeFormatter: DateTimeFormatter? = this.dateTimeFormatter,
        dateTimePattern: String? = this.dateTimePattern,
        nullStrings: Set<String>? = this.nullStrings,
    ): ParserOptions =
        ParserOptions(
            locale = locale,
            dateTimeFormatter = dateTimeFormatter,
            dateTimePattern = dateTimePattern,
            nullStrings = nullStrings,
            skipTypes = skipTypes,
            useFastDoubleParser = useFastDoubleParser,
        )

    internal fun getDateTimeFormatter(): DateTimeFormatter? =
        when {
            dateTimeFormatter != null -> dateTimeFormatter
            dateTimePattern != null && locale != null -> DateTimeFormatter.ofPattern(dateTimePattern, locale)
            dateTimePattern != null -> DateTimeFormatter.ofPattern(dateTimePattern)
            else -> null
        }

    public fun copy(
        locale: Locale? = this.locale,
        dateTimeFormatter: DateTimeFormatter? = this.dateTimeFormatter,
        dateTimePattern: String? = this.dateTimePattern,
        nullStrings: Set<String>? = this.nullStrings,
        skipTypes: Set<KType>? = this.skipTypes,
        useFastDoubleParser: Boolean? = this.useFastDoubleParser,
    ): ParserOptions =
        ParserOptions(
            locale = locale,
            dateTimeFormatter = dateTimeFormatter,
            dateTimePattern = dateTimePattern,
            nullStrings = nullStrings,
            skipTypes = skipTypes,
            useFastDoubleParser = useFastDoubleParser,
        )

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ParserOptions

        if (useFastDoubleParser != other.useFastDoubleParser) return false
        if (locale != other.locale) return false
        if (dateTimeFormatter != other.dateTimeFormatter) return false
        if (dateTimePattern != other.dateTimePattern) return false
        if (nullStrings != other.nullStrings) return false
        if (skipTypes != other.skipTypes) return false

        return true
    }

    override fun hashCode(): Int {
        var result = useFastDoubleParser?.hashCode() ?: 0
        result = 31 * result + (locale?.hashCode() ?: 0)
        result = 31 * result + (dateTimeFormatter?.hashCode() ?: 0)
        result = 31 * result + (dateTimePattern?.hashCode() ?: 0)
        result = 31 * result + (nullStrings?.hashCode() ?: 0)
        result = 31 * result + (skipTypes?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String =
        "ParserOptions(locale=$locale, dateTimeFormatter=$dateTimeFormatter, dateTimePattern=$dateTimePattern, nullStrings=$nullStrings, skipTypes=$skipTypes, useFastDoubleParser=$useFastDoubleParser)"
}

/** @include [tryParseImpl] */
public fun DataColumn<String?>.tryParse(options: ParserOptions? = null): DataColumn<*> = tryParseImpl(options)

/**
 * Tries to parse a column of chars into a column of a different type.
 * Each parser in [Parsers] is run in order until a valid parser is found,
 * a.k.a. that parser was able to parse all values in the column successfully. If a parser
 * fails to parse any value, the next parser is tried. If all the others fail, the final parser
 * returns strings.
 *
 * Parsers that are [covered by][StringParser.coveredBy] other parsers are skipped.
 *
 * @param options options for parsing, like providing a locale or a custom date-time formatter
 * @throws IllegalStateException if no valid parser is found (unlikely, unless the `String` parser is disabled)
 * @return a new column with parsed values
 */
@JvmName("tryParseChar")
public fun DataColumn<Char?>.tryParse(options: ParserOptions? = null): DataColumn<*> {
    // skip the Char parser, as we're trying to parse away from Char
    val providedSkipTypes = options?.skipTypes ?: DataFrame.parser.skipTypes
    val parserOptions = (options ?: ParserOptions()).copy(skipTypes = providedSkipTypes + typeOf<Char>())

    return map { it?.toString() }.tryParse(parserOptions)
}

public fun <T> DataFrame<T>.parse(options: ParserOptions? = null): DataFrame<T> =
    parse(options) {
        colsAtAnyDepth { !it.isColumnGroup() }
    }

/**
 * Tries to parse a column of strings into a column of a different type.
 * Each parser in [Parsers] is run in order until a valid parser is found,
 * a.k.a. that parser was able to parse all values in the column successfully. If a parser
 * fails to parse any value, the next parser is tried.
 *
 * If all fail [IllegalStateException] is thrown. If you don't want this exception to be thrown,
 * use [tryParse] instead.
 *
 * Parsers that are [covered by][StringParser.coveredBy] other parsers are skipped.
 *
 * @param options options for parsing, like providing a locale or a custom date-time formatter
 * @throws IllegalStateException if no valid parser is found
 * @return a new column with parsed values
 */
public fun DataColumn<String?>.parse(options: ParserOptions? = null): DataColumn<*> =
    tryParse(options).also { if (it.typeClass == String::class) error("Can't guess column type") }

/**
 * Tries to parse a column of chars as strings into a column of a different type.
 * Each parser in [Parsers] is run in order until a valid parser is found,
 * a.k.a. that parser was able to parse all values in the column successfully. If a parser
 * fails to parse any value, the next parser is tried.
 *
 * If all fail, the column is returned as `String`, this can never fail.
 *
 * Parsers that are [covered by][StringParser.coveredBy] other parsers are skipped.
 *
 * @param options options for parsing, like providing a locale or a custom date-time formatter
 * @return a new column with parsed values
 */
@JvmName("parseChar")
public fun DataColumn<Char?>.parse(options: ParserOptions? = null): DataColumn<*> =
    tryParse(options) // no need to throw an exception, as Char can always be parsed as String

@JvmName("parseAnyFrameNullable")
public fun DataColumn<AnyFrame?>.parse(options: ParserOptions? = null): DataColumn<AnyFrame?> =
    map { it?.parse(options) }
