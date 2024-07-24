package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.ColumnsSelector
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.columns.ColumnReference
import org.jetbrains.kotlinx.dataframe.columns.toColumnSet
import org.jetbrains.kotlinx.dataframe.impl.api.Parsers
import org.jetbrains.kotlinx.dataframe.impl.api.parseImpl
import org.jetbrains.kotlinx.dataframe.impl.api.tryParseImpl
import org.jetbrains.kotlinx.dataframe.typeClass
import java.time.format.DateTimeFormatter
import java.util.Locale
import kotlin.reflect.KProperty

public val DataFrame.Companion.parser: GlobalParserOptions get() = Parsers

public fun <T> DataFrame<T>.parse(options: ParserOptions? = null, columns: ColumnsSelector<T, Any?>): DataFrame<T> =
    parseImpl(options, columns)

public fun <T> DataFrame<T>.parse(vararg columns: String, options: ParserOptions? = null): DataFrame<T> =
    parse(options) { columns.toColumnSet() }

public fun <T, C> DataFrame<T>.parse(vararg columns: ColumnReference<C>, options: ParserOptions? = null): DataFrame<T> =
    parse(options) { columns.toColumnSet() }

public fun <T, C> DataFrame<T>.parse(vararg columns: KProperty<C>, options: ParserOptions? = null): DataFrame<T> =
    parse(options) { columns.toColumnSet() }

public interface GlobalParserOptions {

    public fun addDateTimePattern(pattern: String)

    public fun addNullString(str: String)

    public fun resetToDefault()

    public var locale: Locale
}

public data class ParserOptions(
    val locale: Locale? = null,
    val dateTimeFormatter: DateTimeFormatter? = null,
    val dateTimePattern: String? = null,
    val nullStrings: Set<String>? = null,
) {
    internal fun getDateTimeFormatter(): DateTimeFormatter? =
        when {
            dateTimeFormatter != null -> dateTimeFormatter
            dateTimePattern != null && locale != null -> DateTimeFormatter.ofPattern(dateTimePattern, locale)
            dateTimePattern != null -> DateTimeFormatter.ofPattern(dateTimePattern)
            else -> null
        }
}

public fun DataColumn<String?>.tryParse(options: ParserOptions? = null): DataColumn<*> = tryParseImpl(options)

public fun <T> DataFrame<T>.parse(options: ParserOptions? = null): DataFrame<T> =
    parse(options) {
        colsAtAnyDepth { !it.isColumnGroup() }
    }

public fun DataColumn<String?>.parse(options: ParserOptions? = null): DataColumn<*> =
    tryParse(options).also { if (it.typeClass == String::class) error("Can't guess column type") }

@JvmName("parseAnyFrameNullable")
public fun DataColumn<AnyFrame?>.parse(options: ParserOptions? = null): DataColumn<AnyFrame?> =
    map { it?.parse(options) }
