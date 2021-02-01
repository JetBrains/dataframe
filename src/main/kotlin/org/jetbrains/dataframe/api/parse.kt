package org.jetbrains.dataframe

import org.jetbrains.dataframe.api.columns.DataColumn
import org.jetbrains.dataframe.api.columns.allNulls
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.withNullability

data class ParseClause<T>(val df: DataFrame<T>, val selector: ColumnsSelector<T, String?>) {

    fun <C: Any> getParser(type: KType) = Parsers[type] as? StringParser<C>

    inline fun <reified C: Any> to(): DataFrame<T> {

        val parser = getParser<C>(getType<C>()) ?: throw IllegalArgumentException("Parsing to type '${C::class}' is not supported")

        return df.update(selector).notNull { parser.parse(it) }
    }
}

fun <T> DataFrame<T>.parse(selector: ColumnsSelector<T, String?>) = ParseClause(this, selector)

class StringParser<T : Any>(val type: KType, val parse: (String) -> T?)

internal object Parsers {

    private fun String.toBooleanOrNull() =
            when (toUpperCase()) {
                "T" -> true
                "TRUE" -> true
                "YES" -> true
                "F" -> false
                "FALSE" -> false
                "NO" -> false
                else -> null
            }

    inline fun <reified T : Any> stringParser(noinline body: (String) -> T?) = StringParser(getType<T>(), body)

    private val allParsers = listOf(
            stringParser { it.toIntOrNull() },
            stringParser { it.toLongOrNull() },
            stringParser { it.toDoubleOrNull() },
            stringParser { it.toBooleanOrNull() },
            stringParser { it.toBigDecimalOrNull() }
    )

    private val parsersMap = allParsers.associateBy { it.type }

    val size: Int = allParsers.size

    operator fun get(index: Int): StringParser<*> = allParsers[index]

    operator fun get(type: KType): StringParser<*>? = parsersMap.get(type)

    operator fun <T: Any> get(type: KClass<T>): StringParser<*>? = parsersMap.get(type.createStarProjectedType(false))

    inline fun <reified T : Any> get(): StringParser<T>? = get(getType<T>()) as? StringParser<T>
}

internal inline fun <reified T : Any> DataColumn<String?>.parse(): DataColumn<T?> {
    val parser = Parsers.get<T>() ?: throw Exception("Couldn't find parser for type ${T::class}")
    return parse(parser)
}

internal fun <T : Any> DataColumn<String?>.parse(parser: StringParser<T>): DataColumn<T?> {
    val parsedValues = values.map {
        it?.let {
            parser.parse(it) ?: throw Exception("Couldn't parse '${it}' to type ${parser.type}")
        }
    }
    return DataColumn.create(name(), parsedValues, parser.type.withNullability(hasNulls)) as DataColumn<T?>
}

internal fun DataColumn<String?>.tryParseAny(): DataColumn<*> {

    if(allNulls()) return this

    var parserId = 0
    val parsedValues = mutableListOf<Any?>()

    do {
        val parser = Parsers[parserId]
        parsedValues.clear()
        for (str in values) {
            if (str == null) parsedValues.add(null)
            else {
                val res = parser.parse(str)
                if (res == null) {
                    parserId++
                    break
                }
                parsedValues.add(res)
            }
        }
    } while (parserId < Parsers.size && parsedValues.size != size)
    if (parserId == Parsers.size) return this
    return DataColumn.create(name(), parsedValues, Parsers[parserId].type.withNullability(hasNulls))
}
