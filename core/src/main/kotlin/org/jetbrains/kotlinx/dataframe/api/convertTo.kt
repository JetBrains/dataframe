package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.ColumnsSelector
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.RowExpression
import org.jetbrains.kotlinx.dataframe.exceptions.ColumnNotFoundException
import org.jetbrains.kotlinx.dataframe.exceptions.ExcessiveColumnsException
import org.jetbrains.kotlinx.dataframe.exceptions.TypeConversionException
import org.jetbrains.kotlinx.dataframe.exceptions.TypeConverterNotFoundException
import org.jetbrains.kotlinx.dataframe.impl.api.ConvertSchemaDslInternal
import org.jetbrains.kotlinx.dataframe.impl.api.convertToImpl
import org.jetbrains.kotlinx.dataframe.schema.ColumnSchema
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.typeOf

/**
 * Specifies how to handle columns in original dataframe that were not matched to any column in destination dataframe schema.
 */
public enum class ExcessiveColumns {
    /**
     * Remove excessive columns from resulting dataframe
     */
    Remove,

    /**
     * Keep excessive columns in resulting dataframe
     */
    Keep,

    /**
     * Throw [ExcessiveColumnsException] if any excessive columns were found in the original dataframe
     */
    Fail,
}

/**
 * Holds data context for [fill] operation
 */
public data class ConvertToFill<T, C>(internal val dsl: ConvertSchemaDsl<T>, val columns: ColumnsSelector<T, C>)

/** Provides access to [fromType] and [toSchema] in the flexible [ConvertSchemaDsl.convertIf] method. */
public class ConverterScope(public val fromType: KType, public val toSchema: ColumnSchema)

/**
 * Dsl to customize column conversion
 *
 * Example:
 * ```kotlin
 * df.convertTo<SomeSchema> {
 *     // defines how to convert Int? -> String
 *     convert<Int?>().with { it?.toString() ?: "No input given" }
 *     // defines how to convert String -> SomeType (and Char.toString() -> SomeType)
 *     parser { SomeType(it) }
 *     // fill missing column `sum` with expression `a+b`
 *     fill { sum }.with { a + b }
 * }
 * ```
 */
public interface ConvertSchemaDsl<in T> {

    /**
     * Defines how to convert [from]: [A] to [to]: [B].
     *
     * Note: In most cases using `convert<Type>().with { }` is more convenient, however
     * if you only have [KType], this method can be used.
     */
    public fun <A, B> convert(from: KType, to: KType, converter: (A) -> B)

    /**
     * Advanced version of [convert].
     * If you want to define a common conversion for multiple types (or any type), or
     * you need extra information about the target, such as its schema, use this method.
     *
     * The exact type conversion does have higher priority. After that, this flexible conversions will be checked
     * in order.
     *
     * @param condition a function that should return `true` if the conversion should be applied from the given `fromType`
     *   to the given `toSchema`.
     * @param converter a function that performs the conversion with access to a [ConverterScope].
     */
    public fun convertIf(
        condition: (fromType: KType, toSchema: ColumnSchema) -> Boolean,
        converter: ConverterScope.(Any?) -> Any?,
    )
}

/**
 * Defines how to fill specified columns in destination schema that were not found in original dataframe.
 * All [fill] operations for missing columns are executed after successful conversion of matched columns, so converted values of matched columns can be safely used in [with] expression.
 * @param columns target columns in destination dataframe schema to be filled
 */
public inline fun <T, reified C> ConvertSchemaDsl<T>.fill(
    noinline columns: ColumnsSelector<T, C>,
): ConvertToFill<T, C> = ConvertToFill(this, columns)

public fun <T, C> ConvertToFill<T, C>.with(expr: RowExpression<T, C>) {
    (dsl as ConvertSchemaDslInternal<T>).fill(columns as ColumnsSelector<*, C>, expr as RowExpression<*, C>)
}

/**
 * Defines how to convert `String` values into given type [C].
 *
 * This method is a shortcut for `convert<String>().with { }`.
 *
 * If no converter is defined for `Char` values, this converter will be used for them as well.
 */
public inline fun <reified C> ConvertSchemaDsl<*>.parser(noinline parser: (String) -> C): Unit =
    convert<String>().with(parser)

/**
 * Defines how to convert values of given type [C]
 */
public inline fun <reified C> ConvertSchemaDsl<*>.convert(): ConvertType<C> = ConvertType(this, typeOf<C>())

/**
 * Defines how to convert values of type [C] into type [R]
 */
public inline fun <C, reified R> ConvertType<C>.with(noinline converter: (C) -> R): Unit =
    dsl.convert(from, typeOf<R>(), converter)

public class ConvertType<T>(
    @PublishedApi internal val dsl: ConvertSchemaDsl<*>,
    @PublishedApi internal val from: KType,
    internal val property: KProperty<T>? = null,
)

// region DataFrame

/**
 * Converts values in [DataFrame] to match given column schema [T].
 *
 * Original columns are mapped to destination columns by column [path][DataColumn.path].
 *
 * Type converters for every column are selected automatically. See [convert] operation for details.
 *
 * To specify custom type converters for the particular types use [ConvertSchemaDsl].
 *
 * Example of Dsl:
 * ```kotlin
 * df.convertTo<SomeSchema> {
 *     // defines how to convert Int? -> String
 *     convert<Int?>().with { it?.toString() ?: "No input given" }
 *     // defines how to convert String -> SomeType
 *     parser { SomeType(it) }
 *     // fill missing column `sum` with expression `a + b`
 *     fill { sum }.with { a + b }
 * }
 * ```
 *
 * @param [T] class that defines target schema for conversion.
 * @param [excessiveColumnsBehavior] how to handle excessive columns in the original [DataFrame].
 * @param [body] optional dsl to define custom type converters.
 * @throws [ColumnNotFoundException] if [DataFrame] doesn't contain columns that are required by destination schema.
 * @throws [ExcessiveColumnsException] if [DataFrame] contains columns that are not required by destination schema and [excessiveColumnsBehavior] is set to [ExcessiveColumns.Fail].
 * @throws [TypeConverterNotFoundException] if suitable type converter for some column was not found.
 * @throws [TypeConversionException] if type converter failed to convert column values.
 * @return converted [DataFrame].
 */
public inline fun <reified T : Any> AnyFrame.convertTo(
    excessiveColumnsBehavior: ExcessiveColumns = ExcessiveColumns.Keep,
    noinline body: ConvertSchemaDsl<T>.() -> Unit = {},
): DataFrame<T> = convertToImpl(typeOf<T>(), true, excessiveColumnsBehavior, body).cast()

/**
 * Converts values in [DataFrame] to match given column schema [T].
 *
 * Original columns are mapped to destination columns by column [path][DataColumn.path].
 *
 * Type converters for every column are selected automatically. See [convert] operation for details.
 *
 * To specify custom type converters for the particular types use [ConvertSchemaDsl].
 *
 * Example of Dsl:
 * ```kotlin
 * df.convertTo(schemaFrom = sample) {
 *     // defines how to convert Int? -> String
 *     convert<Int?>().with { it?.toString() ?: "No input given" }
 *     // defines how to convert String -> SomeType
 *     parser { SomeType(it) }
 *     // fill missing column `sum` with expression `a + b`
 *     fill { sum }.with { a + b }
 * }
 * ```
 *
 * @param [T] class that defines target schema for conversion.
 * @param [schemaFrom] dataframe which type [T] will be used.
 * @param [excessiveColumnsBehavior] how to handle excessive columns in the original [DataFrame].
 * @param [body] optional dsl to define custom type converters.
 * @throws [ColumnNotFoundException] if [DataFrame] doesn't contain columns that are required by destination schema.
 * @throws [ExcessiveColumnsException] if [DataFrame] contains columns that are not required by destination schema and [excessiveColumnsBehavior] is set to [ExcessiveColumns.Fail].
 * @throws [TypeConverterNotFoundException] if suitable type converter for some column was not found.
 * @throws [TypeConversionException] if type converter failed to convert column values.
 * @return converted [DataFrame].
 */
public inline fun <reified T : Any> AnyFrame.convertTo(
    @Suppress("UNUSED_PARAMETER") schemaFrom: DataFrame<T>,
    excessiveColumnsBehavior: ExcessiveColumns = ExcessiveColumns.Keep,
    noinline body: ConvertSchemaDsl<T>.() -> Unit = {},
): DataFrame<T> = convertToImpl(typeOf<T>(), true, excessiveColumnsBehavior, body).cast()

/**
 * Converts values in [DataFrame] to match given column schema [schemaType].
 *
 * Original columns are mapped to destination columns by column [path][DataColumn.path].
 *
 * Type converters for every column are selected automatically. See [convert] operation for details.
 *
 * To specify custom type converters for the particular types use [ConvertSchemaDsl].
 *
 * Example of Dsl:
 * ```kotlin
 * df.convertTo<SomeSchema> {
 *     // defines how to convert Int? -> String
 *     convert<Int?>().with { it?.toString() ?: "No input given" }
 *     // defines how to convert String -> SomeType
 *     parser { SomeType(it) }
 *     // fill missing column `sum` with expression `a+b`
 *     fill { sum }.with { a + b }
 * }
 * ```
 *
 * @param [schemaType] defines target schema for conversion.
 * @param [excessiveColumnsBehavior] how to handle excessive columns in the original [DataFrame].
 * @param [body] optional dsl to define custom type converters.
 * @throws [ColumnNotFoundException] if [DataFrame] doesn't contain columns that are required by destination schema.
 * @throws [ExcessiveColumnsException] if [DataFrame] contains columns that are not required by destination schema and [excessiveColumnsBehavior] is set to [ExcessiveColumns.Fail].
 * @throws [TypeConverterNotFoundException] if suitable type converter for some column was not found.
 * @throws [TypeConversionException] if type converter failed to convert column values.
 * @return converted [DataFrame].
 */
public fun AnyFrame.convertTo(
    schemaType: KType,
    excessiveColumnsBehavior: ExcessiveColumns = ExcessiveColumns.Keep,
    body: ConvertSchemaDsl<Any>.() -> Unit = {},
): AnyFrame = convertToImpl(schemaType, true, excessiveColumnsBehavior, body)

// endregion
