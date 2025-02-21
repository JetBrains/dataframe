package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.AnyColumnReference
import org.jetbrains.kotlinx.dataframe.ColumnsSelector
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.annotations.AccessApiOverload
import org.jetbrains.kotlinx.dataframe.annotations.Interpretable
import org.jetbrains.kotlinx.dataframe.annotations.Refine
import org.jetbrains.kotlinx.dataframe.columns.ColumnReference
import org.jetbrains.kotlinx.dataframe.columns.toColumnSet
import org.jetbrains.kotlinx.dataframe.impl.api.removeImpl
import org.jetbrains.kotlinx.dataframe.util.MINUS
import org.jetbrains.kotlinx.dataframe.util.MINUS_REPLACE
import kotlin.reflect.KProperty

// region DataFrame

// region remove

/**
 * ## The Remove Operation
 *
 * Removes the specified [columns] from the original [DataFrame] and returns a new [DataFrame] without them.
 *
 * This can include [column groups][org.jetbrains.kotlinx.dataframe.columns.ColumnGroup] and nested columns.
 *
 * See [Selecting Columns][Select.SelectSelectingOptions].
 *
 * For more information: [See `remove` on the documentation website.](https://kotlin.github.io/dataframe/remove.html)
 */
internal interface Remove

/**
 * ## The Remove Operation
 *
 * Removes the specified [columns][org.jetbrains.kotlinx.dataframe.columns] from the original [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] and returns a new [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] without them.
 *
 * This can include [column groups][org.jetbrains.kotlinx.dataframe.columns.ColumnGroup] and nested columns.
 *
 * See [Selecting Columns][org.jetbrains.kotlinx.dataframe.api.Select.SelectSelectingOptions].
 *
 * For more information: [See `remove` on the documentation website.](https://kotlin.github.io/dataframe/remove.html)
 * ### This Remove Overload
 * Select or express columns using the [Columns Selection DSL][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl].
 * (Any (combination of) [Access API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi]).
 *
 * This DSL is initiated by a [Columns Selector][org.jetbrains.kotlinx.dataframe.ColumnsSelector] lambda,
 * which operates in the context of the [Columns Selection DSL][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl] and
 * expects you to return a [SingleColumn][org.jetbrains.kotlinx.dataframe.columns.SingleColumn] or [ColumnSet][org.jetbrains.kotlinx.dataframe.columns.ColumnSet] (so, a [ColumnsResolver][org.jetbrains.kotlinx.dataframe.columns.ColumnsResolver]).
 * This is an entity formed by calling any (combination) of the functions
 * in the DSL that is or can be resolved into one or more columns.
 *
 * #### NOTE:
 * While you can use the [String API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.StringApi] and [KProperties API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.KPropertiesApi]
 * in this DSL directly with any function, they are NOT valid return types for the
 * [Columns Selector][org.jetbrains.kotlinx.dataframe.ColumnsSelector] lambda. You'd need to turn them into a [ColumnReference][org.jetbrains.kotlinx.dataframe.columns.ColumnReference] first, for instance
 * with a function like [`col("name")`][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl.col].
 *
 * ### Check out: [Columns Selection DSL Grammar][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl.DslGrammar]
 *
 * &nbsp;&nbsp;&nbsp;&nbsp;
 *
 * [See Column Selectors on the documentation website.](https://kotlin.github.io/dataframe/columnselectors.html)
 *
 * #### For example:
 *
 * `df.`[remove][org.jetbrains.kotlinx.dataframe.api.remove]` { length `[and][org.jetbrains.kotlinx.dataframe.api.AndColumnsSelectionDsl.and]` age }`
 *
 * `df.`[remove][org.jetbrains.kotlinx.dataframe.api.remove]`  {  `[cols][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl.cols]`(1..5) }`
 *
 * `df.`[remove][org.jetbrains.kotlinx.dataframe.api.remove]`  {  `[colsOf][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl.colsOf]`<`[Double][Double]`>() }`
 *
 *
 * @param [columns] The [Columns Selector][ColumnsSelector] used to remove the columns of this [DataFrame].
 */
@Refine
@Interpretable("Remove0")
public fun <T> DataFrame<T>.remove(columns: ColumnsSelector<T, *>): DataFrame<T> =
    removeImpl(allowMissingColumns = true, columns = columns).df

/**
 * ## The Remove Operation
 *
 * Removes the specified [columns][org.jetbrains.kotlinx.dataframe.columns] from the original [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] and returns a new [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] without them.
 *
 * This can include [column groups][org.jetbrains.kotlinx.dataframe.columns.ColumnGroup] and nested columns.
 *
 * See [Selecting Columns][org.jetbrains.kotlinx.dataframe.api.Select.SelectSelectingOptions].
 *
 * For more information: [See `remove` on the documentation website.](https://kotlin.github.io/dataframe/remove.html)
 * ### This Remove Overload
 * Select columns using their [column names][String]
 * ([String API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.StringApi]).
 *
 * #### For example:
 *
 * `df.`[remove][org.jetbrains.kotlinx.dataframe.api.remove]`("length", "age")`
 *
 * @param [columns] The [Column Names][String] used to remove the columns of this [DataFrame].
 */
public fun <T> DataFrame<T>.remove(vararg columns: String): DataFrame<T> = remove { columns.toColumnSet() }

/**
 * ## The Remove Operation
 *
 * Removes the specified [columns][org.jetbrains.kotlinx.dataframe.columns] from the original [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] and returns a new [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] without them.
 *
 * This can include [column groups][org.jetbrains.kotlinx.dataframe.columns.ColumnGroup] and nested columns.
 *
 * See [Selecting Columns][org.jetbrains.kotlinx.dataframe.api.Select.SelectSelectingOptions].
 *
 * For more information: [See `remove` on the documentation website.](https://kotlin.github.io/dataframe/remove.html)
 * ### This Remove Overload
 * Select columns using [column accessors][org.jetbrains.kotlinx.dataframe.columns.ColumnReference]
 * ([Column Accessors API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.ColumnAccessorsApi]).
 *
 * #### For example:
 *
 * `val length by `[column][org.jetbrains.kotlinx.dataframe.api.column]`<`[Double][Double]`>()`
 *
 * `val age by `[column][org.jetbrains.kotlinx.dataframe.api.column]`<`[Double][Double]`>()`
 *
 * `df.`[remove][org.jetbrains.kotlinx.dataframe.api.remove]`(length, age)`
 *
 * @param [columns] The [Column Accessors][ColumnReference] used to remove the columns of this [DataFrame].
 */
@AccessApiOverload
public fun <T> DataFrame<T>.remove(vararg columns: AnyColumnReference): DataFrame<T> = remove { columns.toColumnSet() }

/**
 * ## The Remove Operation
 *
 * Removes the specified [columns][org.jetbrains.kotlinx.dataframe.columns] from the original [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] and returns a new [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] without them.
 *
 * This can include [column groups][org.jetbrains.kotlinx.dataframe.columns.ColumnGroup] and nested columns.
 *
 * See [Selecting Columns][org.jetbrains.kotlinx.dataframe.api.Select.SelectSelectingOptions].
 *
 * For more information: [See `remove` on the documentation website.](https://kotlin.github.io/dataframe/remove.html)
 * ### This Remove Overload
 * Select columns using [KProperties][KProperty] ([KProperties API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.KPropertiesApi]).
 *
 * #### For example:
 * ```kotlin
 * data class Person(val length: Double, val age: Double)
 * ```
 *
 * `df.`[remove][org.jetbrains.kotlinx.dataframe.api.remove]`(Person::length, Person::age)`
 *
 * @param [columns] The [KProperties][KProperty] used to remove the columns of this [DataFrame].
 */
@AccessApiOverload
public fun <T> DataFrame<T>.remove(vararg columns: KProperty<*>): DataFrame<T> = remove { columns.toColumnSet() }

// endregion

// region minus

@Deprecated(MINUS, ReplaceWith(MINUS_REPLACE), DeprecationLevel.ERROR)
public infix operator fun <T> DataFrame<T>.minus(columns: ColumnsSelector<T, *>): DataFrame<T> = remove(columns)

@Deprecated(MINUS, ReplaceWith(MINUS_REPLACE), DeprecationLevel.ERROR)
public infix operator fun <T> DataFrame<T>.minus(column: String): DataFrame<T> = remove(column)

@Deprecated(MINUS, ReplaceWith(MINUS_REPLACE), DeprecationLevel.ERROR)
@AccessApiOverload
public infix operator fun <T> DataFrame<T>.minus(column: AnyColumnReference): DataFrame<T> = remove(column)

@Deprecated(MINUS, ReplaceWith(MINUS_REPLACE), DeprecationLevel.ERROR)
@AccessApiOverload
public infix operator fun <T> DataFrame<T>.minus(columns: KProperty<*>): DataFrame<T> = remove(columns)

// endregion

// endregion
