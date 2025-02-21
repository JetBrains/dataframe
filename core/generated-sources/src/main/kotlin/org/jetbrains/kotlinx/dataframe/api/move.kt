package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.AnyColumnGroupAccessor
import org.jetbrains.kotlinx.dataframe.AnyColumnReference
import org.jetbrains.kotlinx.dataframe.ColumnSelector
import org.jetbrains.kotlinx.dataframe.ColumnsSelector
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.annotations.AccessApiOverload
import org.jetbrains.kotlinx.dataframe.annotations.CandidateForRemoval
import org.jetbrains.kotlinx.dataframe.annotations.Interpretable
import org.jetbrains.kotlinx.dataframe.annotations.Refine
import org.jetbrains.kotlinx.dataframe.api.Move.MoveSelectingOptions
import org.jetbrains.kotlinx.dataframe.columns.ColumnGroup
import org.jetbrains.kotlinx.dataframe.columns.ColumnReference
import org.jetbrains.kotlinx.dataframe.columns.ColumnWithPath
import org.jetbrains.kotlinx.dataframe.columns.toColumnSet
import org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns
import org.jetbrains.kotlinx.dataframe.impl.api.afterOrBefore
import org.jetbrains.kotlinx.dataframe.impl.api.moveImpl
import org.jetbrains.kotlinx.dataframe.impl.api.moveTo
import org.jetbrains.kotlinx.dataframe.ncol
import org.jetbrains.kotlinx.dataframe.util.MOVE_TO_LEFT
import org.jetbrains.kotlinx.dataframe.util.MOVE_TO_LEFT_REPLACE
import org.jetbrains.kotlinx.dataframe.util.MOVE_TO_RIGHT
import org.jetbrains.kotlinx.dataframe.util.MOVE_TO_RIGHT_REPLACE
import org.jetbrains.kotlinx.dataframe.util.TO_LEFT
import org.jetbrains.kotlinx.dataframe.util.TO_LEFT_REPLACE
import org.jetbrains.kotlinx.dataframe.util.TO_RIGHT
import org.jetbrains.kotlinx.dataframe.util.TO_RIGHT_REPLACE
import kotlin.reflect.KProperty

// region DataFrame

// region move

/**
 * ## The Move Operation
 *
 * Moves the specified [columns] within the [DataFrame].
 *
 * This function does not immediately move the columns but instead select columns to move and
 * returns a [MoveClause],
 * which serves as an intermediate step. The [MoveClause] allows specifying the final
 * destination of the selected columns using methods such as [MoveClause.to], [MoveClause.toStart],
 * [MoveClause.toEnd], [MoveClause.into], [MoveClause.intoIndexed], [MoveClause.toTop],
 * [MoveClause.after] or [MoveClause.under], that return a new [DataFrame] with updated columns order.
 * Check out [Grammar].
 *
 * This can include [column groups][org.jetbrains.kotlinx.dataframe.columns.ColumnGroup] and nested columns.
 *
 * See [Selecting Columns][MoveSelectingOptions].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 */
internal interface Move {

    /**
     *
     * ## Selecting Columns
     * Selecting columns for various operations (including but not limited to
     * [DataFrame.select][org.jetbrains.kotlinx.dataframe.DataFrame.select], [DataFrame.update][org.jetbrains.kotlinx.dataframe.DataFrame.update], [DataFrame.gather][org.jetbrains.kotlinx.dataframe.DataFrame.gather], and [DataFrame.fillNulls][org.jetbrains.kotlinx.dataframe.DataFrame.fillNulls])
     * can be done in the following ways:
     * ### 1. [Columns Selection DSL][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.Dsl.WithExample]
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
     * `df.`[move][org.jetbrains.kotlinx.dataframe.api.move]` { length `[and][org.jetbrains.kotlinx.dataframe.api.AndColumnsSelectionDsl.and]` age }`
     *
     * `df.`[move][org.jetbrains.kotlinx.dataframe.api.move]`  {  `[cols][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl.cols]`(1..5) }`
     *
     * `df.`[move][org.jetbrains.kotlinx.dataframe.api.move]`  {  `[colsOf][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl.colsOf]`<`[Double][Double]`>() }`
     *
     *
     * #### NOTE: There's also a 'single column' variant used sometimes: [Column Selection DSL][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.DslSingle.WithExample].
     * ### 2. [Column names][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.ColumnNames.WithExample]
     * Select columns using their [column names][String]
     * ([String API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.StringApi]).
     *
     * #### For example:
     *
     * `df.`[move][org.jetbrains.kotlinx.dataframe.api.move]`("length", "age")`
     *
     * ### 3. [Column references][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.ColumnAccessors.WithExample]
     * Select columns using [column accessors][org.jetbrains.kotlinx.dataframe.columns.ColumnReference]
     * ([Column Accessors API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.ColumnAccessorsApi]).
     *
     * #### For example:
     *
     * `val length by `[column][org.jetbrains.kotlinx.dataframe.api.column]`<`[Double][Double]`>()`
     *
     * `val age by `[column][org.jetbrains.kotlinx.dataframe.api.column]`<`[Double][Double]`>()`
     *
     * `df.`[move][org.jetbrains.kotlinx.dataframe.api.move]`(length, age)`
     *
     * ### 4. [KProperties][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.KProperties.WithExample]
     * Select columns using [KProperties][KProperty] ([KProperties API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.KPropertiesApi]).
     *
     * #### For example:
     * ```kotlin
     * data class Person(val length: Double, val age: Double)
     * ```
     *
     * `df.`[move][org.jetbrains.kotlinx.dataframe.api.move]`(Person::length, Person::age)`
     *
     */
    interface MoveSelectingOptions

    /**
     * ## Move Operation Grammar
     *
     * &nbsp;&nbsp;&nbsp;&nbsp;
     *
     * [(What is this notation?)][org.jetbrains.kotlinx.dataframe.documentation.DslGrammar]
     *
     * &nbsp;&nbsp;&nbsp;&nbsp;
     *
     *
     * **[`move`][move]****`  {  `**[`columns`][SelectingColumns]**` }`**
     *
     * &nbsp;&nbsp;&nbsp;&nbsp;
     * __`.`__[**`into`**][MoveClause.into]**`  {  `**[`pathSelector`][SelectingColumns]**`  }  `**
     *
     * &nbsp;&nbsp;&nbsp;&nbsp;
     * `| `__`.`__[**`intoIndexed`**][MoveClause.intoIndexed]**`  {  `**[`pathSelector`][SelectingColumns]**` }`**
     *
     * &nbsp;&nbsp;&nbsp;&nbsp;
     * `| `__`.`__[**`under`**][MoveClause.under]**`  {  `**[`parentColumn`][SelectingColumns]**` }`**
     *
     * &nbsp;&nbsp;&nbsp;&nbsp;
     * `| `__`.`__[**`after`**][MoveClause.after]**`  {  `**[`column`][SelectingColumns]**` }`**
     *
     * &nbsp;&nbsp;&nbsp;&nbsp;
     * `| `__`.`__[**`to`**][MoveClause.to]**`(position)`**
     *
     * &nbsp;&nbsp;&nbsp;&nbsp;
     * `| `__`.`__[**`toTop`**][MoveClause.toTop]**`()`**
     *
     * &nbsp;&nbsp;&nbsp;&nbsp;
     * `| `__`.`__[**`toStart`**][MoveClause.toStart]**`()`**
     *
     * &nbsp;&nbsp;&nbsp;&nbsp;
     * `| `__`.`__[**`toEnd`**][MoveClause.toEnd]**`()`**
     */
    interface Grammar
}

/**
 * ## The Move Operation
 *
 * Moves the specified [columns][org.jetbrains.kotlinx.dataframe.columns] within the [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame].
 *
 * This function does not immediately move the columns but instead select columns to move and
 * returns a [MoveClause][org.jetbrains.kotlinx.dataframe.api.MoveClause],
 * which serves as an intermediate step. The [MoveClause][org.jetbrains.kotlinx.dataframe.api.MoveClause] allows specifying the final
 * destination of the selected columns using methods such as [MoveClause.to][org.jetbrains.kotlinx.dataframe.api.MoveClause.to], [MoveClause.toStart][org.jetbrains.kotlinx.dataframe.api.MoveClause.toStart],
 * [MoveClause.toEnd][org.jetbrains.kotlinx.dataframe.api.MoveClause.toEnd], [MoveClause.into][org.jetbrains.kotlinx.dataframe.api.MoveClause.into], [MoveClause.intoIndexed][org.jetbrains.kotlinx.dataframe.api.MoveClause.intoIndexed], [MoveClause.toTop][org.jetbrains.kotlinx.dataframe.api.MoveClause.toTop],
 * [MoveClause.after][org.jetbrains.kotlinx.dataframe.api.MoveClause.after] or [MoveClause.under][org.jetbrains.kotlinx.dataframe.api.MoveClause.under], that return a new [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] with updated columns order.
 * Check out [Grammar][org.jetbrains.kotlinx.dataframe.api.Move.Grammar].
 *
 * This can include [column groups][org.jetbrains.kotlinx.dataframe.columns.ColumnGroup] and nested columns.
 *
 * See [Selecting Columns][org.jetbrains.kotlinx.dataframe.api.Move.MoveSelectingOptions].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 * ### This Move Overload
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
 * ### Examples:
 * ```
 * df.move { columnA and columnB }.after { columnC }
 * df.move { cols(0..3) }.under("info")
 * df.move { colsOf<String>() }.to(5)
 * ```
 * @param [columns] The [Columns Selector][ColumnsSelector] used to select the columns of this [DataFrame] to move.
 */
@Interpretable("Move0")
public fun <T, C> DataFrame<T>.move(columns: ColumnsSelector<T, C>): MoveClause<T, C> = MoveClause(this, columns)

/**
 * ## The Move Operation
 *
 * Moves the specified [columns][org.jetbrains.kotlinx.dataframe.columns] within the [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame].
 *
 * This function does not immediately move the columns but instead select columns to move and
 * returns a [MoveClause][org.jetbrains.kotlinx.dataframe.api.MoveClause],
 * which serves as an intermediate step. The [MoveClause][org.jetbrains.kotlinx.dataframe.api.MoveClause] allows specifying the final
 * destination of the selected columns using methods such as [MoveClause.to][org.jetbrains.kotlinx.dataframe.api.MoveClause.to], [MoveClause.toStart][org.jetbrains.kotlinx.dataframe.api.MoveClause.toStart],
 * [MoveClause.toEnd][org.jetbrains.kotlinx.dataframe.api.MoveClause.toEnd], [MoveClause.into][org.jetbrains.kotlinx.dataframe.api.MoveClause.into], [MoveClause.intoIndexed][org.jetbrains.kotlinx.dataframe.api.MoveClause.intoIndexed], [MoveClause.toTop][org.jetbrains.kotlinx.dataframe.api.MoveClause.toTop],
 * [MoveClause.after][org.jetbrains.kotlinx.dataframe.api.MoveClause.after] or [MoveClause.under][org.jetbrains.kotlinx.dataframe.api.MoveClause.under], that return a new [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] with updated columns order.
 * Check out [Grammar][org.jetbrains.kotlinx.dataframe.api.Move.Grammar].
 *
 * This can include [column groups][org.jetbrains.kotlinx.dataframe.columns.ColumnGroup] and nested columns.
 *
 * See [Selecting Columns][org.jetbrains.kotlinx.dataframe.api.Move.MoveSelectingOptions].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 * ### This Move Overload
 * Select columns using their [column names][String]
 * ([String API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.StringApi]).
 * ### Examples:
 * ```
 * df.move("columnA", "columnB").after("columnC")
 * df.move("age").under("info")
 * ```
 * @param [columns] The [Column Names][String] used to select the columns of this [DataFrame] to move.
 */
public fun <T> DataFrame<T>.move(vararg columns: String): MoveClause<T, Any?> = move { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C> DataFrame<T>.move(vararg columns: ColumnReference<C>): MoveClause<T, C> =
    move { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C> DataFrame<T>.move(vararg columns: KProperty<C>): MoveClause<T, C> = move { columns.toColumnSet() }

// endregion

// region moveTo

/**
 * ## The MoveTo Operation
 *
 * Moves the specified [columns] to a new position specified by
 * [newColumnIndex] within the [DataFrame].
 *
 * Returns a new [DataFrame] with updated columns order.
 *
 * This can include [column groups][org.jetbrains.kotlinx.dataframe.columns.ColumnGroup] and nested columns.
 *
 * See [Selecting Columns][MoveToSelectingOptions].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 */
internal interface MoveTo {
    /**
     *
     * ## Selecting Columns
     * Selecting columns for various operations (including but not limited to
     * [DataFrame.select][org.jetbrains.kotlinx.dataframe.DataFrame.select], [DataFrame.update][org.jetbrains.kotlinx.dataframe.DataFrame.update], [DataFrame.gather][org.jetbrains.kotlinx.dataframe.DataFrame.gather], and [DataFrame.fillNulls][org.jetbrains.kotlinx.dataframe.DataFrame.fillNulls])
     * can be done in the following ways:
     * ### 1. [Columns Selection DSL][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.Dsl.WithExample]
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
     * `df.`[moveTo][org.jetbrains.kotlinx.dataframe.api.moveTo]` { length `[and][org.jetbrains.kotlinx.dataframe.api.AndColumnsSelectionDsl.and]` age }`
     *
     * `df.`[moveTo][org.jetbrains.kotlinx.dataframe.api.moveTo]`  {  `[cols][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl.cols]`(1..5) }`
     *
     * `df.`[moveTo][org.jetbrains.kotlinx.dataframe.api.moveTo]`  {  `[colsOf][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl.colsOf]`<`[Double][Double]`>() }`
     *
     *
     * #### NOTE: There's also a 'single column' variant used sometimes: [Column Selection DSL][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.DslSingle.WithExample].
     * ### 2. [Column names][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.ColumnNames.WithExample]
     * Select columns using their [column names][String]
     * ([String API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.StringApi]).
     *
     * #### For example:
     *
     * `df.`[moveTo][org.jetbrains.kotlinx.dataframe.api.moveTo]`("length", "age")`
     *
     * ### 3. [Column references][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.ColumnAccessors.WithExample]
     * Select columns using [column accessors][org.jetbrains.kotlinx.dataframe.columns.ColumnReference]
     * ([Column Accessors API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.ColumnAccessorsApi]).
     *
     * #### For example:
     *
     * `val length by `[column][org.jetbrains.kotlinx.dataframe.api.column]`<`[Double][Double]`>()`
     *
     * `val age by `[column][org.jetbrains.kotlinx.dataframe.api.column]`<`[Double][Double]`>()`
     *
     * `df.`[moveTo][org.jetbrains.kotlinx.dataframe.api.moveTo]`(length, age)`
     *
     * ### 4. [KProperties][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.KProperties.WithExample]
     * Select columns using [KProperties][KProperty] ([KProperties API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.KPropertiesApi]).
     *
     * #### For example:
     * ```kotlin
     * data class Person(val length: Double, val age: Double)
     * ```
     *
     * `df.`[moveTo][org.jetbrains.kotlinx.dataframe.api.moveTo]`(Person::length, Person::age)`
     *
     */
    interface MoveToSelectingOptions
}

/**
 * ## The MoveTo Operation
 *
 * Moves the specified [columns][org.jetbrains.kotlinx.dataframe.columns] to a new position specified by
 * [newColumnIndex] within the [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame].
 *
 * Returns a new [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] with updated columns order.
 *
 * This can include [column groups][org.jetbrains.kotlinx.dataframe.columns.ColumnGroup] and nested columns.
 *
 * See [Selecting Columns][org.jetbrains.kotlinx.dataframe.api.MoveTo.MoveToSelectingOptions].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 * ### This MoveTo Overload
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
 * ### Examples:
 * ```
 * df.moveTo(0) { length and age }
 * df.moveTo(2) { cols(1..5) }
 * ```
 * @param [newColumnIndex] The index specifying the position in the [DataFrame] columns
 * where the selected columns will be moved.
 * @param [columns] The [Columns Selector][ColumnsSelector] used to select the columns of this [DataFrame] to move.
 */
public fun <T> DataFrame<T>.moveTo(newColumnIndex: Int, columns: ColumnsSelector<T, *>): DataFrame<T> =
    move(columns).to(newColumnIndex)

/**
 * ## The MoveTo Operation
 *
 * Moves the specified [columns][org.jetbrains.kotlinx.dataframe.columns] to a new position specified by
 * [newColumnIndex] within the [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame].
 *
 * Returns a new [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] with updated columns order.
 *
 * This can include [column groups][org.jetbrains.kotlinx.dataframe.columns.ColumnGroup] and nested columns.
 *
 * See [Selecting Columns][org.jetbrains.kotlinx.dataframe.api.MoveTo.MoveToSelectingOptions].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 * ### This MoveTo Overload
 * Select columns using their [column names][String]
 * ([String API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.StringApi]).
 * ### Examples:
 * ```
 * df.moveTo(0) { length and age }
 * df.moveTo(2) { cols(1..5) }
 * ```
 * @param [newColumnIndex] The index specifying the position in the [DataFrame] columns
 * where the selected columns will be moved.
 * @param [columns] The [Columns Selector][ColumnsSelector] used to select the columns of this [DataFrame] to move.
 */
public fun <T> DataFrame<T>.moveTo(newColumnIndex: Int, vararg columns: String): DataFrame<T> =
    moveTo(newColumnIndex) { columns.toColumnSet() }

public fun <T> DataFrame<T>.moveTo(newColumnIndex: Int, vararg columns: AnyColumnReference): DataFrame<T> =
    moveTo(newColumnIndex) { columns.toColumnSet() }

@AccessApiOverload
public fun <T> DataFrame<T>.moveTo(newColumnIndex: Int, vararg columns: KProperty<*>): DataFrame<T> =
    moveTo(newColumnIndex) { columns.toColumnSet() }

// endregion

// region moveToStart

/**
 * ## The MoveToStart Operation
 *
 * Moves the specified [columns] to the [DataFrame] start.
 * Returns a new [DataFrame] with updated columns order.
 *
 * This can include [column groups][org.jetbrains.kotlinx.dataframe.columns.ColumnGroup] and nested columns.
 *
 * See [Selecting Columns][MoveToStartSelectingOptions].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 */
internal interface MoveToStart {
    /**
     *
     * ## Selecting Columns
     * Selecting columns for various operations (including but not limited to
     * [DataFrame.select][org.jetbrains.kotlinx.dataframe.DataFrame.select], [DataFrame.update][org.jetbrains.kotlinx.dataframe.DataFrame.update], [DataFrame.gather][org.jetbrains.kotlinx.dataframe.DataFrame.gather], and [DataFrame.fillNulls][org.jetbrains.kotlinx.dataframe.DataFrame.fillNulls])
     * can be done in the following ways:
     * ### 1. [Columns Selection DSL][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.Dsl.WithExample]
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
     * `df.`[moveToStart][org.jetbrains.kotlinx.dataframe.api.moveToStart]` { length `[and][org.jetbrains.kotlinx.dataframe.api.AndColumnsSelectionDsl.and]` age }`
     *
     * `df.`[moveToStart][org.jetbrains.kotlinx.dataframe.api.moveToStart]`  {  `[cols][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl.cols]`(1..5) }`
     *
     * `df.`[moveToStart][org.jetbrains.kotlinx.dataframe.api.moveToStart]`  {  `[colsOf][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl.colsOf]`<`[Double][Double]`>() }`
     *
     *
     * #### NOTE: There's also a 'single column' variant used sometimes: [Column Selection DSL][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.DslSingle.WithExample].
     * ### 2. [Column names][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.ColumnNames.WithExample]
     * Select columns using their [column names][String]
     * ([String API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.StringApi]).
     *
     * #### For example:
     *
     * `df.`[moveToStart][org.jetbrains.kotlinx.dataframe.api.moveToStart]`("length", "age")`
     *
     * ### 3. [Column references][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.ColumnAccessors.WithExample]
     * Select columns using [column accessors][org.jetbrains.kotlinx.dataframe.columns.ColumnReference]
     * ([Column Accessors API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.ColumnAccessorsApi]).
     *
     * #### For example:
     *
     * `val length by `[column][org.jetbrains.kotlinx.dataframe.api.column]`<`[Double][Double]`>()`
     *
     * `val age by `[column][org.jetbrains.kotlinx.dataframe.api.column]`<`[Double][Double]`>()`
     *
     * `df.`[moveToStart][org.jetbrains.kotlinx.dataframe.api.moveToStart]`(length, age)`
     *
     * ### 4. [KProperties][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.KProperties.WithExample]
     * Select columns using [KProperties][KProperty] ([KProperties API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.KPropertiesApi]).
     *
     * #### For example:
     * ```kotlin
     * data class Person(val length: Double, val age: Double)
     * ```
     *
     * `df.`[moveToStart][org.jetbrains.kotlinx.dataframe.api.moveToStart]`(Person::length, Person::age)`
     *
     */
    interface MoveToStartSelectingOptions
}

@Deprecated(MOVE_TO_LEFT, ReplaceWith(MOVE_TO_LEFT_REPLACE), DeprecationLevel.ERROR)
public fun <T> DataFrame<T>.moveToLeft(columns: ColumnsSelector<T, *>): DataFrame<T> = move(columns).toStart()

/**
 * ## The MoveToStart Operation
 *
 * Moves the specified [columns][org.jetbrains.kotlinx.dataframe.columns] to the [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] start.
 * Returns a new [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] with updated columns order.
 *
 * This can include [column groups][org.jetbrains.kotlinx.dataframe.columns.ColumnGroup] and nested columns.
 *
 * See [Selecting Columns][org.jetbrains.kotlinx.dataframe.api.MoveToStart.MoveToStartSelectingOptions].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 * ### This MoveToStart Overload
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
 * `df.`[moveToStart][org.jetbrains.kotlinx.dataframe.api.moveToStart]` { length `[and][org.jetbrains.kotlinx.dataframe.api.AndColumnsSelectionDsl.and]` age }`
 *
 * `df.`[moveToStart][org.jetbrains.kotlinx.dataframe.api.moveToStart]`  {  `[cols][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl.cols]`(1..5) }`
 *
 * `df.`[moveToStart][org.jetbrains.kotlinx.dataframe.api.moveToStart]`  {  `[colsOf][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl.colsOf]`<`[Double][Double]`>() }`
 *
 *
 * @param [columns] The [Columns Selector][ColumnsSelector] used to select the columns of this [DataFrame] to move.
 */
@Refine
@Interpretable("MoveToStart1")
public fun <T> DataFrame<T>.moveToStart(columns: ColumnsSelector<T, *>): DataFrame<T> = move(columns).toStart()

@Deprecated(MOVE_TO_LEFT, ReplaceWith(MOVE_TO_LEFT_REPLACE), DeprecationLevel.ERROR)
public fun <T> DataFrame<T>.moveToLeft(vararg columns: String): DataFrame<T> = moveToStart { columns.toColumnSet() }

/**
 * ## The MoveToStart Operation
 *
 * Moves the specified [columns][org.jetbrains.kotlinx.dataframe.columns] to the [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] start.
 * Returns a new [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] with updated columns order.
 *
 * This can include [column groups][org.jetbrains.kotlinx.dataframe.columns.ColumnGroup] and nested columns.
 *
 * See [Selecting Columns][org.jetbrains.kotlinx.dataframe.api.MoveToStart.MoveToStartSelectingOptions].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 * ### This MoveToStart Overload
 * Select columns using their [column names][String]
 * ([String API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.StringApi]).
 *
 * #### For example:
 *
 * `df.`[moveToStart][org.jetbrains.kotlinx.dataframe.api.moveToStart]`("length", "age")`
 *
 * @param [columns] The [Columns Selector][ColumnsSelector] used to select the columns of this [DataFrame] to move.
 */
public fun <T> DataFrame<T>.moveToStart(vararg columns: String): DataFrame<T> = moveToStart { columns.toColumnSet() }

@Deprecated(MOVE_TO_LEFT, ReplaceWith(MOVE_TO_LEFT_REPLACE), DeprecationLevel.ERROR)
@AccessApiOverload
public fun <T> DataFrame<T>.moveToLeft(vararg columns: AnyColumnReference): DataFrame<T> =
    moveToStart { columns.toColumnSet() }

@AccessApiOverload
public fun <T> DataFrame<T>.moveToStart(vararg columns: AnyColumnReference): DataFrame<T> =
    moveToStart { columns.toColumnSet() }

@Deprecated(MOVE_TO_LEFT, ReplaceWith(MOVE_TO_LEFT_REPLACE), DeprecationLevel.ERROR)
@AccessApiOverload
public fun <T> DataFrame<T>.moveToLeft(vararg columns: KProperty<*>): DataFrame<T> =
    moveToStart { columns.toColumnSet() }

@AccessApiOverload
public fun <T> DataFrame<T>.moveToStart(vararg columns: KProperty<*>): DataFrame<T> =
    moveToStart { columns.toColumnSet() }

// endregion

// region moveToEnd

/**
 * ## The MoveToEnd Operation
 *
 * Moves the specified [columns] to the [DataFrame] end.
 * Returns a new [DataFrame] with updated columns order.
 *
 * This can include [column groups][org.jetbrains.kotlinx.dataframe.columns.ColumnGroup] and nested columns.
 *
 * See [Selecting Columns][MoveToEndSelectingOptions].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 */
internal interface MoveToEnd {
    /**
     *
     * ## Selecting Columns
     * Selecting columns for various operations (including but not limited to
     * [DataFrame.select][org.jetbrains.kotlinx.dataframe.DataFrame.select], [DataFrame.update][org.jetbrains.kotlinx.dataframe.DataFrame.update], [DataFrame.gather][org.jetbrains.kotlinx.dataframe.DataFrame.gather], and [DataFrame.fillNulls][org.jetbrains.kotlinx.dataframe.DataFrame.fillNulls])
     * can be done in the following ways:
     * ### 1. [Columns Selection DSL][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.Dsl.WithExample]
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
     * `df.`[moveToEnd][org.jetbrains.kotlinx.dataframe.api.moveToEnd]` { length `[and][org.jetbrains.kotlinx.dataframe.api.AndColumnsSelectionDsl.and]` age }`
     *
     * `df.`[moveToEnd][org.jetbrains.kotlinx.dataframe.api.moveToEnd]`  {  `[cols][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl.cols]`(1..5) }`
     *
     * `df.`[moveToEnd][org.jetbrains.kotlinx.dataframe.api.moveToEnd]`  {  `[colsOf][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl.colsOf]`<`[Double][Double]`>() }`
     *
     *
     * #### NOTE: There's also a 'single column' variant used sometimes: [Column Selection DSL][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.DslSingle.WithExample].
     * ### 2. [Column names][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.ColumnNames.WithExample]
     * Select columns using their [column names][String]
     * ([String API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.StringApi]).
     *
     * #### For example:
     *
     * `df.`[moveToEnd][org.jetbrains.kotlinx.dataframe.api.moveToEnd]`("length", "age")`
     *
     * ### 3. [Column references][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.ColumnAccessors.WithExample]
     * Select columns using [column accessors][org.jetbrains.kotlinx.dataframe.columns.ColumnReference]
     * ([Column Accessors API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.ColumnAccessorsApi]).
     *
     * #### For example:
     *
     * `val length by `[column][org.jetbrains.kotlinx.dataframe.api.column]`<`[Double][Double]`>()`
     *
     * `val age by `[column][org.jetbrains.kotlinx.dataframe.api.column]`<`[Double][Double]`>()`
     *
     * `df.`[moveToEnd][org.jetbrains.kotlinx.dataframe.api.moveToEnd]`(length, age)`
     *
     * ### 4. [KProperties][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.KProperties.WithExample]
     * Select columns using [KProperties][KProperty] ([KProperties API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.KPropertiesApi]).
     *
     * #### For example:
     * ```kotlin
     * data class Person(val length: Double, val age: Double)
     * ```
     *
     * `df.`[moveToEnd][org.jetbrains.kotlinx.dataframe.api.moveToEnd]`(Person::length, Person::age)`
     *
     */
    interface MoveToEndSelectingOptions
}

@Deprecated(MOVE_TO_RIGHT, ReplaceWith(MOVE_TO_RIGHT_REPLACE), DeprecationLevel.ERROR)
public fun <T> DataFrame<T>.moveToRight(columns: ColumnsSelector<T, *>): DataFrame<T> = move(columns).toEnd()

/**
 * ## The MoveToEnd Operation
 *
 * Moves the specified [columns][org.jetbrains.kotlinx.dataframe.columns] to the [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] end.
 * Returns a new [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] with updated columns order.
 *
 * This can include [column groups][org.jetbrains.kotlinx.dataframe.columns.ColumnGroup] and nested columns.
 *
 * See [Selecting Columns][org.jetbrains.kotlinx.dataframe.api.MoveToEnd.MoveToEndSelectingOptions].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 * ### This MoveToEnd Overload
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
 * `df.`[moveToEnd][org.jetbrains.kotlinx.dataframe.api.moveToEnd]` { length `[and][org.jetbrains.kotlinx.dataframe.api.AndColumnsSelectionDsl.and]` age }`
 *
 * `df.`[moveToEnd][org.jetbrains.kotlinx.dataframe.api.moveToEnd]`  {  `[cols][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl.cols]`(1..5) }`
 *
 * `df.`[moveToEnd][org.jetbrains.kotlinx.dataframe.api.moveToEnd]`  {  `[colsOf][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl.colsOf]`<`[Double][Double]`>() }`
 *
 *
 * @param [columns] The [Columns Selector][ColumnsSelector] used to select the columns of this [DataFrame] to move.
 */
@Refine
@Interpretable("MoveToEnd1")
public fun <T> DataFrame<T>.moveToEnd(columns: ColumnsSelector<T, *>): DataFrame<T> = move(columns).toEnd()

@Deprecated(MOVE_TO_RIGHT, ReplaceWith(MOVE_TO_RIGHT_REPLACE), DeprecationLevel.ERROR)
public fun <T> DataFrame<T>.moveToRight(vararg columns: String): DataFrame<T> = moveToEnd { columns.toColumnSet() }

/**
 * ## The MoveToEnd Operation
 *
 * Moves the specified [columns][org.jetbrains.kotlinx.dataframe.columns] to the [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] end.
 * Returns a new [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] with updated columns order.
 *
 * This can include [column groups][org.jetbrains.kotlinx.dataframe.columns.ColumnGroup] and nested columns.
 *
 * See [Selecting Columns][org.jetbrains.kotlinx.dataframe.api.MoveToEnd.MoveToEndSelectingOptions].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 * ### This MoveToEnd Overload
 * Select columns using their [column names][String]
 * ([String API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.StringApi]).
 *
 * #### For example:
 *
 * `df.`[moveToEnd][org.jetbrains.kotlinx.dataframe.api.moveToEnd]`("length", "age")`
 *
 * @param [columns] The [Columns Selector][ColumnsSelector] used to select the columns of this [DataFrame] to move.
 */
public fun <T> DataFrame<T>.moveToEnd(vararg columns: String): DataFrame<T> = moveToEnd { columns.toColumnSet() }

@Deprecated(MOVE_TO_RIGHT, ReplaceWith(MOVE_TO_RIGHT_REPLACE), DeprecationLevel.ERROR)
@AccessApiOverload
public fun <T> DataFrame<T>.moveToRight(vararg columns: AnyColumnReference): DataFrame<T> =
    moveToEnd { columns.toColumnSet() }

@AccessApiOverload
public fun <T> DataFrame<T>.moveToEnd(vararg columns: AnyColumnReference): DataFrame<T> =
    moveToEnd { columns.toColumnSet() }

@Deprecated(MOVE_TO_RIGHT, ReplaceWith(MOVE_TO_RIGHT_REPLACE), DeprecationLevel.ERROR)
@AccessApiOverload
public fun <T> DataFrame<T>.moveToRight(vararg columns: KProperty<*>): DataFrame<T> =
    moveToEnd { columns.toColumnSet() }

@AccessApiOverload
public fun <T> DataFrame<T>.moveToEnd(vararg columns: KProperty<*>): DataFrame<T> = moveToEnd { columns.toColumnSet() }

// endregion

// endregion

// region MoveClause

// region into

/**
 * ## Move Into
 *
 * Moves columns, previously selected with [move] into a new position specified with a
 * given column path within the [DataFrame].
 * If there are non-existent column groups on this path, they will be created.
 *
 * See [Selecting Columns][SelectingColumns].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 *
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
 * ### Examples:
 * ```
 * df.move { age and weight }.into { pathOf("info", it.name()) }
 * df.move { age and weight }.into { "info"[it.name()] }
 * df.move { name.firstName and name.lastName }.into { pathOf("fullName", it.name().dropLast(4)) }
 * ```
 *
 * @param [column] The [Column With Path Selector][ColumnsSelector] used to specify
 * a path in the [DataFrame] to move columns.
 */
public fun <T, C> MoveClause<T, C>.into(
    column: ColumnsSelectionDsl<T>.(ColumnWithPath<C>) -> AnyColumnReference,
): DataFrame<T> =
    moveImpl(
        under = false,
        newPathExpression = column,
    )

/**
 * Move a single selected column to top level with a new name
 */
@CandidateForRemoval
@Refine
@Interpretable("MoveInto0")
public fun <T, C> MoveClause<T, C>.into(column: String): DataFrame<T> = pathOf(column).let { path -> into { path } }

/**
 * ## Move IntoIndexed
 *
 * Moves columns, previously selected with [move] into a new position specified with a
 * given column path within the [DataFrame].
 * Provides selected column indices.
 * If there are non-existent column groups on this path, they will be created.
 *
 * See [Selecting Columns][SelectingColumns].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 *
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
 * ### Examples:
 * ```
 * df.move { cols { it.name == "user" } }
 *    .intoIndexed { it, index -> "allUsers"["user$index"] }
 * ```
 *
 * @param [column] The [Column With Path Selector And Indices][ColumnsSelector] used to specify
 * a path in the [DataFrame] to move columns.
 */
public fun <T, C> MoveClause<T, C>.intoIndexed(
    newPathExpression: ColumnsSelectionDsl<T>.(ColumnWithPath<C>, Int) -> AnyColumnReference,
): DataFrame<T> {
    var counter = 0
    return into { col ->
        newPathExpression(this, col, counter++)
    }
}

// endregion

// region under

/**
 * ## Move Under
 *
 * Moves columns, previously selected with [move] under a new or
 * an existing column group within the [DataFrame].
 * If the column group doesn't exist, it will be created.
 *
 * See [Selecting Columns][SelectingColumns].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 *
 * Select columns using their [column names][String]
 * ([String API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.StringApi]).
 *
 * ### Examples:
 * ```
 * df.move("age", "weight").under("info")
 * df.move { age and weight }.under("info")
 * ```
 *
 * @param [column] A [ColumnsSelector] that defines the path to a [ColumnGroup]
 * in the [DataFrame], where the selected columns will be moved.
 */
@Refine
@Interpretable("MoveUnder0")
public fun <T, C> MoveClause<T, C>.under(column: String): DataFrame<T> = pathOf(column).let { path -> under { path } }

@AccessApiOverload
public fun <T, C> MoveClause<T, C>.under(column: AnyColumnGroupAccessor): DataFrame<T> =
    column.path().let { path -> under { path } }

/**
 * ## Move Under
 *
 * Moves columns, previously selected with [move] under a new or
 * an existing column group specified with a
 * column path within the [DataFrame].
 * If there are non-existent column groups on this path, they will be created.
 *
 * See [Selecting Columns][SelectingColumns].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 *
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
 * ### Examples:
 * ```
 * // move under an existing column group
 * df.move { age and weight }.under { info }
 * // move under a new column group
 * df.move { age and weight }.under { columnGroup(info) }
 * ```
 *
 * @param [column] The [ColumnsSelector] that defines the path to a [ColumnGroup]
 * in the [DataFrame], where the selected columns will be moved.
 */
@Refine
@Interpretable("MoveUnder1")
public fun <T, C> MoveClause<T, C>.under(
    column: ColumnsSelectionDsl<T>.(ColumnWithPath<C>) -> AnyColumnReference,
): DataFrame<T> =
    moveImpl(
        under = true,
        newPathExpression = column,
    )

// endregion

// region to

/**
 * ## Move To
 *
 * Moves columns, previously selected with [move] to a new position specified
 * by [columnIndex] within the [DataFrame].
 *
 * Returns a new [DataFrame] with updated columns order.
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 *
 * ### Examples:
 * ```
 * df.move { age and weight }.to(0)
 * df.move("age", "weight").to(2)
 * ```
 *
 * @param [columnIndex] The index specifying the position in the [DataFrame] columns
 *  * where the selected columns will be moved.
 */
public fun <T, C> MoveClause<T, C>.to(columnIndex: Int): DataFrame<T> = moveTo(columnIndex)

/**
 * ## Move ToTop
 *
 * Moves columns, previously selected with [move] to the top-level within the [DataFrame].
 * Moved columns name can be specified via special ColumnSelectionDsl.
 *
 * Returns a new [DataFrame] with updated columns.
 *
 * See [Selecting Columns][SelectingColumns].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 *
 * ### Examples:
 * ```
 * df.move { info.age and info.weight }.toTop()
 * df.move { colsAtAnyDepth { it.name() == "number" } }.toTop { it.parentName + it.name() }
 * ```
 *
 * @param [newColumnName] The special [ColumnsSelector] for define name of moved column.
 * Optional, the original name is used by default
 */
@Refine
@Interpretable("ToTop")
public fun <T, C> MoveClause<T, C>.toTop(
    newColumnName: ColumnsSelectionDsl<T>.(ColumnWithPath<C>) -> String = { it.name() },
): DataFrame<T> = into { newColumnName(it).toColumnAccessor() }

// endregion

// region after

/**
 * ## Move After
 *
 * Moves columns, previously selected with [move] to the position after the
 * specified [column] within the [DataFrame].
 *
 * Returns a new [DataFrame] with updated columns.
 *
 * See [Selecting Columns][SelectingColumns].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 *
 * ### This After Overload
 */
internal interface MoveAfter

/**
 * ## Move After
 *
 * Moves columns, previously selected with [move][org.jetbrains.kotlinx.dataframe.api.move] to the position after the
 * specified [column][org.jetbrains.kotlinx.dataframe.api.column] within the [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame].
 *
 * Returns a new [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] with updated columns.
 *
 * See [Selecting Columns][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 *
 * ### This After Overload
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
 * ### Examples:
 * ```
 * df.move { age and weight }.after { surname }
 * df.move { cols(0..2) }.after { col(3) }
 * ```
 *
 * @param [column] A [ColumnSelector] specifying the column
 * after which the selected columns will be placed.
 */
@Refine
@Interpretable("MoveAfter0")
public fun <T, C> MoveClause<T, C>.after(column: ColumnSelector<T, *>): DataFrame<T> = afterOrBefore(column, true)

/**
 * ## Move After
 *
 * Moves columns, previously selected with [move][org.jetbrains.kotlinx.dataframe.api.move] to the position after the
 * specified [column][org.jetbrains.kotlinx.dataframe.api.column] within the [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame].
 *
 * Returns a new [DataFrame][org.jetbrains.kotlinx.dataframe.DataFrame] with updated columns.
 *
 * See [Selecting Columns][org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns].
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 *
 * ### This After Overload
 * Select columns using their [column names][String]
 * ([String API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.StringApi]).
 *
 * ### Examples:
 * ```
 * df.move("age", "weight").after("surname")
 * ```
 * @param [column] The [Column Name][String] specifying the column
 * after which the selected columns will be placed.
 */
public fun <T, C> MoveClause<T, C>.after(column: String): DataFrame<T> = after { column.toColumnAccessor() }

@AccessApiOverload
public fun <T, C> MoveClause<T, C>.after(column: AnyColumnReference): DataFrame<T> = after { column }

@AccessApiOverload
public fun <T, C> MoveClause<T, C>.after(column: KProperty<*>): DataFrame<T> = after { column.toColumnAccessor() }

// endregion

/* TODO: implement 'before'
fun <T, C> MoveColsClause<T, C>.before(columnPath: ColumnPath) = before { columnPath.toColumnDef() }
fun <T, C> MoveColsClause<T, C>.before(column: Column) = before { column }
fun <T, C> MoveColsClause<T, C>.before(column: KProperty<*>) = before { column.toColumnDef() }
fun <T, C> MoveColsClause<T, C>.before(column: String) = before { column.toColumnDef() }
fun <T, C> MoveColsClause<T, C>.before(column: ColumnSelector<T, *>) = afterOrBefore(column, false)
*/

@Deprecated(TO_LEFT, ReplaceWith(TO_LEFT_REPLACE), DeprecationLevel.ERROR)
public fun <T, C> MoveClause<T, C>.toLeft(): DataFrame<T> = to(0)

/**
 * ## Move ToStart
 *
 * Moves columns, previously selected with [move] to the [DataFrame] start.
 *
 * Returns a new [DataFrame] with updated columns.
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 *
 * ### Examples:
 * ```
 * df.move { age and weight }.toStart()
 * df.move { colsOf<String>() }.toStart()
 * df.move("age", "weight").toStart()
 * ```
 */
@Refine
@Interpretable("MoveToStart0")
public fun <T, C> MoveClause<T, C>.toStart(): DataFrame<T> = to(0)

@Deprecated(TO_RIGHT, ReplaceWith(TO_RIGHT_REPLACE), DeprecationLevel.ERROR)
public fun <T, C> MoveClause<T, C>.toRight(): DataFrame<T> = to(df.ncol)

/**
 * ## Move ToEnd
 *
 * Moves columns, previously selected with [move] to the [DataFrame] end.
 *
 * Returns a new [DataFrame] with updated columns.
 *
 * For more information: [See `move` on the documentation website.](https://kotlin.github.io/dataframe/move.html)
 *
 * ### Examples:
 * ```
 * df.move { age and weight }.toEnd()
 * df.move { colsOf<String>() }.toEnd()
 * df.move("age", "weight").toEnd()
 * ```
 */
@Refine
@Interpretable("MoveToEnd0")
public fun <T, C> MoveClause<T, C>.toEnd(): DataFrame<T> = to(df.ncol)

/**
 * An intermediate class used in the [move] operation.
 *
 * This class itself does nothing—it is just a transitional step before specifying
 * where to move the selected columns.
 * It must be followed by one of the positioning methods
 * to produce a new [DataFrame] with the updated column order.
 *
 * Use the following methods to finalize the move:
 * - [to] – moves columns to a specific index.
 * - [toStart] – moves columns to the beginning.
 * - [toEnd] – moves columns to the end.
 * - [into] / [intoIndexed] – moves columns to a new position.
 * - [toTop] – moves columns to the top-level.
 * - [after] – places columns after a specific column.
 * - [under] – nests columns under a column group.
 *
 * See [Grammar][Move.Grammar] for more details.
 */
public class MoveClause<T, C>(internal val df: DataFrame<T>, internal val columns: ColumnsSelector<T, C>) {
    override fun toString(): String = "MoveClause(df=$df, columns=$columns)"
}

// endregion
