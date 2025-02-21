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
import org.jetbrains.kotlinx.dataframe.documentation.DocumentationUrls
import org.jetbrains.kotlinx.dataframe.documentation.DslGrammarLink
import org.jetbrains.kotlinx.dataframe.documentation.ExcludeFromSources
import org.jetbrains.kotlinx.dataframe.documentation.Indent
import org.jetbrains.kotlinx.dataframe.documentation.LineBreak
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
 * @include [SelectingColumns.ColumnGroupsAndNestedColumnsMention]
 *
 * See [Selecting Columns][MoveSelectingOptions].
 *
 * For more information: {@include [DocumentationUrls.Move]}
 */
internal interface Move {

    /**
     * {@comment Version of [SelectingColumns] with correctly filled in examples}
     * @include [SelectingColumns] {@include [SetMoveOperationArg]}
     */
    interface MoveSelectingOptions

    /**
     * ## Move Operation Grammar
     * {@include [LineBreak]}
     * {@include [DslGrammarLink]}
     * {@include [LineBreak]}
     *
     * **[`move`][move]****`  {  `**[`columns`][SelectingColumns]**` }`**
     *
     * {@include [Indent]}
     * __`.`__[**`into`**][MoveClause.into]**`  {  `**[`pathSelector`][SelectingColumns]**`  }  `**
     *
     * {@include [Indent]}
     * `| `__`.`__[**`intoIndexed`**][MoveClause.intoIndexed]**`  {  `**[`pathSelector`][SelectingColumns]**` }`**
     *
     * {@include [Indent]}
     * `| `__`.`__[**`under`**][MoveClause.under]**`  {  `**[`parentColumn`][SelectingColumns]**` }`**
     *
     * {@include [Indent]}
     * `| `__`.`__[**`after`**][MoveClause.after]**`  {  `**[`column`][SelectingColumns]**` }`**
     *
     * {@include [Indent]}
     * `| `__`.`__[**`to`**][MoveClause.to]**`(position)`**
     *
     * {@include [Indent]}
     * `| `__`.`__[**`toTop`**][MoveClause.toTop]**`()`**
     *
     * {@include [Indent]}
     * `| `__`.`__[**`toStart`**][MoveClause.toStart]**`()`**
     *
     * {@include [Indent]}
     * `| `__`.`__[**`toEnd`**][MoveClause.toEnd]**`()`**
     */
    interface Grammar
}

/** {@set [SelectingColumns.OPERATION] [move][move]} */
@ExcludeFromSources
private interface SetMoveOperationArg

/**
 * {@include [Move]}
 * ### This Move Overload
 */
@ExcludeFromSources
private interface CommonMoveDocs

/**
 * @include [CommonMoveDocs]
 * @include [SelectingColumns.Dsl] {@include [SetMoveOperationArg]}
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
 * @include [CommonMoveDocs]
 * @include [SelectingColumns.ColumnNames] {@include [SetMoveOperationArg]}
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
 * @include [SelectingColumns.ColumnGroupsAndNestedColumnsMention]
 *
 * See [Selecting Columns][MoveToSelectingOptions].
 *
 * For more information: {@include [DocumentationUrls.Move]}
 */
internal interface MoveTo {
    /**
     * {@comment Version of [SelectingColumns] with correctly filled in examples}
     * @include [SelectingColumns] {@include [SetMoveToOperationArg]}
     */
    interface MoveToSelectingOptions
}

/** {@set [SelectingColumns.OPERATION] [moveTo][moveTo]} */
@ExcludeFromSources
private interface SetMoveToOperationArg

/**
 * {@include [MoveTo]}
 * ### This MoveTo Overload
 */
@ExcludeFromSources
private interface CommonMoveToDocs

/**
 * @include [CommonMoveToDocs]
 * @include [SelectingColumns.Dsl] {@include [SetMoveToOperationArg]}
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
 * @include [CommonMoveToDocs]
 * @include [SelectingColumns.ColumnNames] {@include [SetMoveToOperationArg]}
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
 * @include [SelectingColumns.ColumnGroupsAndNestedColumnsMention]
 *
 * See [Selecting Columns][MoveToStartSelectingOptions].
 *
 * For more information: {@include [DocumentationUrls.Move]}
 */
internal interface MoveToStart {
    /**
     * {@comment Version of [SelectingColumns] with correctly filled in examples}
     * @include [SelectingColumns] {@include [SetMoveToStartOperationArg]}
     */
    interface MoveToStartSelectingOptions
}

/** {@set [SelectingColumns.OPERATION] [moveToStart][moveToStart]} */
@ExcludeFromSources
private interface SetMoveToStartOperationArg

/**
 * {@include [MoveToStart]}
 * ### This MoveToStart Overload
 */
@ExcludeFromSources
private interface CommonMoveToStartDocs

@Deprecated(MOVE_TO_LEFT, ReplaceWith(MOVE_TO_LEFT_REPLACE), DeprecationLevel.ERROR)
public fun <T> DataFrame<T>.moveToLeft(columns: ColumnsSelector<T, *>): DataFrame<T> = move(columns).toStart()

/**
 * @include [CommonMoveToStartDocs]
 * @include [SelectingColumns.Dsl.WithExample] {@include [SetMoveToStartOperationArg]}
 * @param [columns] The [Columns Selector][ColumnsSelector] used to select the columns of this [DataFrame] to move.
 */
@Refine
@Interpretable("MoveToStart1")
public fun <T> DataFrame<T>.moveToStart(columns: ColumnsSelector<T, *>): DataFrame<T> = move(columns).toStart()

@Deprecated(MOVE_TO_LEFT, ReplaceWith(MOVE_TO_LEFT_REPLACE), DeprecationLevel.ERROR)
public fun <T> DataFrame<T>.moveToLeft(vararg columns: String): DataFrame<T> = moveToStart { columns.toColumnSet() }

/**
 * @include [CommonMoveToStartDocs]
 * @include [SelectingColumns.ColumnNames.WithExample] {@include [SetMoveToStartOperationArg]}
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
 * @include [SelectingColumns.ColumnGroupsAndNestedColumnsMention]
 *
 * See [Selecting Columns][MoveToEndSelectingOptions].
 *
 * For more information: {@include [DocumentationUrls.Move]}
 */
internal interface MoveToEnd {
    /**
     * {@comment Version of [SelectingColumns] with correctly filled in examples}
     * @include [SelectingColumns] {@include [SetMoveToEndOperationArg]}
     */
    interface MoveToEndSelectingOptions
}

/** {@set [SelectingColumns.OPERATION] [moveToEnd][moveToEnd]} */
@ExcludeFromSources
private interface SetMoveToEndOperationArg

/**
 * {@include [MoveToEnd]}
 * ### This MoveToEnd Overload
 */
@ExcludeFromSources
private interface CommonMoveToEndDocs

@Deprecated(MOVE_TO_RIGHT, ReplaceWith(MOVE_TO_RIGHT_REPLACE), DeprecationLevel.ERROR)
public fun <T> DataFrame<T>.moveToRight(columns: ColumnsSelector<T, *>): DataFrame<T> = move(columns).toEnd()

/**
 * @include [CommonMoveToEndDocs]
 * @include [SelectingColumns.Dsl.WithExample] {@include [SetMoveToEndOperationArg]}
 * @param [columns] The [Columns Selector][ColumnsSelector] used to select the columns of this [DataFrame] to move.
 */
@Refine
@Interpretable("MoveToEnd1")
public fun <T> DataFrame<T>.moveToEnd(columns: ColumnsSelector<T, *>): DataFrame<T> = move(columns).toEnd()

@Deprecated(MOVE_TO_RIGHT, ReplaceWith(MOVE_TO_RIGHT_REPLACE), DeprecationLevel.ERROR)
public fun <T> DataFrame<T>.moveToRight(vararg columns: String): DataFrame<T> = moveToEnd { columns.toColumnSet() }

/**
 * @include [CommonMoveToEndDocs]
 * @include [SelectingColumns.ColumnNames.WithExample] {@include [SetMoveToEndOperationArg]}
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
 * For more information: {@include [DocumentationUrls.Move]}
 *
 * @include [SelectingColumns.Dsl]
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
 * For more information: {@include [DocumentationUrls.Move]}
 *
 * @include [SelectingColumns.Dsl]
 *
 * ### Examples:
 * ```
 * df.move { cols { it.name == "user" } }
 *    .intoIndexed { it, index -> "allUsers"["user\$index"] }
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
 * For more information: {@include [DocumentationUrls.Move]}
 *
 * @include [SelectingColumns.ColumnNames]
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
 * For more information: {@include [DocumentationUrls.Move]}
 *
 * @include [SelectingColumns.Dsl]
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
 * For more information: {@include [DocumentationUrls.Move]}
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
 * For more information: {@include [DocumentationUrls.Move]}
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
 * For more information: {@include [DocumentationUrls.Move]}
 *
 * ### This After Overload
 */
internal interface MoveAfter

/**
 * {@include [MoveAfter]}
 * @include [SelectingColumns.Dsl]
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
 * {@include [MoveAfter]}
 * @include [SelectingColumns.ColumnNames]
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
 * For more information: {@include [DocumentationUrls.Move]}
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
 * For more information: {@include [DocumentationUrls.Move]}
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
