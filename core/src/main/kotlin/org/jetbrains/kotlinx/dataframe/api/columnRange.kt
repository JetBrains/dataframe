package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.AnyColumnReference
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.annotations.AccessApiOverload
import org.jetbrains.kotlinx.dataframe.api.ColumnRangeColumnsSelectionDsl.Grammar
import org.jetbrains.kotlinx.dataframe.columns.ColumnReference
import org.jetbrains.kotlinx.dataframe.columns.ColumnSet
import org.jetbrains.kotlinx.dataframe.documentation.AccessApiLink
import org.jetbrains.kotlinx.dataframe.documentation.DslGrammarTemplateColumnsSelectionDsl.DslGrammarTemplate
import org.jetbrains.kotlinx.dataframe.documentation.ExcludeFromSources
import org.jetbrains.kotlinx.dataframe.impl.columns.addPath
import org.jetbrains.kotlinx.dataframe.impl.columns.createColumnSet
import kotlin.reflect.KProperty

// region ColumnsSelectionDsl

/**
 * ## Range of Columns {@include [ColumnsSelectionDslLink]}
 *
 * See [Grammar] for all functions in this interface.
 */
public interface ColumnRangeColumnsSelectionDsl {

    /**
     * ## Range of Columns Grammar
     *
     * @include [DslGrammarTemplate]
     * {@set [DslGrammarTemplate.DefinitionsArg]
     *  {@include [DslGrammarTemplate.ColumnDef]}
     * }
     *
     * {@set [DslGrammarTemplate.PlainDslFunctionsArg]
     *  {@include [DslGrammarTemplate.ColumnRef]}` `{@include [Grammar.PlainDslName]}` `{@include [DslGrammarTemplate.ColumnRef]}
     * }
     * {@set [DslGrammarTemplate.ColumnSetPart]}
     * {@set [DslGrammarTemplate.ColumnGroupPart]}
     */
    public interface Grammar {

        /** [**`..`**][ColumnsSelectionDsl.rangeTo] */
        public interface PlainDslName
    }

    /**
     * ## Range of Columns
     * Creates a [ColumnSet] containing all columns from [this\] up to (and including) [endInclusive\].
     *
     * Columns inside column groups are also supported (as long as they share the same direct parent),
     * as well as any combination of {@include [AccessApiLink]}.
     *
     * ### Check out: [Grammar]
     *
     * #### For example:
     *
     * `df.`[select][DataFrame.select]`  {  `<code>{@get [CommonRangeOfColumnsDocs.Example]}</code>` }`
     *
     * @param [endInclusive\] The last column in the subset.
     * @receiver The first column in the subset.
     * @return A [ColumnSet] containing all columns from [this\] to [endInclusive\].
     * @throws [IllegalArgumentException\] if the columns have different parents or the end column is before the
     *   start column.
     * @see [ColumnsSelectionDsl.allBefore\]
     * @see [ColumnsSelectionDsl.allAfter\]
     * @see [ColumnsSelectionDsl.allFrom\]
     * @see [ColumnsSelectionDsl.allUpTo\]
     */
    @ExcludeFromSources
    private interface CommonRangeOfColumnsDocs {

        /** Examples key */
        interface Example
    }

    /**
     * @include [CommonRangeOfColumnsDocs]
     * {@set [CommonRangeOfColumnsDocs.Example] `"fromColumn"`[`..`][String.rangeTo]`"toColumn"`}
     */
    public operator fun String.rangeTo(endInclusive: String): ColumnSet<*> =
        toColumnAccessor().rangeTo(endInclusive.toColumnAccessor())

    /**
     * @include [CommonRangeOfColumnsDocs]
     * {@set [CommonRangeOfColumnsDocs.Example] `"fromColumn"`[`..`][String.rangeTo]`Type::toColumn`}
     */
    @AccessApiOverload
    public operator fun String.rangeTo(endInclusive: KProperty<*>): ColumnSet<*> =
        toColumnAccessor().rangeTo(endInclusive.toColumnAccessor())

    /**
     * @include [CommonRangeOfColumnsDocs]
     * {@set [CommonRangeOfColumnsDocs.Example] `"fromColumn"`[`..`][String.rangeTo]`toColumn`}
     */
    @AccessApiOverload
    public operator fun String.rangeTo(endInclusive: AnyColumnReference): ColumnSet<*> =
        toColumnAccessor().rangeTo(endInclusive)

    /**
     * @include [CommonRangeOfColumnsDocs]
     * {@set [CommonRangeOfColumnsDocs.Example] `Type::fromColumn`[`..`][KProperty.rangeTo]`"toColumn"`}
     */
    public operator fun KProperty<*>.rangeTo(endInclusive: String): ColumnSet<*> =
        toColumnAccessor().rangeTo(endInclusive.toColumnAccessor())

    /**
     * @include [CommonRangeOfColumnsDocs]
     * {@set [CommonRangeOfColumnsDocs.Example] `Type::fromColumn`[`..`][KProperty.rangeTo]`Type::toColumn`}
     */
    @AccessApiOverload
    public operator fun KProperty<*>.rangeTo(endInclusive: KProperty<*>): ColumnSet<*> =
        toColumnAccessor().rangeTo(endInclusive.toColumnAccessor())

    /**
     * @include [CommonRangeOfColumnsDocs]
     * {@set [CommonRangeOfColumnsDocs.Example] `Type::fromColumn`[`..`][KProperty.rangeTo]`toColumn`}
     */
    @AccessApiOverload
    public operator fun KProperty<*>.rangeTo(endInclusive: AnyColumnReference): ColumnSet<*> =
        toColumnAccessor().rangeTo(endInclusive)

    /**
     * @include [CommonRangeOfColumnsDocs]
     * {@set [CommonRangeOfColumnsDocs.Example] `fromColumn`[`..`][ColumnReference.rangeTo]`"toColumn"`}
     */
    public operator fun AnyColumnReference.rangeTo(endInclusive: String): ColumnSet<*> =
        rangeTo(endInclusive.toColumnAccessor())

    /**
     * @include [CommonRangeOfColumnsDocs]
     * {@set [CommonRangeOfColumnsDocs.Example] `fromColumn`[`..`][ColumnReference.rangeTo]`Type::toColumn`}
     */
    @AccessApiOverload
    public operator fun AnyColumnReference.rangeTo(endInclusive: KProperty<*>): ColumnSet<*> =
        rangeTo(endInclusive.toColumnAccessor())

    /**
     * @include [CommonRangeOfColumnsDocs]
     * {@set [CommonRangeOfColumnsDocs.Example] `fromColumn`[`..`][ColumnReference.rangeTo]`toColumn`}
     */
    @AccessApiOverload
    public operator fun AnyColumnReference.rangeTo(endInclusive: AnyColumnReference): ColumnSet<*> =
        createColumnSet { context ->
            val startPath = this@rangeTo.resolveSingle(context)!!.path
            val endPath = endInclusive.resolveSingle(context)!!.path
            val parentPath = startPath.parent()
            val parentEndPath = endPath.parent()
            require(parentPath == parentEndPath) {
                "Start and end columns have different parent column paths: $parentPath and $parentEndPath"
            }
            val parentCol = context.df.getColumnGroup(parentPath!!)
            val startIndex = parentCol.getColumnIndex(startPath.name)
            val endIndex = parentCol.getColumnIndex(endPath.name)

            require(startIndex <= endIndex) { "End column is before start column" }

            (startIndex..endIndex).map {
                parentCol.getColumn(it).let {
                    it.addPath(parentPath + it.name)
                }
            }
        }
}

// endregion
