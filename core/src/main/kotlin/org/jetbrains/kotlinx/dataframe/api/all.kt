package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.AnyCol
import org.jetbrains.kotlinx.dataframe.AnyColumnReference
import org.jetbrains.kotlinx.dataframe.AnyRow
import org.jetbrains.kotlinx.dataframe.ColumnFilter
import org.jetbrains.kotlinx.dataframe.ColumnSelector
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.DataRow
import org.jetbrains.kotlinx.dataframe.Predicate
import org.jetbrains.kotlinx.dataframe.RowFilter
import org.jetbrains.kotlinx.dataframe.annotations.AccessApiOverload
import org.jetbrains.kotlinx.dataframe.annotations.Interpretable
import org.jetbrains.kotlinx.dataframe.api.AllColumnsSelectionDsl.CommonAllSubsetDocs.BehaviorArg
import org.jetbrains.kotlinx.dataframe.api.AllColumnsSelectionDsl.CommonAllSubsetDocs.ColumnDoesNotExistArg
import org.jetbrains.kotlinx.dataframe.api.AllColumnsSelectionDsl.CommonAllSubsetDocs.ExampleArg
import org.jetbrains.kotlinx.dataframe.api.AllColumnsSelectionDsl.CommonAllSubsetDocs.FunctionArg
import org.jetbrains.kotlinx.dataframe.api.AllColumnsSelectionDsl.CommonAllSubsetDocs.FunctionColsArg
import org.jetbrains.kotlinx.dataframe.api.AllColumnsSelectionDsl.CommonAllSubsetDocs.TitleArg
import org.jetbrains.kotlinx.dataframe.api.AllColumnsSelectionDsl.Grammar
import org.jetbrains.kotlinx.dataframe.api.AllColumnsSelectionDsl.Grammar.After
import org.jetbrains.kotlinx.dataframe.api.AllColumnsSelectionDsl.Grammar.Before
import org.jetbrains.kotlinx.dataframe.api.AllColumnsSelectionDsl.Grammar.ColumnGroupName
import org.jetbrains.kotlinx.dataframe.api.AllColumnsSelectionDsl.Grammar.ColumnSetName
import org.jetbrains.kotlinx.dataframe.api.AllColumnsSelectionDsl.Grammar.From
import org.jetbrains.kotlinx.dataframe.api.AllColumnsSelectionDsl.Grammar.PlainDslName
import org.jetbrains.kotlinx.dataframe.api.AllColumnsSelectionDsl.Grammar.UpTo
import org.jetbrains.kotlinx.dataframe.columns.ColumnGroup
import org.jetbrains.kotlinx.dataframe.columns.ColumnPath
import org.jetbrains.kotlinx.dataframe.columns.ColumnSet
import org.jetbrains.kotlinx.dataframe.columns.ColumnWithPath
import org.jetbrains.kotlinx.dataframe.columns.ColumnsResolver
import org.jetbrains.kotlinx.dataframe.columns.SingleColumn
import org.jetbrains.kotlinx.dataframe.columns.isSingleColumnWithGroup
import org.jetbrains.kotlinx.dataframe.columns.size
import org.jetbrains.kotlinx.dataframe.columns.values
import org.jetbrains.kotlinx.dataframe.documentation.AccessApiLink
import org.jetbrains.kotlinx.dataframe.documentation.DslGrammarTemplateColumnsSelectionDsl.DslGrammarTemplate
import org.jetbrains.kotlinx.dataframe.documentation.ExcludeFromSources
import org.jetbrains.kotlinx.dataframe.documentation.Indent
import org.jetbrains.kotlinx.dataframe.documentation.Issues
import org.jetbrains.kotlinx.dataframe.documentation.LineBreak
import org.jetbrains.kotlinx.dataframe.impl.columns.TransformableColumnSet
import org.jetbrains.kotlinx.dataframe.impl.columns.addPath
import org.jetbrains.kotlinx.dataframe.impl.columns.onResolve
import org.jetbrains.kotlinx.dataframe.impl.columns.transform
import org.jetbrains.kotlinx.dataframe.impl.owner
import kotlin.reflect.KProperty

// region DataColumn

/** Returns `true` if all [values] match the given [predicate] or [values] is empty. */
public fun <T> DataColumn<T>.all(predicate: Predicate<T>): Boolean = values.all(predicate)

/** Returns `true` if all [values] are `null` or [values] is empty. */
public fun <C> DataColumn<C>.allNulls(): Boolean = size == 0 || all { it == null }

// endregion

// region DataRow

public fun AnyRow.allNA(): Boolean = owner.columns().all { it[index()].isNA }

// endregion

// region DataFrame

/** Returns `true` if all [rows] match the given [predicate] or [rows] is empty. */
public fun <T> DataFrame<T>.all(predicate: RowFilter<T>): Boolean = rows().all { predicate(it, it) }

// endregion

// region ColumnsSelectionDsl

/**
 * ## All Flavors of All (Cols) {@include [ColumnsSelectionDslLink]}
 *
 * See [Grammar] for all functions in this interface.
 * @param _UNUSED {@include [Issues.ConflictingOverloadsK2Link]}
 */
public interface AllColumnsSelectionDsl<out _UNUSED> {

    /**
     * ## Grammar of All Flavors of All (Cols):
     *
     * @include [DslGrammarTemplate]
     * {@set [DslGrammarTemplate.DefinitionsArg]
     *  {@include [DslGrammarTemplate.ColumnSetDef]}
     *  {@include [LineBreak]}
     *  {@include [DslGrammarTemplate.ColumnGroupDef]}
     *  {@include [LineBreak]}
     *  {@include [DslGrammarTemplate.ColumnDef]}
     *  {@include [LineBreak]}
     *  {@include [DslGrammarTemplate.ColumnSelectorDef]}
     *  {@include [LineBreak]}
     *  {@include [DslGrammarTemplate.ConditionDef]}
     * }
     *
     * {@set [DslGrammarTemplate.PlainDslFunctionsArg]
     *  {@include [PlainDslName]}**`()`**
     *
     *  `| `**`all`**`(`{@include [Before]}`|`{@include [After]}`|`{@include [From]}`|`{@include [UpTo]}`) ( `**`(`**{@include [DslGrammarTemplate.ColumnRef]}**`)`**`  |  `**`{ `**{@include [DslGrammarTemplate.ColumnSelectorRef]}**` \}`**` )`
     * }
     *
     * {@set [DslGrammarTemplate.ColumnSetFunctionsArg]
     *  {@include [Indent]}{@include [ColumnSetName]}**`()`**
     *
     *  {@include [Indent]}`| `**`.all`**`(`{@include [Before]}`|`{@include [After]}`|`{@include [From]}`|`{@include [UpTo]}`) ( `**`(`**{@include [DslGrammarTemplate.ColumnRef]}**`)`**`  |  `**`{ `**{@include [DslGrammarTemplate.ConditionRef]}**` \}`**` )`
     * }
     *
     * {@set [DslGrammarTemplate.ColumnGroupFunctionsArg]
     *  {@include [Indent]}{@include [ColumnGroupName]}**`()`**
     *
     *  {@include [Indent]}`| `**`.allCols`**`(`{@include [Before]}`|`{@include [After]}`|`{@include [From]}`|`{@include [UpTo]}`) ( `**`(`**{@include [DslGrammarTemplate.ColumnRef]}**`)`**`  |  `**`{ `**{@include [DslGrammarTemplate.ColumnSelectorRef]}**` \}`**` )`
     * }
     */
    public interface Grammar {

        /** [**`all`**][ColumnsSelectionDsl.all] */
        public interface PlainDslName

        /** __`.`__[**`all`**][ColumnsSelectionDsl.all] */
        public interface ColumnSetName

        /** __`.`__[**`allCols`**][ColumnsSelectionDsl.allCols] */
        public interface ColumnGroupName

        /** [**`Before`**][ColumnsSelectionDsl.allColsBefore] */
        public interface Before

        /** [**`After`**][ColumnsSelectionDsl.allAfter] */
        public interface After

        /** [**`From`**][ColumnsSelectionDsl.allColsFrom] */
        public interface From

        /** [**`UpTo`**][ColumnsSelectionDsl.allColsUpTo] */
        public interface UpTo
    }

    /**
     * #### Flavors of All (Cols):
     *
     * - [`all(Cols)`][ColumnsSelectionDsl.allCols]`()`:
     *     All columns
     *
     * - [`all(Cols)Before`][ColumnsSelectionDsl.allColsBefore]`(column)`:
     *     All columns before the specified column, excluding that column
     *
     * - [`all(Cols)After`][ColumnsSelectionDsl.allColsAfter]`(column)`:
     *     All columns after the specified column, excluding that column
     *
     * - [`all(Cols)From`][ColumnsSelectionDsl.allColsFrom]`(column)`:
     *     All columns from the specified column, including that column
     *
     * - [`all(Cols)UpTo`][ColumnsSelectionDsl.allColsUpTo]`(column)`:
     *     All columns up to the specified column, including that column
     */
    @ExcludeFromSources
    private interface AllFlavors

    /**
     * ## {@get [TitleArg]}
     *
     * Creates a new [ColumnSet] that contains a subset of columns from [this\],
     * containing all columns {@get [BehaviorArg]}.
     *
     * [column\] can be specified both relative to the current [ColumnGroup] or the outer scope and
     * can be referenced using any {@include [AccessApiLink]}.
     *
     * If [column\] does not exist, {@get [ColumnDoesNotExistArg]}.
     * {@include [LineBreak]}
     * NOTE: Using the `{}` overloads of these functions requires a [ColumnSelector]
     * in the Plain DSL and on [column groups][ColumnGroup].
     * On [ColumnSets][ColumnSet] it requires a [ColumnFilter] instead.
     *
     * ### Check out: [Grammar]
     *
     * #### For example:
     *
     * `df.`[select][DataFrame.select]`  {  `[{@get [FunctionArg]}][ColumnsSelectionDsl.{@get [FunctionArg]}]`("someColumn") }`
     *
     * `df.`[select][DataFrame.select]`  {  `[colGroup][ColumnsSelectionDsl.colGroup]`(Type::myColGroup).`[{@get [FunctionColsArg]}][SingleColumn.{@get [FunctionColsArg]}]`(someColumn) }`
     *
     * `df.`[select][DataFrame.select]`  {  `[colsOf][ColumnsSelectionDsl.colsOf]`<`[Int][Int]`>().`[{@get [FunctionArg]}][ColumnSet.{@get [FunctionArg]}]`(Type::someColumn) }`
     *
     * #### Examples for this overload:
     *
     * {@get [ExampleArg]}
     *
     * {@include [AllFlavors]}
     *
     * @return A new [ColumnSet] containing all columns {@get [BehaviorArg]}.
     * @see [allBefore\]
     * @see [allAfter\]
     * @see [allFrom\]
     * @see [allUpTo\]
     * @see [ColumnsSelectionDsl.allExcept\]
     * @see [all\]
     * @see [cols\]
     */
    @ExcludeFromSources
    private interface CommonAllSubsetDocs {

        // The title of the function, a.k.a. "All (Cols) After"
        interface TitleArg

        // The exact name of the function, a.k.a. "allAfter"
        interface FunctionArg

        // The exact name of the function, a.k.a. "allColsAfter"
        interface FunctionColsArg

        /*
         * Small line of text explaining the behavior of the function,
         * a.k.a. "after [column\], excluding [column\]"
         */
        interface BehaviorArg

        /*
         * Small line of text explaining what happens if `column` does not exist.
         */
        interface ColumnDoesNotExistArg

        // Example argument
        interface ExampleArg
    }

    // region all

    /**
     * ## All (Cols)
     *
     * Creates a new [ColumnSet] that contains all columns from [this\],
     * the opposite of [none][ColumnsSelectionDsl.none].
     *
     * This makes the function equivalent to [cols()][ColumnsSelectionDsl.cols] without filter.
     *
     * This function operates solely on columns at the top-level.
     *
     * NOTE: For [column groups][ColumnGroup], `all` is named `allCols` instead to avoid confusion.
     *
     * ### Check out: [Grammar]
     *
     * #### For example:
     * `df.`[move][DataFrame.move]`  {  `[all][ColumnsSelectionDsl.all]`() }.`[under][MoveClause.under]`("info")`
     *
     * `df.`[select][DataFrame.select]` { myGroup.`[allCols][SingleColumn.allCols]`() }`
     *
     * #### Examples for this overload:
     *
     * {@get [CommonAllDocs.Examples]}
     *
     * {@include [AllFlavors]}
     *
     * @see [ColumnsSelectionDsl.rangeTo\]
     * @see [ColumnsSelectionDsl.allBefore\]
     * @see [ColumnsSelectionDsl.allAfter\]
     * @see [ColumnsSelectionDsl.allFrom\]
     * @see [ColumnsSelectionDsl.allUpTo\]
     * @see [ColumnsSelectionDsl.allExcept\]
     * @see [ColumnsSelectionDsl.cols\]
     */
    @ExcludeFromSources
    private interface CommonAllDocs {

        /** Example argument */
        interface Examples
    }

    /**
     * @include [CommonAllDocs]
     * @set [CommonAllDocs.Examples]
     *
     * `df.`[select][DataFrame.select]`  {  `[cols][ColumnsSelectionDsl.cols]`  { "a" in  `[name][ColumnWithPath.name]` }.`[all][ColumnSet.all]`() }`
     * {@include [LineBreak]}
     * NOTE: This is an identity call and can be omitted in most cases.
     */
    @Suppress("UNCHECKED_CAST")
    public fun <C> ColumnSet<C>.all(): TransformableColumnSet<C> = allColumnsInternal() as TransformableColumnSet<C>

    /**
     * @include [CommonAllDocs]
     * @set [CommonAllDocs.Examples]
     *
     * `df.`[select][DataFrame.select]`  {  `[all][ColumnsSelectionDsl.all]`() }`
     */
    @Interpretable("All0")
    public fun ColumnsSelectionDsl<*>.all(): TransformableColumnSet<*> = asSingleColumn().allColumnsInternal()

    /**
     * @include [CommonAllDocs]
     * @set [CommonAllDocs.Examples]
     *
     * `df.`[select][DataFrame.select]` { myGroup.`[allCols][SingleColumn.allCols]`() }`
     */
    public fun SingleColumn<DataRow<*>>.allCols(): TransformableColumnSet<*> =
        ensureIsColumnGroup().allColumnsInternal()

    /**
     * @include [CommonAllDocs]
     * @set [CommonAllDocs.Examples]
     *
     * `df.`[select][DataFrame.select]` { "myGroupCol".`[allCols][String.allCols]`() }`
     */
    public fun String.allCols(): TransformableColumnSet<*> = columnGroup(this).allCols()

    /**
     * @include [CommonAllDocs]
     * @set [CommonAllDocs.Examples]
     *
     * `df.`[select][DataFrame.select]` { DataSchemaType::columnGroup.`[allCols][KProperty.allCols]`() }`
     */
    public fun KProperty<*>.allCols(): TransformableColumnSet<*> = columnGroup(this).allCols()

    /**
     * @include [CommonAllDocs]
     * @set [CommonAllDocs.Examples]
     *
     * `df.`[select][DataFrame.select]` { "pathTo"["myGroup"].`[allCols][ColumnPath.allCols]`() }`
     */
    public fun ColumnPath.allCols(): TransformableColumnSet<*> = columnGroup(this).allCols()

    // endregion

    // region allAfter

    /**
     * @include [CommonAllSubsetDocs]
     * @set [CommonAllSubsetDocs.TitleArg] All (Cols) After
     * @set [CommonAllSubsetDocs.FunctionArg] allAfter
     * @set [CommonAllSubsetDocs.FunctionColsArg] allColsAfter
     * @set [CommonAllSubsetDocs.BehaviorArg] after [column\], excluding [column\] itself
     * @set [CommonAllSubsetDocs.ColumnDoesNotExistArg] the function will return an empty [ColumnSet][ColumnSet]
     * @param [column\] The specified column after which all columns should be taken. This column can be referenced
     *   to both relatively to the current [ColumnsResolver] and absolutely.
     */
    private interface AllAfterDocs

    /**
     * @include [AllAfterDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]`  {  `[cols][ColumnsSelectionDsl.cols]` { .. }.`[allAfter][ColumnSet.allAfter]`{@get [ColumnSetAllAfterDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface ColumnSetAllAfterDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [ColumnSetAllAfterDocs] {@set [ColumnSetAllAfterDocs.Arg] \ \{ myColumn `[in][String.contains]` it.`[name][ColumnWithPath.name]` \}} */
    @Suppress("UNCHECKED_CAST")
    public fun <C> ColumnSet<C>.allAfter(column: ColumnFilter<C>): ColumnSet<C> =
        allAfterInternal(column as ColumnFilter<*>) as ColumnSet<C>

    /** @include [ColumnSetAllAfterDocs] {@set [ColumnSetAllAfterDocs.Arg] ("pathTo"["myColumn"])} */
    @Suppress("UNCHECKED_CAST")
    public fun <C> ColumnSet<C>.allAfter(column: ColumnPath): ColumnSet<C> =
        allAfterInternal { it.path == column } as ColumnSet<C>

    /** @include [ColumnSetAllAfterDocs] {@set [ColumnSetAllAfterDocs.Arg] ("myColumn")} */
    public fun <C> ColumnSet<C>.allAfter(column: String): ColumnSet<C> = allAfter(pathOf(column))

    /** @include [ColumnSetAllAfterDocs] {@set [ColumnSetAllAfterDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun <C> ColumnSet<C>.allAfter(column: AnyColumnReference): ColumnSet<C> = allAfter(column.path())

    /** @include [ColumnSetAllAfterDocs] {@set [ColumnSetAllAfterDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun <C> ColumnSet<C>.allAfter(column: KProperty<*>): ColumnSet<C> =
        allAfter(column.toColumnAccessor().path())

    /**
     * @include [AllAfterDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]`  {  `[allAfter][ColumnsSelectionDsl.allAfter]`{@get [ColumnsSelectionDslAllAfterDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface ColumnsSelectionDslAllAfterDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [ColumnsSelectionDslAllAfterDocs] {@set [ColumnsSelectionDslAllAfterDocs.Arg] \ \{ myColumn \}} */
    public fun <T> ColumnsSelectionDsl<T>.allAfter(column: ColumnSelector<T, *>): ColumnSet<*> =
        asSingleColumn().allColsAfter(column)

    /** @include [ColumnsSelectionDslAllAfterDocs] {@set [ColumnsSelectionDslAllAfterDocs.Arg] ("pathTo"["myColumn"])} */
    public fun ColumnsSelectionDsl<*>.allAfter(column: ColumnPath): ColumnSet<*> = asSingleColumn().allColsAfter(column)

    /** @include [ColumnsSelectionDslAllAfterDocs] {@set [ColumnsSelectionDslAllAfterDocs.Arg] ("myColumn")} */
    public fun ColumnsSelectionDsl<*>.allAfter(column: String): ColumnSet<*> = allAfter(pathOf(column))

    /** @include [ColumnsSelectionDslAllAfterDocs] {@set [ColumnsSelectionDslAllAfterDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun ColumnsSelectionDsl<*>.allAfter(column: AnyColumnReference): ColumnSet<*> = allAfter(column.path())

    /** @include [ColumnsSelectionDslAllAfterDocs] {@set [ColumnsSelectionDslAllAfterDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun ColumnsSelectionDsl<*>.allAfter(column: KProperty<*>): ColumnSet<*> =
        allAfter(column.toColumnAccessor().path())

    /**
     * @include [AllAfterDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]` { someColumnGroup.`[allColsAfter][SingleColumn.allColsAfter]`{@get [SingleColumnAllAfterDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface SingleColumnAllAfterDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [SingleColumnAllAfterDocs] {@set [SingleColumnAllAfterDocs.Arg] \ \{ myColumn \}} */
    public fun <T> SingleColumn<DataRow<T>>.allColsAfter(column: ColumnSelector<T, *>): ColumnSet<*> {
        var resolvedCol: DataColumn<*>? = null
        return this
            .ensureIsColumnGroup()
            .onResolve { resolvedCol = it!!.asColumnGroup().getColumn(column) }
            .allAfterInternal { it.data == resolvedCol!! }
    }

    /** @include [SingleColumnAllAfterDocs] {@set [SingleColumnAllAfterDocs.Arg] ("pathTo"["myColumn"])} */
    public fun SingleColumn<DataRow<*>>.allColsAfter(column: ColumnPath): ColumnSet<*> {
        var path: ColumnPath? = null
        return this
            .ensureIsColumnGroup()
            .onResolve { path = it!!.path }
            .allAfterInternal {
                // accept both relative and full column path
                it.path == path!! + column || it.path == column
            }
    }

    /** @include [SingleColumnAllAfterDocs] {@set [SingleColumnAllAfterDocs.Arg] ("myColumn")} */
    public fun SingleColumn<DataRow<*>>.allColsAfter(column: String): ColumnSet<*> = allColsAfter(pathOf(column))

    /** @include [SingleColumnAllAfterDocs] {@set [SingleColumnAllAfterDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun SingleColumn<DataRow<*>>.allColsAfter(column: AnyColumnReference): ColumnSet<*> =
        allColsAfter(column.path())

    /** @include [SingleColumnAllAfterDocs] {@set [SingleColumnAllAfterDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun SingleColumn<DataRow<*>>.allColsAfter(column: KProperty<*>): ColumnSet<*> =
        allColsAfter(column.toColumnAccessor().path())

    /**
     * @include [AllAfterDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]` { "someColGroup".`[allColsAfter][String.allColsAfter]`{@get [StringAllAfterDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface StringAllAfterDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [StringAllAfterDocs] {@set [StringAllAfterDocs.Arg] \ \{ myColumn \}} */
    public fun String.allColsAfter(column: ColumnSelector<*, *>): ColumnSet<*> = columnGroup(this).allColsAfter(column)

    /** @include [StringAllAfterDocs] {@set [StringAllAfterDocs.Arg] ("pathTo"["myColumn"])} */
    public fun String.allColsAfter(column: ColumnPath): ColumnSet<*> = columnGroup(this).allColsAfter(column)

    /** @include [StringAllAfterDocs] {@set [StringAllAfterDocs.Arg] ("myColumn")} */
    public fun String.allColsAfter(column: String): ColumnSet<*> = columnGroup(this).allColsAfter(column)

    /** @include [StringAllAfterDocs] {@set [StringAllAfterDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun String.allColsAfter(column: AnyColumnReference): ColumnSet<*> = columnGroup(this).allColsAfter(column)

    /** @include [StringAllAfterDocs] {@set [StringAllAfterDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun String.allColsAfter(column: KProperty<*>): ColumnSet<*> = columnGroup(this).allColsAfter(column)

    /**
     * @include [AllAfterDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]` { DataSchemaType::myColGroup.`[allColsAfter][KProperty.allColsAfter]`{@get [KPropertyAllAfterDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface KPropertyAllAfterDocs {

        /** Example argument to use */
        interface Arg
    }

    /**
     * @include [KPropertyAllAfterDocs] {@set [KPropertyAllAfterDocs.Arg] \ \{ myColumn \}}
     */
    public fun <C> KProperty<C>.allColsAfter(column: ColumnSelector<C, *>): ColumnSet<*> =
        columnGroup(this).allColsAfter(column)

    /** @include [KPropertyAllAfterDocs] {@set [KPropertyAllAfterDocs.Arg] ("pathTo"["myColumn"])} */
    public fun KProperty<*>.allColsAfter(column: ColumnPath): ColumnSet<*> = columnGroup(this).allColsAfter(column)

    /** @include [KPropertyAllAfterDocs] {@set [KPropertyAllAfterDocs.Arg] ("myColumn")} */
    public fun KProperty<*>.allColsAfter(column: String): ColumnSet<*> = columnGroup(this).allColsAfter(column)

    /** @include [KPropertyAllAfterDocs] {@set [KPropertyAllAfterDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun KProperty<*>.allColsAfter(column: AnyColumnReference): ColumnSet<*> =
        columnGroup(this).allColsAfter(column)

    /** @include [KPropertyAllAfterDocs] {@set [KPropertyAllAfterDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun KProperty<*>.allColsAfter(column: KProperty<*>): ColumnSet<*> = columnGroup(this).allColsAfter(column)

    /**
     * @include [AllAfterDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]` { "pathTo"["someColGroup"].`[allColsAfter][ColumnPath.allColsAfter]`{@get [ColumnPathAllAfterDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface ColumnPathAllAfterDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [ColumnPathAllAfterDocs] {@set [ColumnPathAllAfterDocs.Arg] \ \{ myColumn \}} */
    public fun ColumnPath.allColsAfter(column: ColumnSelector<*, *>): ColumnSet<*> =
        columnGroup(this).allColsAfter(column)

    /** @include [ColumnPathAllAfterDocs] {@set [ColumnPathAllAfterDocs.Arg] ("pathTo"["myColumn"])} */
    public fun ColumnPath.allColsAfter(column: ColumnPath): ColumnSet<*> = columnGroup(this).allColsAfter(column)

    /** @include [ColumnPathAllAfterDocs] {@set [ColumnPathAllAfterDocs.Arg] ("myColumn")} */
    public fun ColumnPath.allColsAfter(column: String): ColumnSet<*> = columnGroup(this).allColsAfter(column)

    /** @include [ColumnPathAllAfterDocs] {@set [ColumnPathAllAfterDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun ColumnPath.allColsAfter(column: AnyColumnReference): ColumnSet<*> =
        columnGroup(this).allColsAfter(column)

    /** @include [ColumnPathAllAfterDocs] {@set [ColumnPathAllAfterDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun ColumnPath.allColsAfter(column: KProperty<*>): ColumnSet<*> = columnGroup(this).allColsAfter(column)

    // endregion

    // region allFrom

    /**
     * @include [CommonAllSubsetDocs]
     * @set [CommonAllSubsetDocs.TitleArg] All (Cols) From
     * @set [CommonAllSubsetDocs.FunctionArg] allFrom
     * @set [CommonAllSubsetDocs.FunctionColsArg] allColsFrom
     * @set [CommonAllSubsetDocs.BehaviorArg] from [column\], including [column\] itself
     * @set [CommonAllSubsetDocs.ColumnDoesNotExistArg] the function will return an empty [ColumnSet][ColumnSet]
     * @param [column\] The specified column from which all columns should be taken. This column can be referenced
     *   to both relatively to the current [ColumnsResolver] and absolutely.
     */
    private interface AllFromDocs

    /**
     * @include [AllFromDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]`  {  `[cols][ColumnsSelectionDsl.cols]` { .. }.`[allFrom][ColumnSet.allFrom]`{@get [ColumnSetAllFromDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface ColumnSetAllFromDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [ColumnSetAllFromDocs] {@set [ColumnSetAllFromDocs.Arg] \ \{ myColumn `[in][String.contains]` it.`[name][ColumnWithPath.name]` \}} */
    @Suppress("UNCHECKED_CAST")
    public fun <C> ColumnSet<C>.allFrom(column: ColumnFilter<C>): ColumnSet<C> =
        allFromInternal(column as ColumnFilter<*>) as ColumnSet<C>

    /** @include [ColumnSetAllFromDocs] {@set [ColumnSetAllFromDocs.Arg] ("pathTo"["myColumn"])} */
    @Suppress("UNCHECKED_CAST")
    public fun <C> ColumnSet<C>.allFrom(column: ColumnPath): ColumnSet<C> =
        allFromInternal { it.path == column } as ColumnSet<C>

    /** @include [ColumnSetAllFromDocs] {@set [ColumnSetAllFromDocs.Arg] ("myColumn")} */
    public fun <C> ColumnSet<C>.allFrom(column: String): ColumnSet<C> = allFrom(pathOf(column))

    /** @include [ColumnSetAllFromDocs] {@set [ColumnSetAllFromDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun <C> ColumnSet<C>.allFrom(column: AnyColumnReference): ColumnSet<C> = allFrom(column.path())

    /** @include [ColumnSetAllFromDocs] {@set [ColumnSetAllFromDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun <C> ColumnSet<C>.allFrom(column: KProperty<*>): ColumnSet<C> = allFrom(column.toColumnAccessor().path())

    /**
     * @include [AllFromDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]`  {  `[allFrom][ColumnsSelectionDsl.allFrom]`{@get [ColumnsSelectionDslAllFromDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface ColumnsSelectionDslAllFromDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [ColumnsSelectionDslAllFromDocs] {@set [ColumnsSelectionDslAllFromDocs.Arg] \ \{ myColumn \}} */
    public fun <T> ColumnsSelectionDsl<T>.allFrom(column: ColumnSelector<T, *>): ColumnSet<*> =
        asSingleColumn().allColsFrom(column)

    /** @include [ColumnsSelectionDslAllFromDocs] {@set [ColumnsSelectionDslAllFromDocs.Arg] ("pathTo"["myColumn"])} */
    public fun ColumnsSelectionDsl<*>.allFrom(column: ColumnPath): ColumnSet<*> = asSingleColumn().allColsFrom(column)

    /** @include [ColumnsSelectionDslAllFromDocs] {@set [ColumnsSelectionDslAllFromDocs.Arg] ("myColumn")} */
    public fun ColumnsSelectionDsl<*>.allFrom(column: String): ColumnSet<*> = asSingleColumn().allColsFrom(column)

    /** @include [ColumnsSelectionDslAllFromDocs] {@set [ColumnsSelectionDslAllFromDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun ColumnsSelectionDsl<*>.allFrom(column: AnyColumnReference): ColumnSet<*> =
        asSingleColumn().allColsFrom(column)

    /** @include [ColumnsSelectionDslAllFromDocs] {@set [ColumnsSelectionDslAllFromDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun ColumnsSelectionDsl<*>.allFrom(column: KProperty<*>): ColumnSet<*> = asSingleColumn().allColsFrom(column)

    /**
     * @include [AllFromDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]` { someColumnGroup.`[allColsFrom][SingleColumn.allColsFrom]`{@get [SingleColumnAllFromDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface SingleColumnAllFromDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [SingleColumnAllFromDocs] {@set [SingleColumnAllFromDocs.Arg] \ \{ myColumn \}} */
    public fun <T> SingleColumn<DataRow<T>>.allColsFrom(column: ColumnSelector<T, *>): ColumnSet<*> {
        var resolvedCol: DataColumn<*>? = null
        return this
            .ensureIsColumnGroup()
            .onResolve { resolvedCol = it!!.asColumnGroup().getColumn(column) }
            .allFromInternal { it.data == resolvedCol!! }
    }

    /** @include [SingleColumnAllFromDocs] {@set [SingleColumnAllFromDocs.Arg] ("pathTo"["myColumn"])} */
    public fun SingleColumn<DataRow<*>>.allColsFrom(column: ColumnPath): ColumnSet<*> {
        var path: ColumnPath? = null
        return this
            .ensureIsColumnGroup()
            .onResolve { path = it!!.path }
            .allFromInternal {
                // accept both relative and full column path
                it.path == path!! + column || it.path == column
            }
    }

    /** @include [SingleColumnAllFromDocs] {@set [SingleColumnAllFromDocs.Arg] ("myColumn")} */
    public fun SingleColumn<DataRow<*>>.allColsFrom(column: String): ColumnSet<*> = allColsFrom(pathOf(column))

    /** @include [SingleColumnAllFromDocs] {@set [SingleColumnAllFromDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun SingleColumn<DataRow<*>>.allColsFrom(column: AnyColumnReference): ColumnSet<*> =
        allColsFrom(column.path())

    /** @include [SingleColumnAllFromDocs] {@set [SingleColumnAllFromDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun SingleColumn<DataRow<*>>.allColsFrom(column: KProperty<*>): ColumnSet<*> =
        allColsFrom(column.toColumnAccessor().path())

    /**
     * @include [AllFromDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]` { "someColGroup".`[allColsFrom][String.allColsFrom]`{@get [StringAllFromDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface StringAllFromDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [StringAllFromDocs] {@set [StringAllFromDocs.Arg] \ \{ myColumn \}} */
    public fun String.allColsFrom(column: ColumnSelector<*, *>): ColumnSet<*> = columnGroup(this).allColsFrom(column)

    /** @include [StringAllFromDocs] {@set [StringAllFromDocs.Arg] ("pathTo"["myColumn"])} */
    public fun String.allColsFrom(column: ColumnPath): ColumnSet<*> = columnGroup(this).allColsFrom(column)

    /** @include [StringAllFromDocs] {@set [StringAllFromDocs.Arg] ("myColumn")} */
    public fun String.allColsFrom(column: String): ColumnSet<*> = columnGroup(this).allColsFrom(column)

    /** @include [StringAllFromDocs] {@set [StringAllFromDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun String.allColsFrom(column: AnyColumnReference): ColumnSet<*> = columnGroup(this).allColsFrom(column)

    /** @include [StringAllFromDocs] {@set [StringAllFromDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun String.allColsFrom(column: KProperty<*>): ColumnSet<*> = columnGroup(this).allColsFrom(column)

    /**
     * @include [AllFromDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]` { DataSchemaType::someColGroup.`[allColsFrom][KProperty.allColsFrom]`{@get [KPropertyAllFromDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface KPropertyAllFromDocs {

        /** Example argument to use */
        interface Arg
    }

    /**
     * @include [KPropertyAllFromDocs] {@set [KPropertyAllFromDocs.Arg] \ \{ myColumn \}}
     */
    public fun <C> KProperty<C>.allColsFrom(column: ColumnSelector<C, *>): ColumnSet<*> =
        columnGroup(this).allColsFrom(column)

    /** @include [KPropertyAllFromDocs] {@set [KPropertyAllFromDocs.Arg] ("pathTo"["myColumn"])} */
    public fun KProperty<*>.allColsFrom(column: ColumnPath): ColumnSet<*> = columnGroup(this).allColsFrom(column)

    /** @include [KPropertyAllFromDocs] {@set [KPropertyAllFromDocs.Arg] ("myColumn")} */
    public fun KProperty<*>.allColsFrom(column: String): ColumnSet<*> = columnGroup(this).allColsFrom(column)

    /** @include [KPropertyAllFromDocs] {@set [KPropertyAllFromDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun KProperty<*>.allColsFrom(column: AnyColumnReference): ColumnSet<*> =
        columnGroup(this).allColsFrom(column)

    /** @include [KPropertyAllFromDocs] {@set [KPropertyAllFromDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun KProperty<*>.allColsFrom(column: KProperty<*>): ColumnSet<*> = columnGroup(this).allColsFrom(column)

    /**
     * @include [AllFromDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]` { "pathTo"["someColGroup"].`[allFrom][ColumnPath.allColsFrom]`{@get [ColumnPathAllFromDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface ColumnPathAllFromDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [ColumnPathAllFromDocs] {@set [ColumnPathAllFromDocs.Arg] \ \{ myColumn \}} */
    public fun ColumnPath.allColsFrom(column: ColumnSelector<*, *>): ColumnSet<*> =
        columnGroup(this).allColsFrom(column)

    /** @include [ColumnPathAllFromDocs] {@set [ColumnPathAllFromDocs.Arg] ("pathTo"["myColumn"])} */
    public fun ColumnPath.allColsFrom(column: ColumnPath): ColumnSet<*> = columnGroup(this).allColsFrom(column)

    /** @include [ColumnPathAllFromDocs] {@set [ColumnPathAllFromDocs.Arg] ("myColumn")} */
    public fun ColumnPath.allColsFrom(column: String): ColumnSet<*> = columnGroup(this).allColsFrom(column)

    /** @include [ColumnPathAllFromDocs] {@set [ColumnPathAllFromDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun ColumnPath.allColsFrom(column: AnyColumnReference): ColumnSet<*> = columnGroup(this).allColsFrom(column)

    /** @include [ColumnPathAllFromDocs] {@set [ColumnPathAllFromDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun ColumnPath.allColsFrom(column: KProperty<*>): ColumnSet<*> = columnGroup(this).allColsFrom(column)

    // endregion

    // region allBefore

    /**
     * @include [CommonAllSubsetDocs]
     * @set [CommonAllSubsetDocs.TitleArg] All (Cols) Before
     * @set [CommonAllSubsetDocs.FunctionArg] allBefore
     * @set [CommonAllSubsetDocs.FunctionColsArg] allColsBefore
     * @set [CommonAllSubsetDocs.BehaviorArg] before [column\], excluding [column\] itself
     * @set [CommonAllSubsetDocs.ColumnDoesNotExistArg] the function will return a [ColumnSet][ColumnSet] containing all columns
     * @param [column\] The specified column before which all columns should be taken. This column can be referenced
     *   to both relatively to the current [ColumnsResolver] and absolutely.
     */
    private interface AllBeforeDocs

    /**
     * @include [AllBeforeDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]`  {  `[cols][ColumnsSelectionDsl.cols]` { .. }.`[allBefore][ColumnSet.allBefore]`{@get [ColumnSetAllBeforeDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface ColumnSetAllBeforeDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [ColumnSetAllBeforeDocs] {@set [ColumnSetAllBeforeDocs.Arg] \ \{ myColumn `[in][String.contains]` it.`[name][ColumnWithPath.name]` \}} */
    @Suppress("UNCHECKED_CAST")
    public fun <C> ColumnSet<C>.allBefore(column: ColumnFilter<C>): ColumnSet<C> =
        allBeforeInternal(column as ColumnFilter<*>) as ColumnSet<C>

    /** @include [ColumnSetAllBeforeDocs] {@set [ColumnSetAllBeforeDocs.Arg] ("pathTo"["myColumn"])} */
    @Suppress("UNCHECKED_CAST")
    public fun <C> ColumnSet<C>.allBefore(column: ColumnPath): ColumnSet<C> =
        allBeforeInternal { it.path == column } as ColumnSet<C>

    /** @include [ColumnSetAllBeforeDocs] {@set [ColumnSetAllBeforeDocs.Arg] ("myColumn")} */
    public fun <C> ColumnSet<C>.allBefore(column: String): ColumnSet<C> = allBefore(pathOf(column))

    /** @include [ColumnSetAllBeforeDocs] {@set [ColumnSetAllBeforeDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun <C> ColumnSet<C>.allBefore(column: AnyColumnReference): ColumnSet<C> = allBefore(column.path())

    /** @include [ColumnSetAllBeforeDocs] {@set [ColumnSetAllBeforeDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun <C> ColumnSet<C>.allBefore(column: KProperty<*>): ColumnSet<C> =
        allBefore(column.toColumnAccessor().path())

    /**
     * @include [AllBeforeDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]`  {  `[allBefore][ColumnsSelectionDsl.allBefore]`{@get [ColumnsSelectionDslAllBeforeDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface ColumnsSelectionDslAllBeforeDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [ColumnsSelectionDslAllBeforeDocs] {@set [ColumnsSelectionDslAllBeforeDocs.Arg] \ \{ myColumn \}} */
    public fun <T> ColumnsSelectionDsl<T>.allBefore(column: ColumnSelector<T, *>): ColumnSet<*> =
        asSingleColumn().allColsBefore(column)

    /** @include [ColumnsSelectionDslAllBeforeDocs] {@set [ColumnsSelectionDslAllBeforeDocs.Arg] ("pathTo"["myColumn"])} */
    public fun ColumnsSelectionDsl<*>.allBefore(column: ColumnPath): ColumnSet<*> =
        asSingleColumn().allColsBefore(column)

    /** @include [ColumnsSelectionDslAllBeforeDocs] {@set [ColumnsSelectionDslAllBeforeDocs.Arg] ("myColumn")} */
    public fun ColumnsSelectionDsl<*>.allBefore(column: String): ColumnSet<*> = allBefore(pathOf(column))

    /** @include [ColumnsSelectionDslAllBeforeDocs] {@set [ColumnsSelectionDslAllBeforeDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun ColumnsSelectionDsl<*>.allBefore(column: AnyColumnReference): ColumnSet<*> = allBefore(column.path())

    /** @include [ColumnsSelectionDslAllBeforeDocs] {@set [ColumnsSelectionDslAllBeforeDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun ColumnsSelectionDsl<*>.allBefore(column: KProperty<*>): ColumnSet<*> =
        allBefore(column.toColumnAccessor().path())

    /**
     * @include [AllBeforeDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]` { someColumnGroup.`[allColsBefore][SingleColumn.allColsBefore]`{@get [SingleColumnAllBeforeDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface SingleColumnAllBeforeDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [SingleColumnAllBeforeDocs] {@set [SingleColumnAllBeforeDocs.Arg] \ \{ myColumn \}} */
    public fun <T> SingleColumn<DataRow<T>>.allColsBefore(column: ColumnSelector<T, *>): ColumnSet<*> {
        var resolvedCol: DataColumn<*>? = null
        return this
            .ensureIsColumnGroup()
            .onResolve { resolvedCol = it!!.asColumnGroup().getColumn(column) }
            .allBeforeInternal { it.data == resolvedCol }
    }

    /** @include [SingleColumnAllBeforeDocs] {@set [SingleColumnAllBeforeDocs.Arg] ("pathTo"["myColumn"])} */
    public fun SingleColumn<DataRow<*>>.allColsBefore(column: ColumnPath): ColumnSet<*> {
        var path: ColumnPath? = null
        return this
            .ensureIsColumnGroup()
            .onResolve { path = it!!.path }
            .allBeforeInternal { it.path == path!! + column || it.path == column }
    }

    /** @include [SingleColumnAllBeforeDocs] {@set [SingleColumnAllBeforeDocs.Arg] ("myColumn")} */
    public fun SingleColumn<DataRow<*>>.allColsBefore(column: String): ColumnSet<*> = allColsBefore(pathOf(column))

    /** @include [SingleColumnAllBeforeDocs] {@set [SingleColumnAllBeforeDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun SingleColumn<DataRow<*>>.allColsBefore(column: AnyColumnReference): ColumnSet<*> =
        allColsBefore(column.path())

    /** @include [SingleColumnAllBeforeDocs] {@set [SingleColumnAllBeforeDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun SingleColumn<DataRow<*>>.allColsBefore(column: KProperty<*>): ColumnSet<*> =
        allColsBefore(column.toColumnAccessor().path())

    /**
     * @include [AllBeforeDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]` { "someColGroup".`[allColsBefore][String.allColsBefore]`{@get [StringAllBeforeDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface StringAllBeforeDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [StringAllBeforeDocs] {@set [StringAllBeforeDocs.Arg] \ \{ myColumn \}} */
    public fun String.allColsBefore(column: ColumnSelector<*, *>): ColumnSet<*> =
        columnGroup(this).allColsBefore(column)

    /** @include [StringAllBeforeDocs] {@set [StringAllBeforeDocs.Arg] ("pathTo"["myColumn"])} */
    public fun String.allColsBefore(column: ColumnPath): ColumnSet<*> = columnGroup(this).allColsBefore(column)

    /** @include [StringAllBeforeDocs] {@set [StringAllBeforeDocs.Arg] ("myColumn")} */
    public fun String.allColsBefore(column: String): ColumnSet<*> = columnGroup(this).allColsBefore(column)

    /** @include [StringAllBeforeDocs] {@set [StringAllBeforeDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun String.allColsBefore(column: AnyColumnReference): ColumnSet<*> = columnGroup(this).allColsBefore(column)

    /** @include [StringAllBeforeDocs] {@set [StringAllBeforeDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun String.allColsBefore(column: KProperty<*>): ColumnSet<*> = columnGroup(this).allColsBefore(column)

    /**
     * @include [AllBeforeDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]` { DataSchemaType::someColGroup.`[allColsBefore][KProperty.allColsBefore]`{@get [KPropertyAllBeforeDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface KPropertyAllBeforeDocs {

        /** Example argument to use */
        interface Arg
    }

    /**
     * @include [KPropertyAllBeforeDocs] {@set [KPropertyAllBeforeDocs.Arg] \ \{ myColumn \}}
     */
    public fun <C> KProperty<C>.allColsBefore(column: ColumnSelector<C, *>): ColumnSet<*> =
        columnGroup(this).allColsBefore(column)

    /** @include [KPropertyAllBeforeDocs] {@set [KPropertyAllBeforeDocs.Arg] ("pathTo"["myColumn"])} */
    public fun KProperty<*>.allColsBefore(column: ColumnPath): ColumnSet<*> = columnGroup(this).allColsBefore(column)

    /** @include [KPropertyAllBeforeDocs] {@set [KPropertyAllBeforeDocs.Arg] ("myColumn")} */
    public fun KProperty<*>.allColsBefore(column: String): ColumnSet<*> = columnGroup(this).allColsBefore(column)

    /** @include [KPropertyAllBeforeDocs] {@set [KPropertyAllBeforeDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun KProperty<*>.allColsBefore(column: AnyColumnReference): ColumnSet<*> =
        columnGroup(this).allColsBefore(column)

    /** @include [KPropertyAllBeforeDocs] {@set [KPropertyAllBeforeDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun KProperty<*>.allColsBefore(column: KProperty<*>): ColumnSet<*> = columnGroup(this).allColsBefore(column)

    /**
     * @include [AllBeforeDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]` { "pathTo"["someColGroup"].`[allColsBefore][ColumnPath.allColsBefore]`{@get [ColumnPathAllBeforeDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface ColumnPathAllBeforeDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [ColumnPathAllBeforeDocs] {@set [ColumnPathAllBeforeDocs.Arg] \ \{ myColumn \}} */
    public fun ColumnPath.allColsBefore(column: ColumnSelector<*, *>): ColumnSet<*> =
        columnGroup(this).allColsBefore(column)

    /** @include [ColumnPathAllBeforeDocs] {@set [ColumnPathAllBeforeDocs.Arg] ("pathTo"["myColumn"])} */
    public fun ColumnPath.allColsBefore(column: ColumnPath): ColumnSet<*> = columnGroup(this).allColsBefore(column)

    /** @include [ColumnPathAllBeforeDocs] {@set [ColumnPathAllBeforeDocs.Arg] ("myColumn")} */
    public fun ColumnPath.allColsBefore(column: String): ColumnSet<*> = columnGroup(this).allColsBefore(column)

    /** @include [ColumnPathAllBeforeDocs] {@set [ColumnPathAllBeforeDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun ColumnPath.allColsBefore(column: AnyColumnReference): ColumnSet<*> =
        columnGroup(this).allColsBefore(column)

    /** @include [ColumnPathAllBeforeDocs] {@set [ColumnPathAllBeforeDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun ColumnPath.allColsBefore(column: KProperty<*>): ColumnSet<*> = columnGroup(this).allColsBefore(column)

    // endregion

    // region allUpTo

    /**
     * @include [CommonAllSubsetDocs]
     * @set [CommonAllSubsetDocs.TitleArg] All (Cols) Up To
     * @set [CommonAllSubsetDocs.FunctionArg] allUpTo
     * @set [CommonAllSubsetDocs.FunctionColsArg] allColsUpTo
     * @set [CommonAllSubsetDocs.BehaviorArg] up to [column\], including [column\] itself
     * @set [CommonAllSubsetDocs.ColumnDoesNotExistArg] the function will return a [ColumnSet][ColumnSet] containing all columns
     * @param [column\] The specified column up to which all columns should be taken. This column can be referenced
     *   to both relatively to the current [ColumnsResolver] and absolutely.
     */
    private interface AllUpToDocs

    /**
     * @include [AllUpToDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]`  {  `[cols][ColumnsSelectionDsl.cols]` { .. }.`[allUpTo][ColumnSet.allUpTo]`{@get [ColumnSetAllUpToDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface ColumnSetAllUpToDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [ColumnSetAllUpToDocs] {@set [ColumnSetAllUpToDocs.Arg] \ \{ myColumn `[in][String.contains]` it.`[name][ColumnWithPath.name]` \}} */
    @Suppress("UNCHECKED_CAST")
    public fun <C> ColumnSet<C>.allUpTo(column: ColumnFilter<C>): ColumnSet<C> =
        allUpToInternal(column as ColumnFilter<*>) as ColumnSet<C>

    /** @include [ColumnSetAllUpToDocs] {@set [ColumnSetAllUpToDocs.Arg] ("pathTo"["myColumn"])} */
    @Suppress("UNCHECKED_CAST")
    public fun <C> ColumnSet<C>.allUpTo(column: ColumnPath): ColumnSet<C> =
        allUpToInternal { it.path == column } as ColumnSet<C>

    /** @include [ColumnSetAllUpToDocs] {@set [ColumnSetAllUpToDocs.Arg] ("myColumn")} */
    public fun <C> ColumnSet<C>.allUpTo(column: String): ColumnSet<C> = allUpTo(pathOf(column))

    /** @include [ColumnSetAllUpToDocs] {@set [ColumnSetAllUpToDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun <C> ColumnSet<C>.allUpTo(column: AnyColumnReference): ColumnSet<C> = allUpTo(column.path())

    /** @include [ColumnSetAllUpToDocs] {@set [ColumnSetAllUpToDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun <C> ColumnSet<C>.allUpTo(column: KProperty<*>): ColumnSet<C> = allUpTo(column.toColumnAccessor().path())

    /**
     * @include [AllUpToDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]`  {  `[allUpTo][ColumnsSelectionDsl.allColsUpTo]`{@get [ColumnsSelectionDslAllUpToDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface ColumnsSelectionDslAllUpToDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [ColumnsSelectionDslAllUpToDocs] {@set [ColumnsSelectionDslAllUpToDocs.Arg] \ \{ myColumn \}} */
    public fun <T> ColumnsSelectionDsl<T>.allUpTo(column: ColumnSelector<T, *>): ColumnSet<*> =
        asSingleColumn().allColsUpTo(column)

    /** @include [ColumnsSelectionDslAllUpToDocs] {@set [ColumnsSelectionDslAllUpToDocs.Arg] ("pathTo"["myColumn"])} */
    public fun ColumnsSelectionDsl<*>.allUpTo(column: ColumnPath): ColumnSet<*> = asSingleColumn().allColsUpTo(column)

    /** @include [ColumnsSelectionDslAllUpToDocs] {@set [ColumnsSelectionDslAllUpToDocs.Arg] ("myColumn")} */
    public fun ColumnsSelectionDsl<*>.allUpTo(column: String): ColumnSet<*> = asSingleColumn().allColsUpTo(column)

    /** @include [ColumnsSelectionDslAllUpToDocs] {@set [ColumnsSelectionDslAllUpToDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun ColumnsSelectionDsl<*>.allUpTo(column: AnyColumnReference): ColumnSet<*> =
        asSingleColumn().allColsUpTo(column)

    /** @include [ColumnsSelectionDslAllUpToDocs] {@set [ColumnsSelectionDslAllUpToDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun ColumnsSelectionDsl<*>.allUpTo(column: KProperty<*>): ColumnSet<*> = asSingleColumn().allColsUpTo(column)

    /**
     * @include [AllUpToDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]` { someColumnGroup.`[allColsUpTo][SingleColumn.allColsUpTo]`{@get [SingleColumnAllUpToDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface SingleColumnAllUpToDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [SingleColumnAllUpToDocs] {@set [SingleColumnAllUpToDocs.Arg] \ \{ myColumn \}} */
    public fun <T> SingleColumn<DataRow<T>>.allColsUpTo(column: ColumnSelector<T, *>): ColumnSet<*> {
        var resolvedCol: DataColumn<*>? = null
        return this
            .ensureIsColumnGroup()
            .onResolve { resolvedCol = it!!.asColumnGroup().getColumn(column) }
            .allUpToInternal { it.data == resolvedCol!! }
    }

    /** @include [SingleColumnAllUpToDocs] {@set [SingleColumnAllUpToDocs.Arg] ("pathTo"["myColumn"])} */
    public fun SingleColumn<DataRow<*>>.allColsUpTo(column: ColumnPath): ColumnSet<*> {
        var path: ColumnPath? = null
        return this
            .ensureIsColumnGroup()
            .onResolve { path = it!!.path }
            .allUpToInternal {
                // accept both relative and full column path
                it.path == path!! + column || it.path == column
            }
    }

    /** @include [SingleColumnAllUpToDocs] {@set [SingleColumnAllUpToDocs.Arg] ("myColumn")} */
    public fun SingleColumn<DataRow<*>>.allColsUpTo(column: String): ColumnSet<*> = allColsUpTo(pathOf(column))

    /** @include [SingleColumnAllUpToDocs] {@set [SingleColumnAllUpToDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun SingleColumn<DataRow<*>>.allColsUpTo(column: AnyColumnReference): ColumnSet<*> =
        allColsUpTo(column.path())

    /** @include [SingleColumnAllUpToDocs] {@set [SingleColumnAllUpToDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun SingleColumn<DataRow<*>>.allColsUpTo(column: KProperty<*>): ColumnSet<*> =
        allColsUpTo(column.toColumnAccessor().path())

    /**
     * @include [AllUpToDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]` { "someColGroup".`[allColsUpTo][String.allColsUpTo]`{@get [StringAllUpToDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface StringAllUpToDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [StringAllUpToDocs] {@set [StringAllUpToDocs.Arg] \ \{ myColumn \}} */
    public fun String.allColsUpTo(column: ColumnSelector<*, *>): ColumnSet<*> = columnGroup(this).allColsUpTo(column)

    /** @include [StringAllUpToDocs] {@set [StringAllUpToDocs.Arg] ("pathTo"["myColumn"])} */
    public fun String.allColsUpTo(column: ColumnPath): ColumnSet<*> = columnGroup(this).allColsUpTo(column)

    /** @include [StringAllUpToDocs] {@set [StringAllUpToDocs.Arg] ("myColumn")} */
    public fun String.allColsUpTo(column: String): ColumnSet<*> = columnGroup(this).allColsUpTo(column)

    /** @include [StringAllUpToDocs] {@set [StringAllUpToDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun String.allColsUpTo(column: AnyColumnReference): ColumnSet<*> = columnGroup(this).allColsUpTo(column)

    /** @include [StringAllUpToDocs] {@set [StringAllUpToDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun String.allColsUpTo(column: KProperty<*>): ColumnSet<*> = columnGroup(this).allColsUpTo(column)

    /**
     * @include [AllUpToDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]` { DataSchemaType::someColGroup.`[allColsUpTo][KProperty.allColsUpTo]`{@get [KPropertyAllUpToDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface KPropertyAllUpToDocs {

        /** Example argument to use */
        interface Arg
    }

    /**
     * @include [KPropertyAllUpToDocs] {@set [KPropertyAllUpToDocs.Arg] \ \{ myColumn \}}
     */
    public fun <C> KProperty<C>.allColsUpTo(column: ColumnSelector<C, *>): ColumnSet<*> =
        columnGroup(this).allColsUpTo(column)

    /** @include [KPropertyAllUpToDocs] {@set [KPropertyAllUpToDocs.Arg] ("pathTo"["myColumn"])} */
    public fun KProperty<*>.allColsUpTo(column: ColumnPath): ColumnSet<*> = columnGroup(this).allColsUpTo(column)

    /** @include [KPropertyAllUpToDocs] {@set [KPropertyAllUpToDocs.Arg] ("myColumn")} */
    public fun KProperty<*>.allColsUpTo(column: String): ColumnSet<*> = columnGroup(this).allColsUpTo(column)

    /** @include [KPropertyAllUpToDocs] {@set [KPropertyAllUpToDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun KProperty<*>.allColsUpTo(column: AnyColumnReference): ColumnSet<*> =
        columnGroup(this).allColsUpTo(column)

    /** @include [KPropertyAllUpToDocs] {@set [KPropertyAllUpToDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun KProperty<*>.allColsUpTo(column: KProperty<*>): ColumnSet<*> = columnGroup(this).allColsUpTo(column)

    /**
     * @include [AllUpToDocs]
     * @set [CommonAllSubsetDocs.ExampleArg]
     *
     * `df.`[select][DataFrame.select]` { "pathTo"["someColGroup"].`[allColsUpTo][ColumnPath.allColsUpTo]`{@get [ColumnPathAllUpToDocs.Arg]} }`
     */
    @ExcludeFromSources
    private interface ColumnPathAllUpToDocs {

        /** Example argument to use */
        interface Arg
    }

    /** @include [ColumnPathAllUpToDocs] {@set [ColumnPathAllUpToDocs.Arg] \ \{ myColumn \}} */
    public fun ColumnPath.allColsUpTo(column: ColumnSelector<*, *>): ColumnSet<*> =
        columnGroup(this).allColsUpTo(column)

    /** @include [ColumnPathAllUpToDocs] {@set [ColumnPathAllUpToDocs.Arg] ("pathTo"["myColumn"])} */
    public fun ColumnPath.allColsUpTo(column: ColumnPath): ColumnSet<*> = columnGroup(this).allColsUpTo(column)

    /** @include [ColumnPathAllUpToDocs] {@set [ColumnPathAllUpToDocs.Arg] ("myColumn")} */
    public fun ColumnPath.allColsUpTo(column: String): ColumnSet<*> = columnGroup(this).allColsUpTo(column)

    /** @include [ColumnPathAllUpToDocs] {@set [ColumnPathAllUpToDocs.Arg] (myColumn)} */
    @AccessApiOverload
    public fun ColumnPath.allColsUpTo(column: AnyColumnReference): ColumnSet<*> = columnGroup(this).allColsUpTo(column)

    /** @include [ColumnPathAllUpToDocs] {@set [ColumnPathAllUpToDocs.Arg] (Type::myColumn)} */
    @AccessApiOverload
    public fun ColumnPath.allColsUpTo(column: KProperty<*>): ColumnSet<*> = columnGroup(this).allColsUpTo(column)

    // endregion
}

/**
 * If [this] is a [SingleColumn] containing a single [ColumnGroup], it
 * returns a [(transformable) ColumnSet][TransformableColumnSet] containing the children of this [ColumnGroup],
 * else it simply returns a [(transformable) ColumnSet][TransformableColumnSet] from [this].
 */
internal fun ColumnsResolver<*>.allColumnsInternal(removePaths: Boolean = false): TransformableColumnSet<*> =
    transform {
        if (isSingleColumnWithGroup(it)) {
            it.single().let {
                if (removePaths) {
                    it.asColumnGroup().columns().map(AnyCol::addPath)
                } else {
                    it.cols()
                }
            }
        } else {
            it
        }
    }

/**
 * Returns a new ColumnSet containing all columns after the first column that matches the given predicate.
 *
 * @param colByPredicate a function that takes a ColumnWithPath and returns true if the column matches the predicate, false otherwise
 * @return a new ColumnSet containing all columns after the first column that matches the given predicate
 */
internal fun ColumnsResolver<*>.allAfterInternal(colByPredicate: ColumnFilter<*>): ColumnSet<*> {
    var take = false
    return colsInternal {
        if (take) {
            true
        } else {
            take = colByPredicate(it)
            false
        }
    }
}

/**
 * Returns a column set containing all columns from the internal column set that satisfy the given predicate.
 *
 * @param colByPredicate the predicate used to determine if a column should be included in the resulting set
 * @return a column set containing all columns that satisfy the predicate
 */
internal fun ColumnsResolver<*>.allFromInternal(colByPredicate: ColumnFilter<*>): ColumnSet<*> {
    var take = false
    return colsInternal {
        if (take) {
            true
        } else {
            take = colByPredicate(it)
            take
        }
    }
}

/**
 * Returns a new ColumnSet containing all columns before the first column that satisfies the given predicate.
 *
 * @param colByPredicate the predicate function used to determine if a column should be included in the returned ColumnSet
 * @return a new ColumnSet containing all columns that come before the first column that satisfies the given predicate
 */
internal fun ColumnsResolver<*>.allBeforeInternal(colByPredicate: ColumnFilter<*>): ColumnSet<*> {
    var take = true
    return colsInternal {
        if (!take) {
            false
        } else {
            take = !colByPredicate(it)
            take
        }
    }
}

/**
 * Returns a ColumnSet containing all columns up to (and including) the first column that satisfies the given predicate.
 *
 * @param colByPredicate a predicate function that takes a ColumnWithPath and returns true if the column satisfies the desired condition.
 * @return a ColumnSet containing all columns up to the first column that satisfies the given predicate.
 */
internal fun ColumnsResolver<*>.allUpToInternal(colByPredicate: ColumnFilter<*>): ColumnSet<*> {
    var take = true
    return colsInternal {
        if (!take) {
            false
        } else {
            take = !colByPredicate(it)
            true
        }
    }
}

// endregion
