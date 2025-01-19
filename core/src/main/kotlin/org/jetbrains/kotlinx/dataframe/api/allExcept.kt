package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.ColumnsSelector
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.DataRow
import org.jetbrains.kotlinx.dataframe.annotations.AccessApiOverload
import org.jetbrains.kotlinx.dataframe.columns.ColumnGroup
import org.jetbrains.kotlinx.dataframe.columns.ColumnPath
import org.jetbrains.kotlinx.dataframe.columns.ColumnSet
import org.jetbrains.kotlinx.dataframe.columns.ColumnWithPath
import org.jetbrains.kotlinx.dataframe.columns.ColumnsResolver
import org.jetbrains.kotlinx.dataframe.columns.SingleColumn
import org.jetbrains.kotlinx.dataframe.columns.toColumnSet
import org.jetbrains.kotlinx.dataframe.documentation.AccessApi.ExtensionPropertiesApiLink
import org.jetbrains.kotlinx.dataframe.documentation.AccessApiLink
import org.jetbrains.kotlinx.dataframe.documentation.DslGrammarTemplateColumnsSelectionDsl.DslGrammarTemplate
import org.jetbrains.kotlinx.dataframe.documentation.Indent
import org.jetbrains.kotlinx.dataframe.documentation.LineBreak
import org.jetbrains.kotlinx.dataframe.impl.aggregation.toColumns
import org.jetbrains.kotlinx.dataframe.impl.columns.allColumnsExceptKeepingStructure
import org.jetbrains.kotlinx.dataframe.impl.columns.changePath
import org.jetbrains.kotlinx.dataframe.impl.columns.createColumnSet
import org.jetbrains.kotlinx.dataframe.impl.columns.transformSingle
import org.jetbrains.kotlinx.dataframe.util.ALL_COLS_EXCEPT
import org.jetbrains.kotlinx.dataframe.util.ALL_COLS_REPLACE
import org.jetbrains.kotlinx.dataframe.util.ALL_COLS_REPLACE_VARARG
import kotlin.reflect.KProperty

// region ColumnsSelectionDsl

/**
 * ## (All) (Cols) Except {@include [ColumnsSelectionDslLink]}
 *
 * See [Grammar] for all functions in this interface.
 */
public interface AllExceptColumnsSelectionDsl {

    /**
     * ## (All) (Cols) Except Grammar
     *
     * @include [DslGrammarTemplate]
     * {@set [DslGrammarTemplate.DefinitionsArg]
     *  {@include [DslGrammarTemplate.ColumnSetDef]}
     *  {@include [LineBreak]}
     *  {@include [DslGrammarTemplate.ColumnGroupDef]}
     *  {@include [LineBreak]}
     *  {@include [DslGrammarTemplate.ColumnsSelectorDef]}
     *  {@include [LineBreak]}
     *  {@include [DslGrammarTemplate.ColumnDef]}
     *  {@include [LineBreak]}
     *  {@include [DslGrammarTemplate.ColumnNoAccessorDef]}
     *  {@include [LineBreak]}
     *  {@include [DslGrammarTemplate.ColumnsResolverDef]}
     * }
     *
     * {@set [DslGrammarTemplate.PlainDslFunctionsArg]
     *  {@include [PlainDslName]}**`   {   `**{@include [DslGrammarTemplate.ColumnsSelectorRef]}**` \}`**
     *
     *  `| `{@include [PlainDslName]}**`(`**{@include [DslGrammarTemplate.ColumnRef]}**`,`**` ..`**`)`**
     * }
     * {@set [DslGrammarTemplate.ColumnSetFunctionsArg]
     *  {@include [Indent]}{@include [ColumnSetName]}` [`**`  {  `**`] `{@include [DslGrammarTemplate.ColumnsResolverRef]}` [`**`  \}  `**`]`
     *
     *  {@include [Indent]}`| `{@include [ColumnSetName]}` `{@include [DslGrammarTemplate.ColumnRef]}
     *
     *  {@include [Indent]}`| `**`.`**{@include [ColumnSetName]}**`(`**{@include [DslGrammarTemplate.ColumnRef]}**`,`**` ..`**`)`**
     * }
     * {@set [DslGrammarTemplate.ColumnGroupFunctionsArg]
     *  {@include [Indent]}{@include [ColumnGroupName]}**`  {  `**{@include [DslGrammarTemplate.ColumnsSelectorRef]}**`  \}  `**
     *
     *  {@include [Indent]}`| `{@include [ColumnGroupName]}**`(`**{@include [DslGrammarTemplate.ColumnNoAccessorRef]}**`,`**` ..`**`)`**
     *
     *  {@include [Indent]}`| `{@include [ColumnGroupExperimentalName]}**`  {  `**{@include [DslGrammarTemplate.ColumnsSelectorRef]}**` \} EXPERIMENTAL!`**
     *
     *  {@include [Indent]}`| `{@include [ColumnGroupExperimentalName]}**`(`**{@include [DslGrammarTemplate.ColumnNoAccessorRef]}**`,`**` ..`**`) EXPERIMENTAL!`**
     * }
     */
    public interface Grammar {

        /** [**`allExcept`**][ColumnsSelectionDsl.allExcept] */
        public interface PlainDslName

        /** [**`except`**][ColumnsSelectionDsl.except] */
        public interface ColumnSetName

        /** __`.`__[**`allColsExcept`**][ColumnsSelectionDsl.allColsExcept] */
        public interface ColumnGroupName

        /** [**`exceptNew`**][ColumnsSelectionDsl.exceptNew] */
        public interface ColumnGroupExperimentalName
    }

    /**
     * ## (All) (Cols) Except
     *
     * Perform a selection of columns using the {@include [ColumnsSelectionDslLink]} to
     * exclude from the current selection.
     *
     * ### Check out: [Grammar]
     *
     * ### On [ColumnSets][ColumnSet]
     * This function can be explained the easiest with [ColumnSets][ColumnSet]. Let's say we want all
     * [Int] columns apart from `age`  and  `height`.
     *
     * We can do:
     *
     * `df.`[select][DataFrame.select]`  {  `[colsOf][colsOf]`<`[Int][Int]`>() `[except][ColumnSet.except]`  (age  `[and][ColumnsSelectionDsl.and]` height) }`
     *
     * which will 'subtract' the [ColumnSet] created by `age `[and][ColumnsSelectionDsl.and]` height` from the [ColumnSet] created by [colsOf][colsOf]`<`[Int][Int]`>()`.
     *
     * {@include [LineBreak]}
     * This operation can also be used to exclude columns from [Column Groups][ColumnGroup].
     *
     * For instance:
     *
     * `df.`[select][DataFrame.select]`  {  `[colsAtAnyDepth][ColumnsSelectionDsl.colsAtAnyDepth]`  { "a"  `[in][String.contains]` it.`[name][DataColumn.name]`() } `[except][ColumnSet.except]` userData.age }`
     * {@include [LineBreak]}
     *
     * Note that the selection of columns to exclude from [column sets][ColumnSet] is always done relative to the outer
     * scope. Use the {@include [ExtensionPropertiesApiLink]} to prevent scoping issues if possible.
     * {@include [LineBreak]}
     * Special case: If a column that needs to be removed appears multiple times in the [ColumnSet], it is excepted
     * each time it is encountered (including inside [ColumnGroups][ColumnGroup]). You could say the receiver [ColumnSet]
     * is [simplified][ColumnsSelectionDsl.simplify] before the operation is performed:
     *
     * [cols][ColumnsSelectionDsl.cols]`(a, a, a.b, a.b).`[except][ColumnSet.except]`(a.b)`
     *
     * `== `[cols][ColumnsSelectionDsl.cols]`(a).`[except][ColumnSet.except]`(a.b)`
     *
     * ### In the [ColumnsSelectionDsl][ColumnsSelectionDsl]
     * Instead of having to write [all][ColumnsSelectionDsl.all]`() `[except][ColumnsSelectionDsl.except]` { ... }` in the DSL,
     * you can use [allExcept][ColumnsSelectionDsl.allExcept]` { ... }` to achieve the same result.
     *
     * For example:
     *
     * `df.`[select][DataFrame.select]`  {  `[allExcept][ColumnsSelectionDsl.allExcept]`  { userData.age  `[and][ColumnsSelectionDsl.and]` height } }`
     *
     * ### On [ColumnGroups][ColumnGroup]
     * The variant of this function on [ColumnGroups][ColumnGroup] is a bit different as it changes the scope relative to
     * the column group.
     * {@include [LineBreak]}
     * In other words:
     *
     * `df.`[select][DataFrame.select]` { myColGroup.`[allColsExcept][SingleColumn.allColsExcept]`  { colA  `[and][ColumnsSelectionDsl.and]` colB } }`
     *
     * is shorthand for
     *
     * `df.`[select][DataFrame.select]` { myColGroup.`[select][ColumnsSelectionDsl.select]`  {  `[all][ColumnsSelectionDsl.all]`() `[except][ColumnSet.except]`  { colA  `[and][ColumnsSelectionDsl.and]` colB } } }`
     *
     * or
     *
     * `df.`[select][DataFrame.select]` { myColGroup.`[allCols][ColumnsSelectionDsl.allCols]`() `[except][ColumnSet.except]`  { myColGroup.colA  `[and][ColumnsSelectionDsl.and]` myColGroup.colB } }`
     * {@include [LineBreak]}
     * Also note the name change, similar to [allCols][ColumnsSelectionDsl.allCols], this makes it clearer that you're selecting
     * columns inside the group, 'lifting' them out.
     *
     * ### Examples for this overload
     * {@get [ExampleArg]}
     *
     * {@get [ParamArg]}
     * @return A [ColumnSet] containing all columns in [this\] except the specified ones.
     * @see ColumnsSelectionDsl.select
     * @see ColumnsSelectionDsl.all
     * @see ColumnsSelectionDsl.allBefore
     * @see ColumnsSelectionDsl.allAfter
     * @see ColumnsSelectionDsl.allUpTo
     * @see ColumnsSelectionDsl.allFrom
     */
    private interface CommonExceptDocs {

        // Example argument
        interface ExampleArg

        // Parameter argument
        interface ParamArg
    }

    // region ColumnSet

    /**
     * @include [CommonExceptDocs]
     * {@set [CommonExceptDocs.ExampleArg]
     *  `df.`[select][ColumnsSelectionDsl.select]`  {  `[colsOf][ColumnsSelectionDsl.colsOf]`<`[Number][Number]`>() `[except][ColumnSet.except]` `{@get [ArgumentArg1]}` \}`
     *
     *  `df.`[select][ColumnsSelectionDsl.select]`  {  `[cols][ColumnsSelectionDsl.cols]`(name, age) `[except][ColumnSet.except]` `{@get [ArgumentArg2]}` \}`
     * }
     */
    private interface ColumnSetInfixDocs {

        // argument
        interface ArgumentArg1

        // argument
        interface ArgumentArg2
    }

    /**
     * @include [CommonExceptDocs]
     * {@set [CommonExceptDocs.ExampleArg]
     *  `df.`[select][ColumnsSelectionDsl.select]`  {  `[colsOf][ColumnsSelectionDsl.colsOf]`<`[Number][Number]`>().`[except][ColumnSet.except]{@get [ArgumentArg1]}` \}`
     *
     *  `df.`[select][ColumnsSelectionDsl.select]`  {  `[cols][ColumnsSelectionDsl.cols]`(name, age).`[except][ColumnSet.except]{@get [ArgumentArg2]}` \}`
     * }
     */
    private interface ColumnSetVarargDocs {

        // argument
        interface ArgumentArg1

        // argument
        interface ArgumentArg2
    }

    /**
     * @include [ColumnSetInfixDocs]
     * @set [CommonExceptDocs.ParamArg] @param [selector\] A lambda in which you specify the columns that need to be
     *   excluded from the [ColumnSet]. The scope of the selector is the same as the outer scope.
     * @set [ColumnSetInfixDocs.ArgumentArg1] `{ "age" `[and][ColumnsSelectionDsl.and]` height }`
     * @set [ColumnSetInfixDocs.ArgumentArg2] `{ name.firstName }`
     */
    public infix fun <C> ColumnSet<C>.except(selector: () -> ColumnsResolver<*>): ColumnSet<C> = except(selector())

    /**
     * @include [ColumnSetInfixDocs]
     * @set [CommonExceptDocs.ParamArg] @param [other\] A [ColumnsResolver] containing the columns that need to be
     *   excluded from the [ColumnSet].
     * @set [ColumnSetInfixDocs.ArgumentArg1] `"age" `[and][ColumnsSelectionDsl.and]` height`
     * @set [ColumnSetInfixDocs.ArgumentArg2] `name.firstName`
     */
    public infix fun <C> ColumnSet<C>.except(other: ColumnsResolver<*>): ColumnSet<C> = exceptInternal(other)

    /**
     * @include [ColumnSetVarargDocs]
     * @set [CommonExceptDocs.ParamArg] @param [others\] Any number of [ColumnsResolvers][ColumnsResolver] containing
     *  the columns that need to be excluded from the [ColumnSet].
     * @set [ColumnSetVarargDocs.ArgumentArg1] `(age, userData.height)`
     * @set [ColumnSetVarargDocs.ArgumentArg2] `(name.firstName, name.middleName)`
     */
    public fun <C> ColumnSet<C>.except(vararg others: ColumnsResolver<*>): ColumnSet<C> = except(others.toColumnSet())

    /**
     * @include [ColumnSetInfixDocs]
     * @set [CommonExceptDocs.ParamArg] @param [other\] A [String] referring to
     *  the column (relative to the current scope) that needs to be excluded from the [ColumnSet].
     * @set [ColumnSetInfixDocs.ArgumentArg1] `"age"`
     * @set [ColumnSetInfixDocs.ArgumentArg2] `"name"`
     */
    public infix fun <C> ColumnSet<C>.except(other: String): ColumnSet<C> = except(column<Any?>(other))

    /**
     * @include [ColumnSetVarargDocs]
     * @set [CommonExceptDocs.ParamArg] @param [others\] Any number of [Strings][String] referring to
     *  the columns (relative to the current scope) that need to be excluded from the [ColumnSet].
     * @set [ColumnSetVarargDocs.ArgumentArg1] `("age", "height")`
     * @set [ColumnSetVarargDocs.ArgumentArg2] `("name")`
     */
    public fun <C> ColumnSet<C>.except(vararg others: String): ColumnSet<C> = except(others.toColumnSet())

    /**
     * @include [ColumnSetInfixDocs]
     * @set [CommonExceptDocs.ParamArg] @param [other\] A [KProperty] referring to
     *  the column (relative to the current scope) that needs to be excluded from the [ColumnSet].
     * @set [ColumnSetInfixDocs.ArgumentArg1] `Person::age`
     * @set [ColumnSetInfixDocs.ArgumentArg2] `Person::name`
     */
    @AccessApiOverload
    public infix fun <C> ColumnSet<C>.except(other: KProperty<C>): ColumnSet<C> = except(column(other))

    /**
     * @include [ColumnSetVarargDocs]
     * @set [CommonExceptDocs.ParamArg] @param [others\] Any number of [KProperties][KProperty] referring to
     *  the columns (relative to the current scope) that need to be excluded from the [ColumnSet].
     * @set [ColumnSetVarargDocs.ArgumentArg1] `(Person::age, Person::height)`
     * @set [ColumnSetVarargDocs.ArgumentArg2] `(Person::name)`
     */
    @AccessApiOverload
    public fun <C> ColumnSet<C>.except(vararg others: KProperty<C>): ColumnSet<C> = except(others.toColumnSet())

    /**
     * @include [ColumnSetInfixDocs]
     * @set [CommonExceptDocs.ParamArg] @param [other\] A [ColumnPath] referring to
     *  the column (relative to the current scope) that needs to be excluded from the [ColumnSet].
     * @set [ColumnSetInfixDocs.ArgumentArg1] `"userdata"["age"]`
     * @set [ColumnSetInfixDocs.ArgumentArg2] `pathOf("name", "firstName")`
     */
    public infix fun <C> ColumnSet<C>.except(other: ColumnPath): ColumnSet<C> = except(column<Any?>(other))

    /**
     * @include [ColumnSetVarargDocs]
     * @set [CommonExceptDocs.ParamArg] @param [others\] Any number of [ColumnPaths][ColumnPath] referring to
     *  the columns (relative to the current scope) that need to be excluded from the [ColumnSet].
     * @set [ColumnSetVarargDocs.ArgumentArg1] `(pathOf("age"), "userdata"["height"])`
     * @set [ColumnSetVarargDocs.ArgumentArg2] `("name"["firstName"], "name"["middleName"])`
     */
    public fun <C> ColumnSet<C>.except(vararg others: ColumnPath): ColumnSet<C> = except(others.toColumnSet())

    // endregion

    // region ColumnsSelectionDsl

    /**
     * @include [CommonExceptDocs]
     * @set [CommonExceptDocs.ExampleArg]
     *  `df.`[select][ColumnsSelectionDsl.select]`  {  `[allExcept][ColumnsSelectionDsl.allExcept]{@get [ArgumentArg1]}` \}`
     *
     *  `df.`[select][ColumnsSelectionDsl.select]`  {  `[allExcept][ColumnsSelectionDsl.allExcept]{@get [ArgumentArg2]}` \}`
     */
    private interface ColumnsSelectionDslDocs {

        // argument
        interface ArgumentArg1

        // argument
        interface ArgumentArg2
    }

    /**
     * @include [ColumnsSelectionDslDocs]
     * @set [CommonExceptDocs.ParamArg] @param [selector\] A lambda in which you specify the columns that need to be
     *  excluded from the current selection. The scope of the selector is the same as the outer scope.
     * @set [ColumnsSelectionDslDocs.ArgumentArg1] `  { "age"  `[and][ColumnsSelectionDsl.and]` height }`
     * @set [ColumnsSelectionDslDocs.ArgumentArg2] ` { name.firstName }`
     */
    public fun <C> ColumnsSelectionDsl<C>.allExcept(selector: ColumnsSelector<C, *>): ColumnSet<*> =
        this.asSingleColumn().allColsExcept(selector)

    /**
     * {@comment No scoping issues, this function can exist for legacy purposes}
     * @include [ColumnsSelectionDslDocs]
     * @set [CommonExceptDocs.ParamArg] @param [others\] A [ColumnsResolver] containing the columns that need to be
     *  excluded from the current selection.
     * @set [ColumnsSelectionDslDocs.ArgumentArg1] `(age, height)`
     * @set [ColumnsSelectionDslDocs.ArgumentArg2] `(name.firstName, name.middleName)`
     */
    public fun ColumnsSelectionDsl<*>.allExcept(vararg others: ColumnsResolver<*>): ColumnSet<*> =
        asSingleColumn().allColsExceptInternal(others.toColumnSet())

    /**
     * @include [ColumnsSelectionDslDocs]
     * @set [CommonExceptDocs.ParamArg] @param [others\] Any number of [Strings][String] referring to
     *  the columns (relative to the current scope) that need to be excluded from the current selection.
     * @set [ColumnsSelectionDslDocs.ArgumentArg1] `("age", "height")`
     * @set [ColumnsSelectionDslDocs.ArgumentArg2] `("name")`
     */
    public fun ColumnsSelectionDsl<*>.allExcept(vararg others: String): ColumnSet<*> =
        asSingleColumn().allColsExceptInternal(others.toColumnSet())

    /**
     * @include [ColumnsSelectionDslDocs]
     * @set [CommonExceptDocs.ParamArg] @param [others\] Any number of [KProperties][KProperty] referring to
     *  the columns (relative to the current scope) that need to be excluded from the current selection.
     * @set [ColumnsSelectionDslDocs.ArgumentArg1] `(Person::age, Person::height)`
     * @set [ColumnsSelectionDslDocs.ArgumentArg2] `(Person::name)`
     */
    @AccessApiOverload
    public fun ColumnsSelectionDsl<*>.allExcept(vararg others: KProperty<*>): ColumnSet<*> =
        asSingleColumn().allColsExceptInternal(others.toColumnSet())

    /**
     * @include [ColumnsSelectionDslDocs]
     * @set [CommonExceptDocs.ParamArg] @param [others\] Any number of [ColumnPaths][ColumnPath] referring to
     *  the columns (relative to the current scope) that need to be excluded from the current selection.
     * @set [ColumnsSelectionDslDocs.ArgumentArg1] `(pathOf("age"), "userdata"["height"])`
     * @set [ColumnsSelectionDslDocs.ArgumentArg2] `("name"["firstName"], "name"["middleName"])`
     */
    public fun ColumnsSelectionDsl<*>.allExcept(vararg others: ColumnPath): ColumnSet<*> =
        asSingleColumn().allColsExceptInternal(others.toColumnSet())

    // endregion

    // region SingleColumn

    /**
     * @include [CommonExceptDocs]
     * @set [CommonExceptDocs.ExampleArg] {@comment <code> blocks are there to prevent double ``}
     *  `df.`[select][ColumnsSelectionDsl.select]`  {  `<code>{@get [ReceiverArg1]}</code>[allColsExcept][{@get [ReceiverType]}.allColsExcept]<code>{@get [ArgumentArg1]}</code>` \}`
     *
     *  `df.`[select][ColumnsSelectionDsl.select]`  { city  `[and][ColumnsSelectionDsl.and]` `<code>{@get [ReceiverArg2]}</code>[allColsExcept][{@get [ReceiverType]}.allColsExcept]<code>{@get [ArgumentArg2]}</code>` \}`
     */
    private interface ColumnGroupDocs {

        // receiver
        interface ReceiverArg1

        // receiver
        interface ReceiverArg2

        // type
        interface ReceiverType

        // argument
        interface ArgumentArg1

        // argument
        interface ArgumentArg2

        /**
         * @set [ColumnGroupDocs.ReceiverArg1] `userData.`
         * @set [ColumnGroupDocs.ReceiverArg2] `name.`
         * @set [ColumnGroupDocs.ReceiverType] SingleColumn
         */
        interface SingleColumnReceiverArgs

        /**
         * @set [ColumnGroupDocs.ReceiverArg1] `"userData".`
         * @set [ColumnGroupDocs.ReceiverArg2] `"name".`
         * @set [ColumnGroupDocs.ReceiverType] String
         */
        interface StringReceiverArgs

        /**
         * @set [ColumnGroupDocs.ReceiverArg1] `DataSchemaPerson::userData.`
         * @set [ColumnGroupDocs.ReceiverArg2] `Person::name.`
         * @set [ColumnGroupDocs.ReceiverType] KProperty
         */
        interface KPropertyReceiverArgs

        /**
         * @set [ColumnGroupDocs.ReceiverArg1] `pathOf("userData").`
         * @set [ColumnGroupDocs.ReceiverArg2] `"pathTo"["myColGroup"].`
         * @set [ColumnGroupDocs.ReceiverType] ColumnPath
         */
        interface ColumnPathReceiverArgs

        /**
         * @set [CommonExceptDocs.ParamArg] @param [selector\] A lambda in which you specify the columns that need to be
         *  excluded from the current selection in [this\] column group. The other columns will be included in the selection
         *  by default. The scope of the selector is relative to the column group.
         * @set [ColumnGroupDocs.ArgumentArg1] `  { "age"  `[and][ColumnsSelectionDsl.and]` height }`
         * @set [ColumnGroupDocs.ArgumentArg2] ` { firstName }`
         */
        interface SelectorArgs

        /**
         * @set [CommonExceptDocs.ParamArg] @param [others\] Any number of [Strings][String] referring to
         *  the columns (relative to the column group) that need to be excluded from the current selection in [this\]
         *  column group. The other columns will be included in the selection by default.
         * @set [ColumnGroupDocs.ArgumentArg1] `("age", "height")`
         * @set [ColumnGroupDocs.ArgumentArg2] `("firstName", "middleName")`
         */
        interface StringArgs

        /**
         * @set [CommonExceptDocs.ParamArg] @param [others\] Any number of [KProperties][KProperty] referring to
         *  the columns (relative to the column group) that need to be excluded from the current selection in [this\]
         *  column group. The other columns will be included in the selection by default.
         * @set [ColumnGroupDocs.ArgumentArg1] `(Person::age, Person::height)`
         * @set [ColumnGroupDocs.ArgumentArg2] `(Person::firstName, Person::middleName)`
         */
        interface KPropertyArgs

        /**
         * @set [CommonExceptDocs.ParamArg] @param [others\] Any number of [ColumnPaths][ColumnPath] referring to
         *  the columns (relative to the column group) that need to be excluded from the current selection in [this\]
         *  column group. The other columns will be included in the selection by default.
         * @set [ColumnGroupDocs.ArgumentArg1] `(pathOf("age"), "extraData"["item1"])`
         * @set [ColumnGroupDocs.ArgumentArg2] `(pathOf("firstName"), "middleNames"["first"])`
         */
        interface ColumnPathArgs
    }

    /**
     * @include [ColumnGroupDocs]
     * @include [ColumnGroupDocs.SingleColumnReceiverArgs]
     * @include [ColumnGroupDocs.SelectorArgs]
     */
    public fun <C> SingleColumn<DataRow<C>>.allColsExcept(selector: ColumnsSelector<C, *>): ColumnSet<*> =
        allColsExceptInternal(selector.toColumns())

    @Deprecated(
        message = ALL_COLS_EXCEPT,
        replaceWith = ReplaceWith(ALL_COLS_REPLACE),
        level = DeprecationLevel.ERROR,
    ) // present solely to redirect users to the right function
    public fun SingleColumn<DataRow<*>>.allColsExcept(other: ColumnsResolver<*>): ColumnSet<*> =
        allColsExcept { other }

    @Deprecated(
        message = ALL_COLS_EXCEPT,
        replaceWith = ReplaceWith(ALL_COLS_REPLACE_VARARG),
        level = DeprecationLevel.ERROR,
    ) // present solely to redirect users to the right function
    public fun SingleColumn<DataRow<*>>.allColsExcept(vararg others: ColumnsResolver<*>): ColumnSet<*> =
        allColsExcept { others.toColumnSet() }

    /**
     * @include [ColumnGroupDocs]
     * @include [ColumnGroupDocs.SingleColumnReceiverArgs]
     * @include [ColumnGroupDocs.StringArgs]
     */
    public fun SingleColumn<DataRow<*>>.allColsExcept(vararg others: String): ColumnSet<*> =
        allColsExceptInternal(others.toColumnSet())

    /**
     * @include [ColumnGroupDocs]
     * @include [ColumnGroupDocs.SingleColumnReceiverArgs]
     * @include [ColumnGroupDocs.KPropertyArgs]
     */
    @AccessApiOverload
    public fun SingleColumn<DataRow<*>>.allColsExcept(vararg others: KProperty<*>): ColumnSet<*> =
        allColsExceptInternal(others.toColumnSet())

    /**
     * @include [ColumnGroupDocs]
     * @include [ColumnGroupDocs.SingleColumnReceiverArgs]
     * @include [ColumnGroupDocs.ColumnPathArgs]
     */
    public fun SingleColumn<DataRow<*>>.allColsExcept(vararg other: ColumnPath): ColumnSet<*> =
        allColsExceptInternal(other.toColumnSet())

    // endregion

    // region String

    /**
     * @include [ColumnGroupDocs]
     * @include [ColumnGroupDocs.StringReceiverArgs]
     * @include [ColumnGroupDocs.SelectorArgs]
     */
    public fun String.allColsExcept(selector: ColumnsSelector<*, *>): ColumnSet<*> =
        columnGroup(this).allColsExcept(selector)

    @Deprecated(
        message = ALL_COLS_EXCEPT,
        replaceWith = ReplaceWith(ALL_COLS_REPLACE),
        level = DeprecationLevel.ERROR,
    ) // present solely to redirect users to the right function
    public fun String.allColsExcept(other: ColumnsResolver<*>): ColumnSet<*> =
        allColsExcept { other }

    @Deprecated(
        message = ALL_COLS_EXCEPT,
        replaceWith = ReplaceWith(ALL_COLS_REPLACE_VARARG),
        level = DeprecationLevel.ERROR,
    ) // present solely to redirect users to the right function
    public fun String.allColsExcept(vararg others: ColumnsResolver<*>): ColumnSet<*> =
        allColsExcept { others.toColumnSet() }

    /**
     * @include [ColumnGroupDocs]
     * @include [ColumnGroupDocs.StringReceiverArgs]
     * @include [ColumnGroupDocs.StringArgs]
     */
    public fun String.allColsExcept(vararg others: String): ColumnSet<*> =
        columnGroup(this).allColsExceptInternal(others.toColumnSet())

    /**
     * @include [ColumnGroupDocs]
     * @include [ColumnGroupDocs.StringReceiverArgs]
     * @include [ColumnGroupDocs.KPropertyArgs]
     */
    @AccessApiOverload
    public fun String.allColsExcept(vararg others: KProperty<*>): ColumnSet<*> =
        columnGroup(this).allColsExceptInternal(others.toColumnSet())

    /**
     * @include [ColumnGroupDocs]
     * @include [ColumnGroupDocs.StringReceiverArgs]
     * @include [ColumnGroupDocs.ColumnPathArgs]
     */
    public fun String.allColsExcept(vararg others: ColumnPath): ColumnSet<*> =
        columnGroup(this).allColsExceptInternal(others.toColumnSet())

    // endregion

    // region KProperty

    /**
     * @include [ColumnGroupDocs]
     * @include [ColumnGroupDocs.KPropertyReceiverArgs]
     * @include [ColumnGroupDocs.SelectorArgs]
     */
    public fun <C> KProperty<C>.allColsExcept(selector: ColumnsSelector<C, *>): ColumnSet<*> =
        columnGroup(this).allColsExcept(selector)

    @Deprecated(
        message = ALL_COLS_EXCEPT,
        replaceWith = ReplaceWith(ALL_COLS_REPLACE),
        level = DeprecationLevel.ERROR,
    ) // present solely to redirect users to the right function
    public fun KProperty<*>.allColsExcept(other: ColumnsResolver<*>): ColumnSet<*> =
        allColsExcept { other }

    @Deprecated(
        message = ALL_COLS_EXCEPT,
        replaceWith = ReplaceWith(ALL_COLS_REPLACE_VARARG),
        level = DeprecationLevel.ERROR,
    ) // present solely to redirect users to the right function
    public fun KProperty<*>.allColsExcept(vararg others: ColumnsResolver<*>): ColumnSet<*> =
        allColsExcept { others.toColumnSet() }

    /**
     * @include [ColumnGroupDocs]
     * @include [ColumnGroupDocs.KPropertyReceiverArgs]
     * @include [ColumnGroupDocs.StringArgs]
     */
    public fun KProperty<*>.allColsExcept(vararg others: String): ColumnSet<*> =
        columnGroup(this).allColsExceptInternal(others.toColumnSet())

    /**
     * @include [ColumnGroupDocs]
     * @include [ColumnGroupDocs.KPropertyReceiverArgs]
     * @include [ColumnGroupDocs.KPropertyArgs]
     */
    @AccessApiOverload
    public fun KProperty<*>.allColsExcept(vararg others: KProperty<*>): ColumnSet<*> =
        columnGroup(this).allColsExceptInternal(others.toColumnSet())

    /**
     * @include [ColumnGroupDocs]
     * @include [ColumnGroupDocs.KPropertyReceiverArgs]
     * @include [ColumnGroupDocs.ColumnPathArgs]
     */
    public fun KProperty<*>.allColsExcept(vararg others: ColumnPath): ColumnSet<*> =
        columnGroup(this).allColsExceptInternal(others.toColumnSet())

    // endregion

    // region ColumnPath

    /**
     * @include [ColumnGroupDocs]
     * @include [ColumnGroupDocs.ColumnPathReceiverArgs]
     * @include [ColumnGroupDocs.SelectorArgs]
     */
    public fun ColumnPath.allColsExcept(selector: ColumnsSelector<*, *>): ColumnSet<*> =
        columnGroup(this).allColsExcept(selector)

    @Deprecated(
        message = ALL_COLS_EXCEPT,
        replaceWith = ReplaceWith(ALL_COLS_REPLACE),
        level = DeprecationLevel.ERROR,
    ) // present solely to redirect users to the right function
    public fun ColumnPath.allColsExcept(other: ColumnsResolver<*>): ColumnSet<*> =
        allColsExcept { other }

    @Deprecated(
        message = ALL_COLS_EXCEPT,
        replaceWith = ReplaceWith(ALL_COLS_REPLACE_VARARG),
        level = DeprecationLevel.ERROR,
    ) // present solely to redirect users to the right function
    public fun ColumnPath.allColsExcept(vararg others: ColumnsResolver<*>): ColumnSet<*> =
        allColsExcept { others.toColumnSet() }

    /**
     * @include [ColumnGroupDocs]
     * @include [ColumnGroupDocs.ColumnPathReceiverArgs]
     * @include [ColumnGroupDocs.StringArgs]
     */
    public fun ColumnPath.allColsExcept(vararg others: String): ColumnSet<*> =
        columnGroup(this).allColsExceptInternal(others.toColumnSet())

    /**
     * @include [ColumnGroupDocs]
     * @include [ColumnGroupDocs.ColumnPathReceiverArgs]
     * @include [ColumnGroupDocs.KPropertyArgs]
     */
    @AccessApiOverload
    public fun ColumnPath.allColsExcept(vararg others: KProperty<*>): ColumnSet<*> =
        columnGroup(this).allColsExceptInternal(others.toColumnSet())

    /**
     * @include [ColumnGroupDocs]
     * @include [ColumnGroupDocs.ColumnPathReceiverArgs]
     * @include [ColumnGroupDocs.ColumnPathArgs]
     */
    public fun ColumnPath.allColsExcept(vararg others: ColumnPath): ColumnSet<*> =
        columnGroup(this).allColsExceptInternal(others.toColumnSet())

    // endregion

    // region experiments

    /**
     * ## EXPERIMENTAL: Except on Column Group
     *
     * Selects the current column group itself, except for the specified columns. This is different from
     * [allColsExcept] in that it does not 'lift' the columns out of the group, but instead selects the group itself.
     *
     * As usual, all overloads for each {@include [AccessApiLink]} are available.
     *
     * These produce the same result:
     *
     * `df.`[select][DataFrame.select]`  {  `[cols][ColumnsSelectionDsl.cols]`(colGroup) `[except][ColumnSet.except]` colGroup.col }`
     *
     * `df.`[select][DataFrame.select]`  { colGroup  `[exceptNew][SingleColumn.except]` { col } }`
     *
     * These functions are experimental and may be removed or changed in the future.
     *
     * Trying these functions requires you to `@`[`OptIn`][OptIn]`(`[ExperimentalExceptCsDsl][ExperimentalExceptCsDsl]`::class)` first.
     *
     * ## NOTE:
     * `exceptNew` will likely be renamed to `except` when the deprecated [SingleColumn.except] functions
     * are deleted.
     */
    private interface ExperimentalExceptDocs

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public infix fun <C> SingleColumn<DataRow<C>>.exceptNew(selector: ColumnsSelector<C, *>): SingleColumn<DataRow<C>> =
        exceptExperimentalInternal(selector.toColumns())

    @ExperimentalExceptCsDsl
    @Deprecated(
        message = ALL_COLS_EXCEPT,
        replaceWith = ReplaceWith("exceptNew { other }"),
        level = DeprecationLevel.ERROR,
    ) // present solely to redirect users to the right function
    public infix fun <C> SingleColumn<DataRow<C>>.exceptNew(other: ColumnsResolver<*>): SingleColumn<DataRow<C>> =
        exceptNew { other }

    @ExperimentalExceptCsDsl
    @Deprecated(
        message = ALL_COLS_EXCEPT,
        replaceWith = ReplaceWith("exceptNew { others.toColumnSet() }"),
        level = DeprecationLevel.ERROR,
    ) // present solely to redirect users to the right function
    public fun <C> SingleColumn<DataRow<C>>.exceptNew(vararg others: ColumnsResolver<*>): SingleColumn<DataRow<C>> =
        exceptNew { others.toColumnSet() }

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public infix fun <C> SingleColumn<DataRow<C>>.exceptNew(other: String): SingleColumn<DataRow<C>> =
        exceptExperimentalInternal(column<Any?>(other))

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public fun <C> SingleColumn<DataRow<C>>.exceptNew(vararg others: String): SingleColumn<DataRow<C>> =
        exceptExperimentalInternal(others.toColumnSet())

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    @AccessApiOverload
    public infix fun <C> SingleColumn<DataRow<C>>.exceptNew(other: KProperty<C>): SingleColumn<DataRow<C>> =
        exceptExperimentalInternal(column(other))

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    @AccessApiOverload
    public fun <C> SingleColumn<DataRow<C>>.exceptNew(vararg others: KProperty<*>): SingleColumn<DataRow<C>> =
        exceptExperimentalInternal(others.toColumnSet())

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public infix fun <C> SingleColumn<DataRow<C>>.exceptNew(other: ColumnPath): SingleColumn<DataRow<C>> =
        exceptExperimentalInternal(column<Any?>(other))

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public fun <C> SingleColumn<DataRow<C>>.exceptNew(vararg others: ColumnPath): SingleColumn<DataRow<C>> =
        exceptExperimentalInternal(others.toColumnSet())

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public infix fun String.exceptNew(selector: ColumnsSelector<*, *>): SingleColumn<DataRow<*>> =
        columnGroup(this).exceptNew(selector)

    @ExperimentalExceptCsDsl
    @Deprecated(
        message = ALL_COLS_EXCEPT,
        replaceWith = ReplaceWith("exceptNew { other }"),
        level = DeprecationLevel.ERROR,
    ) // present solely to redirect users to the right function
    public infix fun String.exceptNew(other: ColumnsResolver<*>): SingleColumn<DataRow<*>> =
        exceptNew { other }

    @ExperimentalExceptCsDsl
    @Deprecated(
        message = ALL_COLS_EXCEPT,
        replaceWith = ReplaceWith("exceptNew { others.toColumnSet() }"),
        level = DeprecationLevel.ERROR,
    ) // present solely to redirect users to the right function
    public fun String.exceptNew(vararg others: ColumnsResolver<*>): SingleColumn<DataRow<*>> =
        exceptNew { others.toColumnSet() }

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public infix fun String.exceptNew(other: String): SingleColumn<DataRow<*>> =
        columnGroup(this).exceptExperimentalInternal(column<Any?>(other))

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public fun String.exceptNew(vararg others: String): SingleColumn<DataRow<*>> =
        columnGroup(this).exceptExperimentalInternal(others.toColumnSet())

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    @AccessApiOverload
    public infix fun String.exceptNew(other: KProperty<*>): SingleColumn<DataRow<*>> =
        columnGroup(this).exceptExperimentalInternal(column(other))

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    @AccessApiOverload
    public fun String.exceptNew(vararg others: KProperty<*>): SingleColumn<DataRow<*>> =
        columnGroup(this).exceptExperimentalInternal(others.toColumnSet())

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public infix fun String.exceptNew(other: ColumnPath): SingleColumn<DataRow<*>> =
        columnGroup(this).exceptExperimentalInternal(column<Any?>(other))

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public fun String.exceptNew(vararg others: ColumnPath): SingleColumn<DataRow<*>> =
        columnGroup(this).exceptExperimentalInternal(others.toColumnSet())

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public infix fun <C> KProperty<C>.exceptNew(selector: ColumnsSelector<C, *>): SingleColumn<DataRow<C>> =
        columnGroup(this).exceptExperimentalInternal(selector.toColumns())

    @ExperimentalExceptCsDsl
    @Deprecated(
        message = ALL_COLS_EXCEPT,
        replaceWith = ReplaceWith("exceptNew { other }"),
        level = DeprecationLevel.ERROR,
    ) // present solely to redirect users to the right function
    public infix fun KProperty<*>.exceptNew(other: ColumnsResolver<*>): SingleColumn<DataRow<*>> =
        exceptNew { other }

    @ExperimentalExceptCsDsl
    @Deprecated(
        message = ALL_COLS_EXCEPT,
        replaceWith = ReplaceWith("exceptNew { others.toColumnSet() }"),
        level = DeprecationLevel.ERROR,
    ) // present solely to redirect users to the right function
    public fun KProperty<*>.exceptNew(vararg others: ColumnsResolver<*>): SingleColumn<DataRow<*>> =
        exceptNew { others.toColumnSet() }

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public infix fun <C> KProperty<C>.exceptNew(other: String): SingleColumn<DataRow<C>> =
        columnGroup(this).exceptExperimentalInternal(column<Any?>(other))

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public fun <C> KProperty<C>.exceptNew(vararg others: String): SingleColumn<DataRow<C>> =
        columnGroup(this).exceptExperimentalInternal(others.toColumnSet())

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("KPropertyDataRowExceptNew")
    public infix fun <C> KProperty<DataRow<C>>.exceptNew(other: String): SingleColumn<DataRow<C>> =
        columnGroup(this).exceptExperimentalInternal(column<Any?>(other))

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("KPropertyDataRowExceptNew")
    public fun <C> KProperty<DataRow<C>>.exceptNew(vararg others: String): SingleColumn<DataRow<C>> =
        columnGroup(this).exceptExperimentalInternal(others.toColumnSet())

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    @AccessApiOverload
    public infix fun <C> KProperty<C>.exceptNew(other: KProperty<*>): SingleColumn<DataRow<C>> =
        columnGroup(this).exceptExperimentalInternal(column(other))

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    @AccessApiOverload
    public fun <C> KProperty<C>.exceptNew(vararg others: KProperty<*>): SingleColumn<DataRow<C>> =
        columnGroup(this).exceptExperimentalInternal(others.toColumnSet())

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("KPropertyDataRowExceptNew")
    @AccessApiOverload
    public infix fun <C> KProperty<DataRow<C>>.exceptNew(other: KProperty<*>): SingleColumn<DataRow<C>> =
        columnGroup(this).exceptExperimentalInternal(column(other))

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("KPropertyDataRowExceptNew")
    @AccessApiOverload
    public fun <C> KProperty<DataRow<C>>.exceptNew(vararg others: KProperty<*>): SingleColumn<DataRow<C>> =
        columnGroup(this).exceptExperimentalInternal(others.toColumnSet())

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public infix fun <C> KProperty<C>.exceptNew(other: ColumnPath): SingleColumn<DataRow<C>> =
        columnGroup(this).exceptExperimentalInternal(column<Any?>(other))

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public fun <C> KProperty<C>.exceptNew(vararg others: ColumnPath): SingleColumn<DataRow<C>> =
        columnGroup(this).exceptExperimentalInternal(others.toColumnSet())

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("KPropertyDataRowExceptNew")
    public infix fun <C> KProperty<DataRow<C>>.exceptNew(other: ColumnPath): SingleColumn<DataRow<C>> =
        columnGroup(this).exceptExperimentalInternal(column<Any?>(other))

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("KPropertyDataRowExceptNew")
    public fun <C> KProperty<DataRow<C>>.exceptNew(vararg others: ColumnPath): SingleColumn<DataRow<C>> =
        columnGroup(this).exceptExperimentalInternal(others.toColumnSet())

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public infix fun ColumnPath.exceptNew(selector: ColumnsSelector<*, *>): SingleColumn<DataRow<*>> =
        columnGroup(this).exceptExperimentalInternal(selector.toColumns<Any?, Any?>())

    @ExperimentalExceptCsDsl
    @Deprecated(
        message = ALL_COLS_EXCEPT,
        replaceWith = ReplaceWith("exceptNew { other }"),
        level = DeprecationLevel.ERROR,
    ) // present solely to redirect users to the right function
    public infix fun ColumnPath.exceptNew(other: ColumnsResolver<*>): SingleColumn<DataRow<*>> =
        exceptNew { other }

    @ExperimentalExceptCsDsl
    @Deprecated(
        message = ALL_COLS_EXCEPT,
        replaceWith = ReplaceWith("exceptNew { others.toColumnSet() }"),
        level = DeprecationLevel.ERROR,
    ) // present solely to redirect users to the right function
    public fun ColumnPath.exceptNew(vararg others: ColumnsResolver<*>): SingleColumn<DataRow<*>> =
        exceptNew { others.toColumnSet() }

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public infix fun ColumnPath.exceptNew(other: String): SingleColumn<DataRow<*>> =
        columnGroup(this).exceptExperimentalInternal(column<Any?>(other))

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public fun ColumnPath.exceptNew(vararg others: String): SingleColumn<DataRow<*>> =
        columnGroup(this).exceptExperimentalInternal(others.toColumnSet())

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    @AccessApiOverload
    public infix fun ColumnPath.exceptNew(other: KProperty<*>): SingleColumn<DataRow<*>> =
        columnGroup(this).exceptExperimentalInternal(column(other))

    @ExperimentalExceptCsDsl
    @AccessApiOverload
    public fun ColumnPath.exceptNew(vararg others: KProperty<*>): SingleColumn<DataRow<*>> =
        columnGroup(this).exceptExperimentalInternal(others.toColumnSet())

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public infix fun ColumnPath.exceptNew(other: ColumnPath): SingleColumn<DataRow<*>> =
        columnGroup(this).exceptExperimentalInternal(column<Any?>(other))

    /**
     * @include [ExperimentalExceptDocs]
     */
    @ExperimentalExceptCsDsl
    public fun ColumnPath.exceptNew(vararg others: ColumnPath): SingleColumn<DataRow<*>> =
        columnGroup(this).exceptExperimentalInternal(others.toColumnSet())

    // endregion
}

/**
 * Removes the columns in the "other" ColumnsResolver from the current ColumnSet while keeping the structure intact.
 * Returns a new ColumnSet with the remaining columns.
 *
 * @param other The ColumnsResolver containing the columns to be removed.
 * @return The new ColumnSet with the remaining columns.
 */
@Suppress("UNCHECKED_CAST")
internal fun <C> ColumnSet<C>.exceptInternal(other: ColumnsResolver<*>): ColumnSet<C> =
    createColumnSet { context ->
        val resolvedCols = this.resolve(context)
        val resolvedColsToExcept = other.resolve(context)
        resolvedCols.allColumnsExceptKeepingStructure(resolvedColsToExcept)
    } as ColumnSet<C>

/**
 * Returns a new ColumnSet that contains all columns from inside the receiver column group
 * except those specified in the "other" ColumnsResolver.
 *
 * @param other The ColumnsResolver containing the columns to be removed.
 * @return The new ColumnSet with the remaining columns.
 */
internal fun SingleColumn<DataRow<*>>.allColsExceptInternal(other: ColumnsResolver<*>): ColumnSet<Any?> =
    selectInternal { all().exceptInternal(other) }

/**
 * Returns a new SingleColumn<DataRow<C>> that has the same structure as the receiver, but excludes columns
 * specified in the "other" ColumnsResolver.
 *
 * @param other The [ColumnsResolver] to use for excluding columns.
 * @return A new [SingleColumn] with the filtered columns excluded.
 */
@Suppress("UNCHECKED_CAST")
internal fun <C> SingleColumn<DataRow<C>>.exceptExperimentalInternal(
    other: ColumnsResolver<*>,
): SingleColumn<DataRow<C>> =
    ensureIsColumnGroup().transformSingle { singleCol ->
        val columnsToExcept = singleCol
            .asColumnGroup()
            .getColumnsWithPaths { other }
            .map { it.changePath(singleCol.path + it.path) }

        val newCols = listOf(singleCol).allColumnsExceptKeepingStructure(columnsToExcept)

        newCols as List<ColumnWithPath<DataRow<*>>>
    }.singleInternal() as SingleColumn<DataRow<C>>

/**
 * Functions annotated with this annotation are experimental and will be removed or renamed in the future.
 */
@RequiresOptIn(level = RequiresOptIn.Level.ERROR)
@Target(AnnotationTarget.FUNCTION)
public annotation class ExperimentalExceptCsDsl

// endregion
