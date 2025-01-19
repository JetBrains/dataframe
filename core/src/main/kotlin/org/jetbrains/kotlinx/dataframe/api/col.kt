package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.AnyColumnGroupAccessor
import org.jetbrains.kotlinx.dataframe.ColumnGroupReference
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.DataRow
import org.jetbrains.kotlinx.dataframe.annotations.AccessApiOverload
import org.jetbrains.kotlinx.dataframe.api.ColColumnsSelectionDsl.Grammar
import org.jetbrains.kotlinx.dataframe.api.ColColumnsSelectionDsl.Grammar.ColumnGroupName
import org.jetbrains.kotlinx.dataframe.api.ColColumnsSelectionDsl.Grammar.ColumnSetName
import org.jetbrains.kotlinx.dataframe.api.ColColumnsSelectionDsl.Grammar.PlainDslName
import org.jetbrains.kotlinx.dataframe.columns.ColumnAccessor
import org.jetbrains.kotlinx.dataframe.columns.ColumnGroup
import org.jetbrains.kotlinx.dataframe.columns.ColumnPath
import org.jetbrains.kotlinx.dataframe.columns.ColumnSet
import org.jetbrains.kotlinx.dataframe.columns.SingleColumn
import org.jetbrains.kotlinx.dataframe.documentation.AccessApiLink
import org.jetbrains.kotlinx.dataframe.documentation.DslGrammarTemplateColumnsSelectionDsl.DslGrammarTemplate
import org.jetbrains.kotlinx.dataframe.documentation.Indent
import org.jetbrains.kotlinx.dataframe.documentation.Issues
import org.jetbrains.kotlinx.dataframe.documentation.LineBreak
import org.jetbrains.kotlinx.dataframe.impl.columns.getAt
import org.jetbrains.kotlinx.dataframe.impl.columns.singleImpl
import org.jetbrains.kotlinx.dataframe.impl.columns.transformSingle
import org.jetbrains.kotlinx.dataframe.util.COL_REPLACE
import org.jetbrains.kotlinx.dataframe.util.IDENTITY_FUNCTION
import kotlin.reflect.KProperty

// region ColumnsSelectionDsl

/**
 * ## Col {@include [ColumnsSelectionDslLink]}
 *
 * See [Grammar] for all functions in this interface.
 * @param _UNUSED {@include [Issues.ConflictingOverloadsK2Link]}
 */
public interface ColColumnsSelectionDsl<out _UNUSED> {

    /**
     * ## Col Grammar
     *
     * @include [DslGrammarTemplate]
     * {@set [DslGrammarTemplate.DefinitionsArg]
     *  {@include [DslGrammarTemplate.ColumnSetDef]}
     *  {@include [LineBreak]}
     *  {@include [DslGrammarTemplate.ColumnGroupDef]}
     *  {@include [LineBreak]}
     *  {@include [DslGrammarTemplate.ColumnDef]}
     *  {@include [LineBreak]}
     *  {@include [DslGrammarTemplate.IndexDef]}
     *  {@include [LineBreak]}
     *  {@include [DslGrammarTemplate.ColumnTypeDef]}
     * }
     *
     * {@set [DslGrammarTemplate.PlainDslFunctionsArg]
     *  {@include [PlainDslName]}`[`**`<`**{@include [DslGrammarTemplate.ColumnTypeRef]}**`>`**`]`**`(`**{@include [DslGrammarTemplate.ColumnRef]}`  |  `{@include [DslGrammarTemplate.IndexRef]}**`)`**
     * }
     *
     * {@set [DslGrammarTemplate.ColumnSetFunctionsArg]
     *  {@include [Indent]}{@include [ColumnSetName]}**`(`**{@include [DslGrammarTemplate.IndexRef]}**`)`**`  |  `[**`[`**][ColumnsSelectionDsl.col]{@include [DslGrammarTemplate.IndexRef]}[**`]`**][ColumnsSelectionDsl.col]
     * }
     *
     * {@set [DslGrammarTemplate.ColumnGroupFunctionsArg]
     *  {@include [Indent]}{@include [ColumnGroupName]}`[`**`<`**{@include [DslGrammarTemplate.ColumnTypeRef]}**`>`**`]`**`(`**{@include [DslGrammarTemplate.ColumnRef]}`  |  `{@include [DslGrammarTemplate.IndexRef]}**`)`**
     * }
     */
    public interface Grammar {

        /** [**`col`**][ColumnsSelectionDsl.col] */
        public interface PlainDslName

        /** __`.`__[**`col`**][ColumnsSelectionDsl.col] */
        public interface ColumnSetName

        /** __`.`__[**`col`**][ColumnsSelectionDsl.col] */
        public interface ColumnGroupName
    }

    /**
     * ## Col
     *
     * Creates a [ColumnAccessor] (or [SingleColumn]) for a column with the given argument which can be either
     * an index ([Int]) or a reference to a column
     * ([String], [ColumnPath], [KProperty], or [ColumnAccessor]; any {@include [AccessApiLink]}).
     *
     * This is a DSL-shorthand for [column] and can be both typed and untyped (in case you're supplying
     * a column name, -path, or index).
     * The function can also be called on [ColumnGroups][ColumnGroupReference] to create
     * an accessor for a column inside a [ColumnGroup].
     * {@include [LineBreak]}
     * $[CommonColDocs.Note]
     *
     * ### Check out: [Grammar]
     *
     * #### For example:
     *
     * `df.`[select][DataFrame.select]`  {  `[col][col]`<`[String][String]`>("colA") }`
     *
     * `df.`[select][DataFrame.select]`  {  `[col][col]`(SomeType::colB) }`
     *
     * `df.`[select][DataFrame.select]` { myColumnGroup.`[col][col]`(1) }`
     *
     * #### Examples for this overload:
     *
     * $[CommonColDocs.ExampleArg]
     *
     * To create a [ColumnAccessor] for a specific kind of column with runtime checks, take a look at the functions
     * [valueCol][ColumnsSelectionDsl.valueCol],
     * [colGroup][ColumnsSelectionDsl.colGroup],
     * and [frameCol][ColumnsSelectionDsl.frameCol].
     *
     * @return A [ColumnAccessor] for the column with the given argument if possible, else a [SingleColumn].
     * @throws [IllegalStateException\] if the column with the given argument does not exist.
     *
     * @see [column\]
     * @see [ColumnsSelectionDsl.colGroup\]
     * @see [ColumnsSelectionDsl.frameCol\]
     * @see [ColumnsSelectionDsl.valueCol\]
     * {@set [CommonColDocs.Note]}
     */
    private interface CommonColDocs {

        // Example argument, can be either {@include [SingleExample]} or {@include [DoubleExample]}
        interface ExampleArg

        /**
         * `df.`[select][DataFrame.select]` { $[CommonColDocs.ReceiverArg]`[col][col]`($[CommonColDocs.Arg]) \}`
         */
        interface SingleExample

        /**
         * `df.`[select][DataFrame.select]` { $[CommonColDocs.ReceiverArg]`[col][col]`($[CommonColDocs.Arg]) \}`
         *
         * `df.`[select][DataFrame.select]` { $[CommonColDocs.ReceiverArg]`[col][col]`<`[String][String]`>($[CommonColDocs.Arg]) \}`
         */
        interface DoubleExample

        // Receiver argument for the example(s)
        interface ReceiverArg

        // Argument for the example(s)
        interface Arg

        // Optional note
        interface Note

        /** @param [C\] The type of the column. */
        interface ColumnTypeParam
    }

    // region reference

    /**
     * @include [CommonColDocs]
     * {@set [CommonColDocs.Arg] columnA}
     * {@set [CommonColDocs.ExampleArg] {@include [CommonColDocs.SingleExample]}}
     * @param [col\] The [ColumnAccessor] pointing to the column.
     * @include [CommonColDocs.ColumnTypeParam]
     */
    private interface ColReferenceDocs

    /**
     * @include [ColReferenceDocs] {@set [CommonColDocs.ReceiverArg]}
     * {@set [CommonColDocs.Note] NOTE: This overload is an identity function and can be omitted.}
     */
    @Deprecated(IDENTITY_FUNCTION, ReplaceWith(COL_REPLACE))
    @AccessApiOverload
    public fun <C> col(col: ColumnAccessor<C>): ColumnAccessor<C> = col

    /**
     * @include [ColReferenceDocs] {@set [CommonColDocs.ReceiverArg] myColumnGroup.}
     */
    @AccessApiOverload
    public fun <C> SingleColumn<DataRow<*>>.col(col: ColumnAccessor<C>): SingleColumn<C> =
        this.ensureIsColumnGroup().transformSingle {
            val child = it.getCol(col)
                ?: throw IllegalStateException("Column '${col.path()}' not found in column group '${it.path}'")
            listOf(child)
        }.singleImpl()

    /**
     * @include [ColReferenceDocs] {@set [CommonColDocs.ReceiverArg] myColumnGroup.}
     */
    @AccessApiOverload
    public fun <C> AnyColumnGroupAccessor.col(col: ColumnAccessor<C>): ColumnAccessor<C> =
        this.ensureIsColumnGroup().column(col.path())

    /**
     * @include [ColReferenceDocs] {@set [CommonColDocs.ReceiverArg] "myColumnGroup".}
     */
    @AccessApiOverload
    public fun <C> String.col(col: ColumnAccessor<C>): ColumnAccessor<C> =
        columnGroup(this).ensureIsColumnGroup().column(col.path())

    /**
     * @include [ColReferenceDocs] {@set [CommonColDocs.ReceiverArg] Type::myColumnGroup.}
     */
    @AccessApiOverload
    public fun <C> KProperty<*>.col(col: ColumnAccessor<C>): ColumnAccessor<C> =
        columnGroup(this).ensureIsColumnGroup().column(col.path())

    /**
     * @include [ColReferenceDocs] {@set [CommonColDocs.ReceiverArg] "pathTo"["myColumnGroup"].}
     */
    @AccessApiOverload
    public fun <C> ColumnPath.col(col: ColumnAccessor<C>): ColumnAccessor<C> =
        columnGroup(this).ensureIsColumnGroup().column(col.path())

    // endregion

    // region name

    /**
     * @include [CommonColDocs]
     * {@set [CommonColDocs.Arg] "columnName"}
     * {@set [CommonColDocs.ExampleArg] {@include [CommonColDocs.DoubleExample]}}
     * @param [name\] The name of the column.
     */
    private interface ColNameDocs

    /**
     * @include [ColNameDocs] {@set [CommonColDocs.ReceiverArg]}
     */
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("colUnTyped")
    public fun col(name: String): ColumnAccessor<*> = column<Any?>(name)

    /**
     * @include [ColNameDocs] {@set [CommonColDocs.ReceiverArg]}
     * @include [CommonColDocs.ColumnTypeParam]
     */
    public fun <C> col(name: String): ColumnAccessor<C> = column(name)

    /**
     * @include [ColNameDocs] {@set [CommonColDocs.ReceiverArg] myColumnGroup.}
     */
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("colUnTyped")
    public fun SingleColumn<DataRow<*>>.col(name: String): SingleColumn<*> = col<Any?>(name)

    /**
     * @include [ColNameDocs] {@set [CommonColDocs.ReceiverArg] myColumnGroup.}
     * @include [CommonColDocs.ColumnTypeParam]
     */
    public fun <C> SingleColumn<DataRow<*>>.col(name: String): SingleColumn<C> =
        this.ensureIsColumnGroup().transformSingle {
            val child = it.getCol(name)?.cast<C>()
                ?: throw IllegalStateException("Column '$name' not found in column group '${it.path}'")
            listOf(child)
        }.singleImpl()

    /**
     * @include [ColNameDocs] {@set [CommonColDocs.ReceiverArg] myColumnGroup.}
     */
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("colUnTyped")
    public fun AnyColumnGroupAccessor.col(name: String): ColumnAccessor<*> = col<Any?>(name)

    /**
     * @include [ColNameDocs] {@set [CommonColDocs.ReceiverArg] myColumnGroup.}
     * @include [CommonColDocs.ColumnTypeParam]
     */
    public fun <C> AnyColumnGroupAccessor.col(name: String): ColumnAccessor<C> = this.ensureIsColumnGroup().column(name)

    /**
     * @include [ColNameDocs] {@set [CommonColDocs.ReceiverArg] "myColumnGroup".}
     */
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("colUnTyped")
    public fun String.col(name: String): ColumnAccessor<*> = col<Any?>(name)

    /**
     * @include [ColNameDocs] {@set [CommonColDocs.ReceiverArg] "myColumnGroup".}
     * @include [CommonColDocs.ColumnTypeParam]
     */
    public fun <C> String.col(name: String): ColumnAccessor<C> =
        columnGroup(this)
            .ensureIsColumnGroup()
            .column(name)

    /**
     * @include [ColNameDocs] {@set [CommonColDocs.ReceiverArg] Type::myColumnGroup.}
     */
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("colUnTyped")
    public fun KProperty<*>.col(name: String): ColumnAccessor<*> = col<Any?>(name)

    /**
     * @include [ColNameDocs] {@set [CommonColDocs.ReceiverArg] Type::myColumnGroup.}
     * @include [CommonColDocs.ColumnTypeParam]
     */
    public fun <C> KProperty<*>.col(name: String): ColumnAccessor<C> =
        columnGroup(this).ensureIsColumnGroup().column(name)

    /**
     * @include [ColNameDocs] {@set [CommonColDocs.ReceiverArg] "pathTo"["myColumnGroup"].}
     */
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("colUnTyped")
    public fun ColumnPath.col(name: String): ColumnAccessor<*> = col<Any?>(name)

    /**
     * @include [ColNameDocs] {@set [CommonColDocs.ReceiverArg] "pathTo"["myColumnGroup"].}
     * @include [CommonColDocs.ColumnTypeParam]
     */
    public fun <C> ColumnPath.col(name: String): ColumnAccessor<C> =
        columnGroup(this).ensureIsColumnGroup().column(name)

    // endregion

    // region path

    /**
     * @include [CommonColDocs]
     * {@set [CommonColDocs.Arg] "pathTo"["columnName"\] }
     * {@set [CommonColDocs.ExampleArg] {@include [CommonColDocs.DoubleExample]}}
     * @param [path\] The path to the column.
     */
    private interface ColPathDocs

    /**
     * @include [ColPathDocs] {@set [CommonColDocs.ReceiverArg]}
     */
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("colUnTyped")
    public fun col(path: ColumnPath): ColumnAccessor<*> = column<Any?>(path)

    /**
     * @include [ColPathDocs] {@set [CommonColDocs.ReceiverArg]}
     * @include [CommonColDocs.ColumnTypeParam]
     */
    public fun <C> col(path: ColumnPath): ColumnAccessor<C> = column(path)

    /**
     * @include [ColPathDocs] {@set [CommonColDocs.ReceiverArg] myColumnGroup.}
     */
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("colUnTyped")
    public fun SingleColumn<DataRow<*>>.col(path: ColumnPath): SingleColumn<*> = col<Any?>(path)

    /**
     * @include [ColPathDocs] {@set [CommonColDocs.ReceiverArg] myColumnGroup.}
     * @include [CommonColDocs.ColumnTypeParam]
     */
    public fun <C> SingleColumn<DataRow<*>>.col(path: ColumnPath): SingleColumn<C> =
        this.ensureIsColumnGroup().transformSingle {
            val child = it.getCol(path)?.cast<C>()
                ?: throw IllegalStateException("Column '$path' not found in column group '${it.path}'")
            listOf(child)
        }.singleImpl()

    /**
     * @include [ColPathDocs] {@set [CommonColDocs.ReceiverArg] myColumnGroup.}
     */
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("colUnTyped")
    public fun AnyColumnGroupAccessor.col(path: ColumnPath): ColumnAccessor<*> = col<Any?>(path)

    /**
     * @include [ColPathDocs] {@set [CommonColDocs.ReceiverArg] myColumnGroup.}
     * @include [CommonColDocs.ColumnTypeParam]
     */
    public fun <C> AnyColumnGroupAccessor.col(path: ColumnPath): ColumnAccessor<C> =
        this.ensureIsColumnGroup().column(path)

    /**
     * @include [ColPathDocs] {@set [CommonColDocs.ReceiverArg] "myColumnGroup".}
     */
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("colUnTyped")
    public fun String.col(path: ColumnPath): ColumnAccessor<*> = col<Any?>(path)

    /**
     * @include [ColPathDocs] {@set [CommonColDocs.ReceiverArg] "myColumnGroup".}
     * @include [CommonColDocs.ColumnTypeParam]
     */
    public fun <C> String.col(path: ColumnPath): ColumnAccessor<C> =
        columnGroup(this).ensureIsColumnGroup().column(path)

    /**
     * @include [ColPathDocs] {@set [CommonColDocs.ReceiverArg] Type::myColumnGroup.}
     */
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("colUnTyped")
    public fun KProperty<*>.col(path: ColumnPath): ColumnAccessor<*> = col<Any?>(path)

    /**
     * @include [ColPathDocs] {@set [CommonColDocs.ReceiverArg] Type::myColumnGroup.}
     * @include [CommonColDocs.ColumnTypeParam]
     */
    public fun <C> KProperty<*>.col(path: ColumnPath): ColumnAccessor<C> =
        columnGroup(this).ensureIsColumnGroup().column(path)

    /**
     * @include [ColPathDocs] {@set [CommonColDocs.ReceiverArg] "pathTo"["myColumnGroup"].}
     */
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("colUnTyped")
    public fun ColumnPath.col(path: ColumnPath): ColumnAccessor<*> = col<Any?>(path)

    /**
     * @include [ColPathDocs] {@set [CommonColDocs.ReceiverArg] "pathTo"["myColumnGroup"].}
     * @include [CommonColDocs.ColumnTypeParam]
     */
    public fun <C> ColumnPath.col(path: ColumnPath): ColumnAccessor<C> =
        columnGroup(this).ensureIsColumnGroup().column(path)

    // endregion

    // region property

    /**
     * @include [CommonColDocs]
     * {@set [CommonColDocs.Arg] Type::columnA}
     * {@set [CommonColDocs.ExampleArg] {@include [CommonColDocs.SingleExample]}}
     * @param [property\] The [KProperty] reference to the column.
     * @include [CommonColDocs.ColumnTypeParam]
     */
    private interface ColKPropertyDocs

    /**
     * @include [ColKPropertyDocs] {@set [CommonColDocs.ReceiverArg]}
     */
    @AccessApiOverload
    public fun <C> col(property: KProperty<C>): SingleColumn<C> = column(property)

    /**
     * @include [ColKPropertyDocs] {@set [CommonColDocs.ReceiverArg] myColumnGroup.}
     */
    @AccessApiOverload
    public fun <C> SingleColumn<DataRow<*>>.col(property: KProperty<C>): SingleColumn<C> = col<C>(property.name)

    /**
     * @include [ColKPropertyDocs] {@set [CommonColDocs.ReceiverArg] myColumnGroup.}
     */
    @AccessApiOverload
    public fun <C> AnyColumnGroupAccessor.col(property: KProperty<C>): ColumnAccessor<C> =
        this.ensureIsColumnGroup().column(property)

    /**
     * @include [ColKPropertyDocs] {@set [CommonColDocs.ReceiverArg] "myColumnGroup".}
     */
    @AccessApiOverload
    public fun <C> String.col(property: KProperty<C>): ColumnAccessor<C> =
        columnGroup(this).ensureIsColumnGroup().column(property)

    /**
     * @include [ColKPropertyDocs] {@set [CommonColDocs.ReceiverArg] Type::myColumnGroup.}
     */
    @AccessApiOverload
    public fun <C> KProperty<*>.col(property: KProperty<C>): ColumnAccessor<C> =
        columnGroup(this).ensureIsColumnGroup().column(property)

    /**
     * @include [ColKPropertyDocs] {@set [CommonColDocs.ReceiverArg] "pathTo"["myColumnGroup"].}
     */
    @AccessApiOverload
    public fun <C> ColumnPath.col(property: KProperty<C>): ColumnAccessor<C> =
        columnGroup(this).ensureIsColumnGroup().column(property)

    // endregion

    // region index

    /**
     * @include [CommonColDocs]
     * {@set [CommonColDocs.Arg] 0}
     * {@set [CommonColDocs.ExampleArg] {@include [CommonColDocs.DoubleExample]}}
     * @param [index\] The index of the column.
     * @throws [IndexOutOfBoundsException\] if the index is out of bounds.
     */
    private interface ColIndexDocs

    /**
     * @include [ColIndexDocs] {@set [CommonColDocs.ReceiverArg] `[colsOf][ColumnsSelectionDsl.colsOf]`<`[Int][Int]`>().}
     * @include [CommonColDocs.ColumnTypeParam]
     * {@set [CommonColDocs.ExampleArg]
     * {@include [CommonColDocs.SingleExample]}
     *
     * `df.`[select][DataFrame.select]`  {  `[colsOf][ColumnsSelectionDsl.colsOf]`<`[String][String]`>()`[`[`][col]`1`[`]`][col]` \}`
     * }
     * {@set [CommonColDocs.Note] NOTE: You can use the get-[] operator on [ColumnSets][ColumnSet] as well!}
     */
    public fun <C> ColumnSet<C>.col(index: Int): SingleColumn<C> = getAt(index)

    /**
     * @include [ColIndexDocs] {@set [CommonColDocs.ReceiverArg] `[colsOf][ColumnsSelectionDsl.colsOf]`<`[Int][Int]`>().}
     * @include [CommonColDocs.ColumnTypeParam]
     * {@set [CommonColDocs.ExampleArg]
     * {@include [CommonColDocs.SingleExample]}
     *
     * `df.`[select][DataFrame.select]`  {  `[colsOf][ColumnsSelectionDsl.colsOf]`<`[String][String]`>()`[`[`][col]`1`[`]`][col]` \}`
     * }
     */
    public operator fun <C> ColumnSet<C>.get(index: Int): SingleColumn<C> = col(index)

    /**
     * @include [ColIndexDocs] {@set [CommonColDocs.ReceiverArg]}
     */
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("colUnTyped")
    public fun ColumnsSelectionDsl<*>.col(index: Int): SingleColumn<*> = col<Any?>(index)

    /**
     * @include [ColIndexDocs] {@set [CommonColDocs.ReceiverArg]}
     * @include [CommonColDocs.ColumnTypeParam]
     */
    public fun <C> ColumnsSelectionDsl<*>.col(index: Int): SingleColumn<C> = asSingleColumn().col<C>(index)

    /**
     * @include [ColIndexDocs] {@set [CommonColDocs.ReceiverArg] myColumnGroup.}
     */
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("colUnTyped")
    public fun SingleColumn<DataRow<*>>.col(index: Int): SingleColumn<*> = col<Any?>(index)

    /**
     * @include [ColIndexDocs] {@set [CommonColDocs.ReceiverArg] myColumnGroup.}
     * @include [CommonColDocs.ColumnTypeParam]
     */
    public fun <C> SingleColumn<DataRow<*>>.col(index: Int): SingleColumn<C> =
        this.ensureIsColumnGroup()
            .allColumnsInternal()
            .getAt(index)
            .cast()

    /**
     * @include [ColIndexDocs] {@set [CommonColDocs.ReceiverArg] "myColumnGroup".}
     */
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("colUnTyped")
    public fun String.col(index: Int): SingleColumn<*> = col<Any?>(index)

    /**
     * @include [ColIndexDocs] {@set [CommonColDocs.ReceiverArg] "myColumnGroup".}
     * @include [CommonColDocs.ColumnTypeParam]
     */
    public fun <C> String.col(index: Int): SingleColumn<C> = columnGroup(this).col<C>(index)

    /**
     * @include [ColIndexDocs] {@set [CommonColDocs.ReceiverArg] Type::myColumnGroup.}
     */
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("colUnTyped")
    public fun KProperty<*>.col(index: Int): SingleColumn<*> = col<Any?>(index)

    /**
     * @include [ColIndexDocs] {@set [CommonColDocs.ReceiverArg] Type::myColumnGroup.}
     * @include [CommonColDocs.ColumnTypeParam]
     */
    public fun <C> KProperty<*>.col(index: Int): SingleColumn<C> = columnGroup(this).col<C>(index)

    /**
     * @include [ColIndexDocs] {@set [CommonColDocs.ReceiverArg] "pathTo"["myColumnGroup"].}
     */
    @Suppress("INAPPLICABLE_JVM_NAME")
    @JvmName("colUnTyped")
    public fun ColumnPath.col(index: Int): SingleColumn<*> = col<Any?>(index)

    /**
     * @include [ColIndexDocs] {@set [CommonColDocs.ReceiverArg] "pathTo"["myColumnGroup"].}
     * @include [CommonColDocs.ColumnTypeParam]
     */
    public fun <C> ColumnPath.col(index: Int): SingleColumn<C> = columnGroup(this).col<C>(index)

    // endregion
}

// endregion
