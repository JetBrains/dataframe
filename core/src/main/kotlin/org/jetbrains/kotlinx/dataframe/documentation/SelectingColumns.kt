package org.jetbrains.kotlinx.dataframe.documentation

import org.jetbrains.kotlinx.dataframe.ColumnSelector
import org.jetbrains.kotlinx.dataframe.ColumnsSelector
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.api.*
import org.jetbrains.kotlinx.dataframe.columns.ColumnReference
import org.jetbrains.kotlinx.dataframe.columns.ColumnSet
import org.jetbrains.kotlinx.dataframe.columns.SingleColumn
import org.jetbrains.kotlinx.dataframe.documentation.SelectingColumns.*
import kotlin.reflect.KProperty

/** [Selecting Columns][SelectingColumns] */
internal interface SelectingColumnsLink

/**
 * ## Selecting Columns
 * Selecting columns for various operations (including but not limited to
 * [DataFrame.select], [DataFrame.update], [DataFrame.gather], and [DataFrame.fillNulls])
 * can be done in the following ways:
 * - {@include [Dsl.WithExample]}
 * - {@include [ColumnNames.WithExample]}
 * - {@include [ColumnAccessors.WithExample]}
 * - {@include [KProperties.WithExample]}
 */
internal interface SelectingColumns {

    /**
     * The key for an @setArg that will define the operation name for the examples below.
     * Make sure to [alias][your examples].
     */
    interface OperationArg

    /** {@setArg [OperationArg] operation} */
    interface SetDefaultOperationArg

    /** Select or express columns using the Column(s) Selection DSL.
     * (Any {@include [AccessApiLink]}).
     *
     * This DSL comes in the form of either a [Column Selector][ColumnSelector]- or [Columns Selector][ColumnsSelector] lambda,
     * which operate in the {@include [ColumnSelectionDslLink]} or the {@include [ColumnsSelectionDslLink]} and
     * expect you to return a [SingleColumn] or [ColumnSet], respectively.
     */
    interface Dsl {

        /** {@include [Dsl]}
         *
         * For example:
         *
         * `df.`{@getArg [OperationArg]}` { length `[and][ColumnsSelectionDsl.and]` age }`
         *
         * `df.`{@getArg [OperationArg]}` { `[cols][ColumnsSelectionDsl.cols]`(1..5) }`
         *
         * `df.`{@getArg [OperationArg]}` { `[colsOf][colsOf]`<`[Double][Double]`>() }`
         * @include [SetDefaultOperationArg]
         */
        interface WithExample
    }

    /** [Columns selector DSL][Dsl.WithExample] */
    interface DslLink

    /** Select columns using their [column names][String]
     * ({@include [AccessApi.StringApiLink]}).
     */
    interface ColumnNames {

        /** {@include [ColumnNames]}
         *
         * For example:
         *
         * `df.`{@getArg [OperationArg]}`("length", "age")`
         * @include [SetDefaultOperationArg]
         */
        interface WithExample
    }

    /** [Column names][ColumnNames.WithExample] */
    interface ColumnNamesLink

    /** Select columns using [column accessors][ColumnReference]
     * ({@include [AccessApi.ColumnAccessorsApiLink]}).
     */
    interface ColumnAccessors {

        /** {@include [ColumnAccessors]}
         *
         * For example:
         *
         * `val length by `[column][column]`<`[Double][Double]`>()`
         *
         * `val age by `[column][column]`<`[Double][Double]`>()`
         *
         * `df.`{@getArg [OperationArg]}`(length, age)`
         * @include [SetDefaultOperationArg]
         */
        interface WithExample
    }

    /** [Column references][ColumnAccessors.WithExample] */
    interface ColumnAccessorsLink

    /** Select columns using [KProperties][KProperty] ({@include [AccessApi.KPropertiesApiLink]}). */
    interface KProperties {

        /** {@include [KProperties]}
         *
         * For example:
         * ```kotlin
         * data class Person(val length: Double, val age: Double)
         * ```
         *
         * `df.`{@getArg [OperationArg]}`(Person::length, Person::age)`
         * @include [SetDefaultOperationArg]
         */
        interface WithExample
    }

    /** [KProperties][KProperties.WithExample] */
    interface KPropertiesLink
}
