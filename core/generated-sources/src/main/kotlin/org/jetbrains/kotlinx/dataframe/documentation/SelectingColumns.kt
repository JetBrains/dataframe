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
 * - Select or express columns using the Column(s) Selection DSL.
 * (Any [Access API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi]).
 *
 * This DSL comes in the form of either a [Column Selector][org.jetbrains.kotlinx.dataframe.ColumnSelector]- or [Columns Selector][org.jetbrains.kotlinx.dataframe.ColumnsSelector] lambda,
 * which operate in the [Column Selection DSL][org.jetbrains.kotlinx.dataframe.api.ColumnSelectionDsl] or the [Columns Selection DSL][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl] and
 * expect you to return a [SingleColumn][org.jetbrains.kotlinx.dataframe.columns.SingleColumn] or [ColumnSet][org.jetbrains.kotlinx.dataframe.columns.ColumnSet], respectively.
 *
 * For example:
 *
 * `df.`operation` { length `[and][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl.and]` age }`
 *
 * `df.`operation` { `[cols][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl.cols]`(1..5) }`
 *
 * `df.`operation` { `[colsOf][org.jetbrains.kotlinx.dataframe.api.colsOf]`<`[Double][Double]`>() }`
 *
 * - Select columns using their [column names][String]
 * ([String API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.StringApi]).
 *
 * For example:
 *
 * `df.`operation`("length", "age")`
 *
 * - Select columns using [column accessors][org.jetbrains.kotlinx.dataframe.columns.ColumnReference]
 * ([Column Accessors API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.ColumnAccessorsApi]).
 *
 * For example:
 *
 * `val length by `[column][org.jetbrains.kotlinx.dataframe.api.column]`<`[Double][Double]`>()`
 *
 * `val age by `[column][org.jetbrains.kotlinx.dataframe.api.column]`<`[Double][Double]`>()`
 *
 * `df.`operation`(length, age)`
 *
 * - Select columns using [KProperties][KProperty] ([KProperties API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.KPropertiesApi]).
 *
 * For example:
 * ```kotlin
 * data class Person(val length: Double, val age: Double)
 * ```
 *
 * `df.`operation`(Person::length, Person::age)`
 *
 */
internal interface SelectingColumns {

    /**
     * The key for an @setArg that will define the operation name for the examples below.
     * Make sure to [alias][your examples].
     */
    interface OperationArg

    interface SetDefaultOperationArg

    /** Select or express columns using the Column(s) Selection DSL.
     * (Any [Access API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi]).
     *
     * This DSL comes in the form of either a [Column Selector][ColumnSelector]- or [Columns Selector][ColumnsSelector] lambda,
     * which operate in the [Column Selection DSL][org.jetbrains.kotlinx.dataframe.api.ColumnSelectionDsl] or the [Columns Selection DSL][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl] and
     * expect you to return a [SingleColumn] or [ColumnSet], respectively.
     */
    interface Dsl {

        /** Select or express columns using the Column(s) Selection DSL.
         * (Any [Access API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi]).
         *
         * This DSL comes in the form of either a [Column Selector][org.jetbrains.kotlinx.dataframe.ColumnSelector]- or [Columns Selector][org.jetbrains.kotlinx.dataframe.ColumnsSelector] lambda,
         * which operate in the [Column Selection DSL][org.jetbrains.kotlinx.dataframe.api.ColumnSelectionDsl] or the [Columns Selection DSL][org.jetbrains.kotlinx.dataframe.api.ColumnsSelectionDsl] and
         * expect you to return a [SingleColumn][org.jetbrains.kotlinx.dataframe.columns.SingleColumn] or [ColumnSet][org.jetbrains.kotlinx.dataframe.columns.ColumnSet], respectively.
         *
         * For example:
         *
         * `df.`operation` { length `[and][ColumnsSelectionDsl.and]` age }`
         *
         * `df.`operation` { `[cols][ColumnsSelectionDsl.cols]`(1..5) }`
         *
         * `df.`operation` { `[colsOf][colsOf]`<`[Double][Double]`>() }`
         *
         */
        interface WithExample
    }

    /** [Columns selector DSL][Dsl.WithExample] */
    interface DslLink

    /** Select columns using their [column names][String]
     * ([String API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.StringApi]).
     */
    interface ColumnNames {

        /** Select columns using their [column names][String]
         * ([String API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.StringApi]).
         *
         * For example:
         *
         * `df.`operation`("length", "age")`
         *
         */
        interface WithExample
    }

    /** [Column names][ColumnNames.WithExample] */
    interface ColumnNamesLink

    /** Select columns using [column accessors][ColumnReference]
     * ([Column Accessors API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.ColumnAccessorsApi]).
     */
    interface ColumnAccessors {

        /** Select columns using [column accessors][org.jetbrains.kotlinx.dataframe.columns.ColumnReference]
         * ([Column Accessors API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.ColumnAccessorsApi]).
         *
         * For example:
         *
         * `val length by `[column][column]`<`[Double][Double]`>()`
         *
         * `val age by `[column][column]`<`[Double][Double]`>()`
         *
         * `df.`operation`(length, age)`
         *
         */
        interface WithExample
    }

    /** [Column references][ColumnAccessors.WithExample] */
    interface ColumnAccessorsLink

    /** Select columns using [KProperties][KProperty] ([KProperties API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.KPropertiesApi]). */
    interface KProperties {

        /** Select columns using [KProperties][KProperty] ([KProperties API][org.jetbrains.kotlinx.dataframe.documentation.AccessApi.KPropertiesApi]).
         *
         * For example:
         * ```kotlin
         * data class Person(val length: Double, val age: Double)
         * ```
         *
         * `df.`operation`(Person::length, Person::age)`
         *
         */
        interface WithExample
    }

    /** [KProperties][KProperties.WithExample] */
    interface KPropertiesLink
}
