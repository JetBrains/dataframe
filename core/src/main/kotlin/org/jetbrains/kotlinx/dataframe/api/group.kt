package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.AnyColumnGroupAccessor
import org.jetbrains.kotlinx.dataframe.AnyColumnReference
import org.jetbrains.kotlinx.dataframe.ColumnsSelector
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.annotations.AccessApiOverload
import org.jetbrains.kotlinx.dataframe.annotations.CandidateForRemoval
import org.jetbrains.kotlinx.dataframe.annotations.Interpretable
import org.jetbrains.kotlinx.dataframe.annotations.Refine
import org.jetbrains.kotlinx.dataframe.columns.ColumnWithPath
import org.jetbrains.kotlinx.dataframe.columns.toColumnSet
import org.jetbrains.kotlinx.dataframe.impl.columnName
import kotlin.experimental.ExperimentalTypeInference
import kotlin.reflect.KProperty

// region DataFrame

@Interpretable("Group0")
public fun <T, C> DataFrame<T>.group(columns: ColumnsSelector<T, C>): GroupClause<T, C> = GroupClause(this, columns)

public fun <T> DataFrame<T>.group(vararg columns: String): GroupClause<T, Any?> = group { columns.toColumnSet() }

@AccessApiOverload
public fun <T> DataFrame<T>.group(vararg columns: AnyColumnReference): GroupClause<T, Any?> =
    group { columns.toColumnSet() }

@AccessApiOverload
public fun <T> DataFrame<T>.group(vararg columns: KProperty<*>): GroupClause<T, Any?> = group { columns.toColumnSet() }

// endregion

// region GroupClause

public class GroupClause<T, C>(internal val df: DataFrame<T>, internal val columns: ColumnsSelector<T, C>) {
    override fun toString(): String = "GroupClause(df=$df, columns=$columns)"
}

// region into

@JvmName("intoString")
@OverloadResolutionByLambdaReturnType
@OptIn(ExperimentalTypeInference::class)
@CandidateForRemoval
public fun <T, C> GroupClause<T, C>.into(column: ColumnsSelectionDsl<T>.(ColumnWithPath<C>) -> String): DataFrame<T> =
    df.move(columns).under { column(it).toColumnAccessor() }

@JvmName("intoColumn")
@CandidateForRemoval
public fun <T, C> GroupClause<T, C>.into(
    column: ColumnsSelectionDsl<T>.(ColumnWithPath<C>) -> AnyColumnReference,
): DataFrame<T> = df.move(columns).under(column)

@Refine
@Interpretable("Into0")
public fun <T, C> GroupClause<T, C>.into(column: String): DataFrame<T> = into(columnGroup().named(column))

@AccessApiOverload
public fun <T, C> GroupClause<T, C>.into(column: AnyColumnGroupAccessor): DataFrame<T> = df.move(columns).under(column)

@AccessApiOverload
public fun <T, C> GroupClause<T, C>.into(column: KProperty<*>): DataFrame<T> = into(column.columnName)

// endregion

// endregion
