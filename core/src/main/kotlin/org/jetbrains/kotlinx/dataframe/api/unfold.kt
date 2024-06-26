package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.AnyCol
import org.jetbrains.kotlinx.dataframe.AnyColumnReference
import org.jetbrains.kotlinx.dataframe.ColumnsSelector
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.annotations.Interpretable
import org.jetbrains.kotlinx.dataframe.annotations.Refine
import org.jetbrains.kotlinx.dataframe.columns.toColumnSet
import org.jetbrains.kotlinx.dataframe.impl.api.unfoldImpl
import kotlin.reflect.KProperty

public inline fun <reified T> DataColumn<T>.unfold(vararg props: KProperty<*>, maxDepth: Int = 0): AnyCol =
    unfoldImpl(skipPrimitive = true) { properties(roots = props, maxDepth = maxDepth) }

public inline fun <reified T> DataColumn<T>.unfold(noinline body: CreateDataFrameDsl<T>.() -> Unit): AnyCol =
    unfoldImpl(skipPrimitive = false, body)

public inline fun <T, reified C> ReplaceClause<T, C>.unfold(vararg props: KProperty<*>, maxDepth: Int = 0): DataFrame<T> =
    with { it.unfold(props = props, maxDepth) }

@Refine
@Interpretable("ReplaceUnfold1")
public inline fun <T, reified C> ReplaceClause<T, C>.unfold(noinline body: CreateDataFrameDsl<C>.() -> Unit): DataFrame<T> =
    with { it.unfoldImpl(skipPrimitive = false, body) }

public fun <T> DataFrame<T>.unfold(columns: ColumnsSelector<T, *>): DataFrame<T> = replace(columns).with { it.unfold() }

public fun <T> DataFrame<T>.unfold(vararg columns: String): DataFrame<T> = unfold { columns.toColumnSet() }

public fun <T> DataFrame<T>.unfold(vararg columns: AnyColumnReference): DataFrame<T> = unfold { columns.toColumnSet() }

public fun <T> DataFrame<T>.unfold(vararg columns: KProperty<*>): DataFrame<T> = unfold { columns.toColumnSet() }
