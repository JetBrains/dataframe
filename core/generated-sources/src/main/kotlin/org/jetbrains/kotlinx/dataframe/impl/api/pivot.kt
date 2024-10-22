package org.jetbrains.kotlinx.dataframe.impl.api

import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.Selector
import org.jetbrains.kotlinx.dataframe.aggregation.AggregateDsl
import org.jetbrains.kotlinx.dataframe.aggregation.NamedValue
import org.jetbrains.kotlinx.dataframe.api.PivotColumnsSelector
import org.jetbrains.kotlinx.dataframe.api.forEach
import org.jetbrains.kotlinx.dataframe.api.groupBy
import org.jetbrains.kotlinx.dataframe.api.toPath
import org.jetbrains.kotlinx.dataframe.columns.ColumnPath
import org.jetbrains.kotlinx.dataframe.columns.ColumnResolutionContext
import org.jetbrains.kotlinx.dataframe.columns.ColumnSet
import org.jetbrains.kotlinx.dataframe.columns.ColumnWithPath
import org.jetbrains.kotlinx.dataframe.columns.ColumnsResolver
import org.jetbrains.kotlinx.dataframe.columns.UnresolvedColumnsPolicy
import org.jetbrains.kotlinx.dataframe.columns.toColumnSet
import org.jetbrains.kotlinx.dataframe.impl.aggregation.GroupByReceiverImpl
import org.jetbrains.kotlinx.dataframe.impl.aggregation.receivers.AggregateInternalDsl
import org.jetbrains.kotlinx.dataframe.impl.aggregation.receivers.AggregatePivotDslImpl
import org.jetbrains.kotlinx.dataframe.impl.columns.resolve
import org.jetbrains.kotlinx.dataframe.impl.columns.toColumnSet

internal class AggregatedPivot<T>(
    private val df: DataFrame<T>,
    val inward: Boolean?,
    internal var aggregator: GroupByReceiverImpl<T>,
) : DataFrame<T> by df

internal data class PivotChainElement(val column: ColumnWithPath<Any?>, val includeColumnName: Boolean)

internal class PivotChain<C>(val columns: List<PivotChainElement>, lastColumn: ColumnWithPath<C>) :
    ColumnWithPath<C> by lastColumn

internal class PivotChainColumnSet<C>(val first: ColumnsResolver<C>, val second: ColumnsResolver<C>) : ColumnSet<C> {

    override fun resolve(context: ColumnResolutionContext): List<ColumnWithPath<C>> {
        val firstCols = first.resolve(context)
        val secondCols = second.resolve(context)
        val result = mutableListOf<PivotChain<C>>()
        firstCols.forEach { a ->
            secondCols.forEach { b ->
                val firstPart = ((a as? PivotChain<*>)?.columns ?: listOf(PivotChainElement(a, false)))
                var secondPart = ((b as? PivotChain<*>)?.columns ?: listOf(PivotChainElement(b, false)))
                if (secondCols.size > 1) {
                    secondPart = listOf(PivotChainElement(secondPart[0].column, true)) + secondPart.drop(1)
                }
                result.add(PivotChain(firstPart + secondPart, b))
            }
        }
        return result
    }
}

internal fun <T, C> DataFrame<T>.getPivotSequences(
    columns: PivotColumnsSelector<T, C>,
): List<List<PivotChainElement>> =
    columns.toColumnSet().resolve(this, UnresolvedColumnsPolicy.Fail)
        .map {
            when (val col = it) {
                is PivotChain<*> -> col.columns as List<PivotChainElement>
                else -> listOf(PivotChainElement(it, false))
            }
        }

internal fun <T> DataFrame<T>.getPivotColumnPaths(columns: PivotColumnsSelector<T, *>): List<ColumnPath> =
    getPivotSequences(columns).flatten().map { it.column.path }.distinct()

internal fun <T, R> aggregatePivot(
    aggregator: AggregateInternalDsl<T>,
    columns: PivotColumnsSelector<T, *>,
    separate: Boolean,
    inward: Boolean? = null,
    globalDefault: Any? = null,
    body: Selector<AggregateDsl<T>, R>,
) {
    val pivotSequences = aggregator.df.getPivotSequences(columns)
    val effectiveInward = inward ?: if (aggregator.hasGroupingKeys) {
        true
    } else {
        pivotSequences.distinctBy { it.first().column.path }.count() > 1
    }
    pivotSequences.forEach { pivotColumns ->
        aggregator.df.groupBy { pivotColumns.map { it.column }.toColumnSet() }.forEach { (key, group) ->

            val pathNames = mutableListOf<String>()
            key.values().forEachIndexed { i, v ->
                if (i == 0 && effectiveInward) {
                    pathNames.addAll(pivotColumns[i].column.path)
                } else if (pivotColumns[i].includeColumnName) {
                    pathNames.add(pivotColumns[i].column.name)
                }
                pathNames.add(v.toString())
            }
            val path = pathNames.toPath()
            val builder = AggregatePivotDslImpl(group)
            val result = body(builder, builder)
            val hasResult = result != null && result != Unit

            fun NamedValue.apply(path: ColumnPath) =
                copy(
                    path = path,
                    value = this.value ?: default ?: globalDefault,
                    default = default ?: globalDefault,
                )

            val values = builder.values
            when {
                values.size == 1 && values[0].path.isEmpty() -> aggregator.yield(values[0].apply(path))

                values.isEmpty() -> aggregator.yield(
                    path = path,
                    value = if (hasResult) result else globalDefault,
                    type = null,
                    default = globalDefault,
                    guessType = true,
                )

                else -> {
                    values.forEach {
                        val targetPath = if (separate) it.path + path else path + it.path
                        aggregator.yield(it.apply(targetPath))
                    }
                }
            }
        }
    }
}
