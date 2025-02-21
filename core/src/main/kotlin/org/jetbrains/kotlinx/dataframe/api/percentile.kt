package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.AnyRow
import org.jetbrains.kotlinx.dataframe.ColumnsSelector
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.DataRow
import org.jetbrains.kotlinx.dataframe.RowExpression
import org.jetbrains.kotlinx.dataframe.aggregation.ColumnsForAggregateSelector
import org.jetbrains.kotlinx.dataframe.annotations.AccessApiOverload
import org.jetbrains.kotlinx.dataframe.columns.ColumnReference
import org.jetbrains.kotlinx.dataframe.columns.toColumnSet
import org.jetbrains.kotlinx.dataframe.impl.aggregation.aggregators.Aggregators
import org.jetbrains.kotlinx.dataframe.impl.aggregation.aggregators.cast
import org.jetbrains.kotlinx.dataframe.impl.aggregation.interComparableColumns
import org.jetbrains.kotlinx.dataframe.impl.aggregation.modes.aggregateAll
import org.jetbrains.kotlinx.dataframe.impl.aggregation.modes.aggregateFor
import org.jetbrains.kotlinx.dataframe.impl.aggregation.modes.aggregateOf
import org.jetbrains.kotlinx.dataframe.impl.aggregation.modes.of
import org.jetbrains.kotlinx.dataframe.impl.columns.toComparableColumns
import org.jetbrains.kotlinx.dataframe.impl.suggestIfNull
import org.jetbrains.kotlinx.dataframe.math.percentile
import kotlin.reflect.KProperty

// region DataColumn

public fun <T : Comparable<T>> DataColumn<T?>.percentile(percentile: Double): T =
    percentileOrNull(percentile).suggestIfNull("percentile")

public fun <T : Comparable<T>> DataColumn<T?>.percentileOrNull(percentile: Double): T? =
    Aggregators.percentile(percentile).cast<T>().aggregate(this)

public inline fun <T, reified R : Comparable<R>> DataColumn<T>.percentileOfOrNull(
    percentile: Double,
    noinline expression: (T) -> R?,
): R? = Aggregators.percentile(percentile).cast<R?>().aggregateOf(this, expression)

public inline fun <T, reified R : Comparable<R>> DataColumn<T>.percentileOf(
    percentile: Double,
    noinline expression: (T) -> R?,
): R = percentileOfOrNull(percentile, expression).suggestIfNull("percentileOf")

// endregion

// region DataRow

public fun AnyRow.rowPercentileOrNull(percentile: Double): Any? =
    Aggregators.percentile(percentile).aggregate(
        values().filterIsInstance<Comparable<Any?>>().toValueColumn(),
    )

public fun AnyRow.rowPercentile(percentile: Double): Any =
    rowPercentileOrNull(percentile).suggestIfNull("rowPercentile")

public inline fun <reified T : Comparable<T>> AnyRow.rowPercentileOfOrNull(percentile: Double): T? =
    valuesOf<T>().percentile(percentile)

public inline fun <reified T : Comparable<T>> AnyRow.rowPercentileOf(percentile: Double): T =
    rowPercentileOfOrNull<T>(percentile).suggestIfNull("rowPercentileOf")

// endregion

// region DataFrame

public fun <T> DataFrame<T>.percentile(percentile: Double): DataRow<T> =
    percentileFor(percentile, interComparableColumns())

public fun <T, C : Comparable<C>> DataFrame<T>.percentileFor(
    percentile: Double,
    columns: ColumnsForAggregateSelector<T, C?>,
): DataRow<T> = Aggregators.percentile(percentile).aggregateFor(this, columns)

public fun <T> DataFrame<T>.percentileFor(percentile: Double, vararg columns: String): DataRow<T> =
    percentileFor(percentile) { columns.toComparableColumns() }

@AccessApiOverload
public fun <T, C : Comparable<C>> DataFrame<T>.percentileFor(
    percentile: Double,
    vararg columns: ColumnReference<C?>,
): DataRow<T> = percentileFor(percentile) { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C : Comparable<C>> DataFrame<T>.percentileFor(
    percentile: Double,
    vararg columns: KProperty<C?>,
): DataRow<T> = percentileFor(percentile) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> DataFrame<T>.percentile(percentile: Double, columns: ColumnsSelector<T, C?>): C =
    percentileOrNull(percentile, columns).suggestIfNull("percentile")

public fun <T> DataFrame<T>.percentile(percentile: Double, vararg columns: String): Any =
    percentile(percentile) { columns.toComparableColumns() }

@AccessApiOverload
public fun <T, C : Comparable<C>> DataFrame<T>.percentile(percentile: Double, vararg columns: ColumnReference<C?>): C =
    percentile(percentile) { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C : Comparable<C>> DataFrame<T>.percentile(percentile: Double, vararg columns: KProperty<C?>): C =
    percentile(percentile) { columns.toColumnSet() }

@Suppress("UNCHECKED_CAST")
public fun <T, C : Comparable<C>> DataFrame<T>.percentileOrNull(
    percentile: Double,
    columns: ColumnsSelector<T, C?>,
): C? = Aggregators.percentile(percentile).aggregateAll(this, columns) as C?

public fun <T> DataFrame<T>.percentileOrNull(percentile: Double, vararg columns: String): Any? =
    percentileOrNull(percentile) { columns.toComparableColumns() }

@AccessApiOverload
public fun <T, C : Comparable<C>> DataFrame<T>.percentileOrNull(
    percentile: Double,
    vararg columns: ColumnReference<C?>,
): C? = percentileOrNull(percentile) { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C : Comparable<C>> DataFrame<T>.percentileOrNull(percentile: Double, vararg columns: KProperty<C?>): C? =
    percentileOrNull(percentile) { columns.toColumnSet() }

public inline fun <T, reified R : Comparable<R>> DataFrame<T>.percentileOf(
    percentile: Double,
    crossinline expression: RowExpression<T, R?>,
): R? = Aggregators.percentile(percentile).of(this, expression) as R?

// endregion

// region GroupBy

public fun <T> Grouped<T>.percentile(percentile: Double): DataFrame<T> =
    percentileFor(percentile, interComparableColumns())

public fun <T, C : Comparable<C>> Grouped<T>.percentileFor(
    percentile: Double,
    columns: ColumnsForAggregateSelector<T, C?>,
): DataFrame<T> = Aggregators.percentile(percentile).aggregateFor(this, columns)

public fun <T> Grouped<T>.percentileFor(percentile: Double, vararg columns: String): DataFrame<T> =
    percentileFor(percentile) { columns.toComparableColumns() }

@AccessApiOverload
public fun <T, C : Comparable<C>> Grouped<T>.percentileFor(
    percentile: Double,
    vararg columns: ColumnReference<C?>,
): DataFrame<T> = percentileFor(percentile) { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C : Comparable<C>> Grouped<T>.percentileFor(
    percentile: Double,
    vararg columns: KProperty<C?>,
): DataFrame<T> = percentileFor(percentile) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> Grouped<T>.percentile(
    percentile: Double,
    name: String? = null,
    columns: ColumnsSelector<T, C?>,
): DataFrame<T> = Aggregators.percentile(percentile).aggregateAll(this, name, columns)

public fun <T> Grouped<T>.percentile(percentile: Double, vararg columns: String, name: String? = null): DataFrame<T> =
    percentile(percentile, name) { columns.toComparableColumns() }

@AccessApiOverload
public fun <T, C : Comparable<C>> Grouped<T>.percentile(
    percentile: Double,
    vararg columns: ColumnReference<C?>,
    name: String? = null,
): DataFrame<T> = percentile(percentile, name) { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C : Comparable<C>> Grouped<T>.percentile(
    percentile: Double,
    vararg columns: KProperty<C?>,
    name: String? = null,
): DataFrame<T> = percentile(percentile, name) { columns.toColumnSet() }

public inline fun <T, reified R : Comparable<R>> Grouped<T>.percentileOf(
    percentile: Double,
    name: String? = null,
    crossinline expression: RowExpression<T, R?>,
): DataFrame<T> = Aggregators.percentile(percentile).aggregateOf(this, name, expression)

// endregion

// region Pivot

public fun <T> Pivot<T>.percentile(percentile: Double, separate: Boolean = false): DataRow<T> =
    percentileFor(percentile, separate, interComparableColumns())

public fun <T, C : Comparable<C>> Pivot<T>.percentileFor(
    percentile: Double,
    separate: Boolean = false,
    columns: ColumnsForAggregateSelector<T, C?>,
): DataRow<T> = delegate { percentileFor(percentile, separate, columns) }

public fun <T> Pivot<T>.percentileFor(
    percentile: Double,
    vararg columns: String,
    separate: Boolean = false,
): DataRow<T> = percentileFor(percentile, separate) { columns.toComparableColumns() }

@AccessApiOverload
public fun <T, C : Comparable<C>> Pivot<T>.percentileFor(
    percentile: Double,
    vararg columns: ColumnReference<C?>,
    separate: Boolean = false,
): DataRow<T> = percentileFor(percentile, separate) { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C : Comparable<C>> Pivot<T>.percentileFor(
    percentile: Double,
    vararg columns: KProperty<C?>,
    separate: Boolean = false,
): DataRow<T> = percentileFor(percentile, separate) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> Pivot<T>.percentile(percentile: Double, columns: ColumnsSelector<T, C?>): DataRow<T> =
    delegate { percentile(percentile, columns) }

public fun <T> Pivot<T>.percentile(percentile: Double, vararg columns: String): DataRow<T> =
    percentile(percentile) { columns.toComparableColumns() }

@AccessApiOverload
public fun <T, C : Comparable<C>> Pivot<T>.percentile(
    percentile: Double,
    vararg columns: ColumnReference<C?>,
): DataRow<T> = percentile(percentile) { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C : Comparable<C>> Pivot<T>.percentile(percentile: Double, vararg columns: KProperty<C?>): DataRow<T> =
    percentile(percentile) { columns.toColumnSet() }

public inline fun <T, reified R : Comparable<R>> Pivot<T>.percentileOf(
    percentile: Double,
    crossinline expression: RowExpression<T, R?>,
): DataRow<T> = delegate { percentileOf(percentile, expression) }

// endregion

// region PivotGroupBy

public fun <T> PivotGroupBy<T>.percentile(percentile: Double, separate: Boolean = false): DataFrame<T> =
    percentileFor(percentile, separate, interComparableColumns())

public fun <T, C : Comparable<C>> PivotGroupBy<T>.percentileFor(
    percentile: Double,
    separate: Boolean = false,
    columns: ColumnsForAggregateSelector<T, C?>,
): DataFrame<T> = Aggregators.percentile(percentile).aggregateFor(this, separate, columns)

public fun <T> PivotGroupBy<T>.percentileFor(
    percentile: Double,
    vararg columns: String,
    separate: Boolean = false,
): DataFrame<T> = percentileFor(percentile, separate) { columns.toComparableColumns() }

@AccessApiOverload
public fun <T, C : Comparable<C>> PivotGroupBy<T>.percentileFor(
    percentile: Double,
    vararg columns: ColumnReference<C?>,
    separate: Boolean = false,
): DataFrame<T> = percentileFor(percentile, separate) { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C : Comparable<C>> PivotGroupBy<T>.percentileFor(
    percentile: Double,
    vararg columns: KProperty<C?>,
    separate: Boolean = false,
): DataFrame<T> = percentileFor(percentile, separate) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> PivotGroupBy<T>.percentile(
    percentile: Double,
    columns: ColumnsSelector<T, C?>,
): DataFrame<T> = Aggregators.percentile(percentile).aggregateAll(this, columns)

public fun <T> PivotGroupBy<T>.percentile(percentile: Double, vararg columns: String): DataFrame<T> =
    percentile(percentile) { columns.toComparableColumns() }

@AccessApiOverload
public fun <T, C : Comparable<C>> PivotGroupBy<T>.percentile(
    percentile: Double,
    vararg columns: ColumnReference<C?>,
): DataFrame<T> = percentile(percentile) { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C : Comparable<C>> PivotGroupBy<T>.percentile(
    percentile: Double,
    vararg columns: KProperty<C?>,
): DataFrame<T> = percentile(percentile) { columns.toColumnSet() }

public inline fun <T, reified R : Comparable<R>> PivotGroupBy<T>.percentileOf(
    percentile: Double,
    crossinline expression: RowExpression<T, R?>,
): DataFrame<T> = Aggregators.percentile(percentile).aggregateOf(this, expression)

// endregion
