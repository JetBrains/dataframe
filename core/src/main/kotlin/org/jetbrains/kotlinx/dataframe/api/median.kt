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
import org.jetbrains.kotlinx.dataframe.math.median
import kotlin.reflect.KProperty

// region DataColumn

public fun <T : Comparable<T>> DataColumn<T?>.median(): T = medianOrNull().suggestIfNull("median")

public fun <T : Comparable<T>> DataColumn<T?>.medianOrNull(): T? = Aggregators.median.cast<T>().aggregate(this)

public inline fun <T, reified R : Comparable<R>> DataColumn<T>.medianOfOrNull(noinline expression: (T) -> R?): R? =
    Aggregators.median.cast<R?>().aggregateOf(this, expression)

public inline fun <T, reified R : Comparable<R>> DataColumn<T>.medianOf(noinline expression: (T) -> R?): R =
    medianOfOrNull(expression).suggestIfNull("medianOf")

// endregion

// region DataRow

public fun AnyRow.rowMedianOrNull(): Any? =
    Aggregators.median.aggregateMixed(
        values().filterIsInstance<Comparable<Any?>>().asIterable(),
    )

public fun AnyRow.rowMedian(): Any = rowMedianOrNull().suggestIfNull("rowMedian")

public inline fun <reified T : Comparable<T>> AnyRow.rowMedianOfOrNull(): T? = valuesOf<T>().median()

public inline fun <reified T : Comparable<T>> AnyRow.rowMedianOf(): T =
    rowMedianOfOrNull<T>().suggestIfNull("rowMedianOf")

// endregion

// region DataFrame

public fun <T> DataFrame<T>.median(): DataRow<T> = medianFor(interComparableColumns())

public fun <T, C : Comparable<C>> DataFrame<T>.medianFor(columns: ColumnsForAggregateSelector<T, C?>): DataRow<T> =
    Aggregators.median.aggregateFor(this, columns)

public fun <T> DataFrame<T>.medianFor(vararg columns: String): DataRow<T> = medianFor { columns.toComparableColumns() }

@AccessApiOverload
public fun <T, C : Comparable<C>> DataFrame<T>.medianFor(vararg columns: ColumnReference<C?>): DataRow<T> =
    medianFor { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C : Comparable<C>> DataFrame<T>.medianFor(vararg columns: KProperty<C?>): DataRow<T> =
    medianFor { columns.toColumnSet() }

public fun <T, C : Comparable<C>> DataFrame<T>.median(columns: ColumnsSelector<T, C?>): C =
    medianOrNull(columns).suggestIfNull("median")

public fun <T> DataFrame<T>.median(vararg columns: String): Any = median { columns.toComparableColumns() }

@AccessApiOverload
public fun <T, C : Comparable<C>> DataFrame<T>.median(vararg columns: ColumnReference<C?>): C =
    median { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C : Comparable<C>> DataFrame<T>.median(vararg columns: KProperty<C?>): C =
    median { columns.toColumnSet() }

@Suppress("UNCHECKED_CAST")
public fun <T, C : Comparable<C>> DataFrame<T>.medianOrNull(columns: ColumnsSelector<T, C?>): C? =
    Aggregators.median.aggregateAll(this, columns) as C?

public fun <T> DataFrame<T>.medianOrNull(vararg columns: String): Any? = medianOrNull { columns.toComparableColumns() }

@AccessApiOverload
public fun <T, C : Comparable<C>> DataFrame<T>.medianOrNull(vararg columns: ColumnReference<C?>): C? =
    medianOrNull { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C : Comparable<C>> DataFrame<T>.medianOrNull(vararg columns: KProperty<C?>): C? =
    medianOrNull { columns.toColumnSet() }

public inline fun <T, reified R : Comparable<R>> DataFrame<T>.medianOf(
    crossinline expression: RowExpression<T, R?>,
): R? = Aggregators.median.of(this, expression) as R?

// endregion

// region GroupBy

public fun <T> Grouped<T>.median(): DataFrame<T> = medianFor(interComparableColumns())

public fun <T, C : Comparable<C>> Grouped<T>.medianFor(columns: ColumnsForAggregateSelector<T, C?>): DataFrame<T> =
    Aggregators.median.aggregateFor(this, columns)

public fun <T> Grouped<T>.medianFor(vararg columns: String): DataFrame<T> = medianFor { columns.toComparableColumns() }

@AccessApiOverload
public fun <T, C : Comparable<C>> Grouped<T>.medianFor(vararg columns: ColumnReference<C?>): DataFrame<T> =
    medianFor { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C : Comparable<C>> Grouped<T>.medianFor(vararg columns: KProperty<C?>): DataFrame<T> =
    medianFor { columns.toColumnSet() }

public fun <T, C : Comparable<C>> Grouped<T>.median(
    name: String? = null,
    columns: ColumnsSelector<T, C?>,
): DataFrame<T> = Aggregators.median.aggregateAll(this, name, columns)

public fun <T> Grouped<T>.median(vararg columns: String, name: String? = null): DataFrame<T> =
    median(name) { columns.toComparableColumns() }

@AccessApiOverload
public fun <T, C : Comparable<C>> Grouped<T>.median(
    vararg columns: ColumnReference<C?>,
    name: String? = null,
): DataFrame<T> = median(name) { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C : Comparable<C>> Grouped<T>.median(vararg columns: KProperty<C?>, name: String? = null): DataFrame<T> =
    median(name) { columns.toColumnSet() }

public inline fun <T, reified R : Comparable<R>> Grouped<T>.medianOf(
    name: String? = null,
    crossinline expression: RowExpression<T, R?>,
): DataFrame<T> = Aggregators.median.aggregateOf(this, name, expression)

// endregion

// region Pivot

public fun <T> Pivot<T>.median(separate: Boolean = false): DataRow<T> = medianFor(separate, interComparableColumns())

public fun <T, C : Comparable<C>> Pivot<T>.medianFor(
    separate: Boolean = false,
    columns: ColumnsForAggregateSelector<T, C?>,
): DataRow<T> = delegate { medianFor(separate, columns) }

public fun <T> Pivot<T>.medianFor(vararg columns: String, separate: Boolean = false): DataRow<T> =
    medianFor(separate) { columns.toComparableColumns() }

@AccessApiOverload
public fun <T, C : Comparable<C>> Pivot<T>.medianFor(
    vararg columns: ColumnReference<C?>,
    separate: Boolean = false,
): DataRow<T> = medianFor(separate) { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C : Comparable<C>> Pivot<T>.medianFor(
    vararg columns: KProperty<C?>,
    separate: Boolean = false,
): DataRow<T> = medianFor(separate) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> Pivot<T>.median(columns: ColumnsSelector<T, C?>): DataRow<T> =
    delegate { median(columns) }

public fun <T> Pivot<T>.median(vararg columns: String): DataRow<T> = median { columns.toComparableColumns() }

@AccessApiOverload
public fun <T, C : Comparable<C>> Pivot<T>.median(vararg columns: ColumnReference<C?>): DataRow<T> =
    median { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C : Comparable<C>> Pivot<T>.median(vararg columns: KProperty<C?>): DataRow<T> =
    median { columns.toColumnSet() }

public inline fun <T, reified R : Comparable<R>> Pivot<T>.medianOf(
    crossinline expression: RowExpression<T, R?>,
): DataRow<T> = delegate { medianOf(expression) }

// endregion

// region PivotGroupBy

public fun <T> PivotGroupBy<T>.median(separate: Boolean = false): DataFrame<T> =
    medianFor(separate, interComparableColumns())

public fun <T, C : Comparable<C>> PivotGroupBy<T>.medianFor(
    separate: Boolean = false,
    columns: ColumnsForAggregateSelector<T, C?>,
): DataFrame<T> = Aggregators.median.aggregateFor(this, separate, columns)

public fun <T> PivotGroupBy<T>.medianFor(vararg columns: String, separate: Boolean = false): DataFrame<T> =
    medianFor(separate) { columns.toComparableColumns() }

@AccessApiOverload
public fun <T, C : Comparable<C>> PivotGroupBy<T>.medianFor(
    vararg columns: ColumnReference<C?>,
    separate: Boolean = false,
): DataFrame<T> = medianFor(separate) { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C : Comparable<C>> PivotGroupBy<T>.medianFor(
    vararg columns: KProperty<C?>,
    separate: Boolean = false,
): DataFrame<T> = medianFor(separate) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> PivotGroupBy<T>.median(columns: ColumnsSelector<T, C?>): DataFrame<T> =
    Aggregators.median.aggregateAll(this, columns)

public fun <T> PivotGroupBy<T>.median(vararg columns: String): DataFrame<T> = median { columns.toComparableColumns() }

@AccessApiOverload
public fun <T, C : Comparable<C>> PivotGroupBy<T>.median(vararg columns: ColumnReference<C?>): DataFrame<T> =
    median { columns.toColumnSet() }

@AccessApiOverload
public fun <T, C : Comparable<C>> PivotGroupBy<T>.median(vararg columns: KProperty<C?>): DataFrame<T> =
    median { columns.toColumnSet() }

public inline fun <T, reified R : Comparable<R>> PivotGroupBy<T>.medianOf(
    crossinline expression: RowExpression<T, R?>,
): DataFrame<T> = Aggregators.median.aggregateOf(this, expression)

// endregion
