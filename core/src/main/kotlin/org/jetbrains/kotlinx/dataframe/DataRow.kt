package org.jetbrains.kotlinx.dataframe

import org.jetbrains.kotlinx.dataframe.annotations.AccessApiOverload
import org.jetbrains.kotlinx.dataframe.api.next
import org.jetbrains.kotlinx.dataframe.api.prev
import org.jetbrains.kotlinx.dataframe.columns.ColumnKind
import org.jetbrains.kotlinx.dataframe.columns.ColumnPath
import org.jetbrains.kotlinx.dataframe.columns.ColumnReference
import org.jetbrains.kotlinx.dataframe.impl.columnName
import org.jetbrains.kotlinx.dataframe.impl.owner
import org.jetbrains.kotlinx.dataframe.impl.toIterable
import kotlin.reflect.KProperty

/**
 * Single row of a [DataFrame].
 *
 * @param T Schema marker. See [DataFrame] for details
 */
@org.jetbrains.annotations.Debug.Renderer(childrenArray = "this.values().toArray()")
public interface DataRow<out T> {

    public fun index(): Int

    public fun df(): DataFrame<T>

    // region get cell value

    public operator fun get(columnIndex: Int): Any?

    public operator fun <R> get(expression: RowExpression<T, R>): R = expression(this, this)

    public operator fun <R> get(column: ColumnReference<R>): R

    public operator fun <R> get(columns: List<ColumnReference<R>>): List<R> = columns.map { get(it) }

    @AccessApiOverload
    public operator fun <R> get(property: KProperty<R>): R = get(property.columnName) as R

    public operator fun get(first: AnyColumnReference, vararg other: AnyColumnReference): DataRow<T> =
        owner.get(first, *other)[index]

    public operator fun get(first: String, vararg other: String): DataRow<T> = owner.get(first, *other)[index]

    public operator fun get(path: ColumnPath): Any? = owner.get(path)[index]

    public operator fun get(name: String): Any?

    public fun getColumnGroup(columnName: String): AnyRow {
        val value = get(columnName)
        if (value == null) {
            val kind = df()[columnName].kind()
            if (kind != ColumnKind.Group) {
                error("Cannot cast null value of a $kind to a ${DataRow::class}")
            }
        }
        return value as AnyRow
    }

    public fun getFrameColumn(columnName: String): AnyFrame {
        val value = get(columnName)
        if (value == null) {
            val kind = df()[columnName].kind()
            if (kind != ColumnKind.Frame) {
                error("Cannot cast null value of a $kind to a ${DataFrame::class}")
            }
        }
        return value as AnyFrame
    }

    public fun getOrNull(name: String): Any?

    @AccessApiOverload
    public fun <R> getValueOrNull(column: ColumnReference<R>): R?

    // endregion

    public fun values(): List<Any?>

    public operator fun String.get(vararg path: String): ColumnPath = ColumnPath(listOf(this) + path)

    @AccessApiOverload
    public operator fun <R> ColumnReference<R>.invoke(): R = get(this)

    public operator fun <R> String.invoke(): R = this@DataRow[this@invoke] as R

    public operator fun <R> ColumnPath.invoke(): R = this@DataRow.get(this) as R

    public fun forwardIterable(): Iterable<DataRow<T>> = this.toIterable { it.next }

    public fun backwardIterable(): Iterable<DataRow<T>> = this.toIterable { it.prev }

    public operator fun <R : Comparable<R>> ColumnReference<R>.compareTo(other: R): Int = get(this).compareTo(other)

    public operator fun ColumnReference<Int>.plus(a: Int): Int = get(this) + a

    public operator fun ColumnReference<Long>.plus(a: Long): Long = get(this) + a

    public operator fun ColumnReference<Double>.plus(a: Double): Double = get(this) + a

    public operator fun ColumnReference<String>.plus(a: String): String = get(this) + a

    public operator fun Int.plus(col: ColumnReference<Int>): Int = this + get(col)

    public operator fun Long.plus(col: ColumnReference<Long>): Long = this + get(col)

    public operator fun Double.plus(col: ColumnReference<Double>): Double = this + get(col)

    public operator fun ColumnReference<Int>.minus(a: Int): Int = get(this) - a

    public operator fun ColumnReference<Long>.minus(a: Long): Long = get(this) - a

    public operator fun ColumnReference<Double>.minus(a: Double): Double = get(this) - a

    public operator fun Int.minus(col: ColumnReference<Int>): Int = this - get(col)

    public operator fun Long.minus(col: ColumnReference<Long>): Long = this - get(col)

    public operator fun Double.minus(col: ColumnReference<Double>): Double = this - get(col)

    public operator fun ColumnReference<Int>.times(a: Int): Int = get(this) * a

    public operator fun ColumnReference<Long>.times(a: Long): Long = get(this) * a

    public operator fun ColumnReference<Double>.times(a: Double): Double = get(this) * a

    public operator fun ColumnReference<Double>.times(a: Int): Double = get(this) * a

    public operator fun ColumnReference<Long>.times(a: Int): Long = get(this) * a

    public operator fun ColumnReference<Double>.times(a: Long): Double = get(this) * a

    public operator fun ColumnReference<Int>.div(a: Int): Int = get(this) / a

    public operator fun ColumnReference<Long>.div(a: Long): Long = get(this) / a

    public operator fun ColumnReference<Double>.div(a: Double): Double = get(this) / a

    public operator fun ColumnReference<Double>.div(a: Int): Double = get(this) / a

    public operator fun ColumnReference<Long>.div(a: Int): Long = get(this) / a

    public operator fun ColumnReference<Double>.div(a: Long): Double = get(this) / a

    public companion object {
        public val empty: AnyRow = DataFrame.empty(1)[0]
    }
}

internal val AnyRow.values: List<Any?> get() = values()
internal val AnyRow.index: Int get() = index()
internal val <T> DataRow<T>.prev: DataRow<T>? get() = this.prev()
internal val <T> DataRow<T>.next: DataRow<T>? get() = this.next()
