package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.AnyColumnReference
import org.jetbrains.kotlinx.dataframe.ColumnsSelector
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.annotations.AccessApiOverload
import org.jetbrains.kotlinx.dataframe.api.Select.SelectSelectingOptions
import org.jetbrains.kotlinx.dataframe.columns.toColumnSet
import org.jetbrains.kotlinx.dataframe.documentation.DocumentationUrls
import org.jetbrains.kotlinx.dataframe.documentation.ExcludeFromSources
import org.jetbrains.kotlinx.dataframe.impl.nothingType
import org.jetbrains.kotlinx.dataframe.impl.nullableNothingType
import org.jetbrains.kotlinx.dataframe.math.cumSum
import org.jetbrains.kotlinx.dataframe.math.defaultCumSumSkipNA
import org.jetbrains.kotlinx.dataframe.typeClass
import java.math.BigDecimal
import java.math.BigInteger
import kotlin.reflect.KProperty
import kotlin.reflect.typeOf

// region DataColumn

/**
 * ## The CumSum Operation
 *
 * Computes the cumulative sum of the values in the {@get DATA_TYPE}.
 *
 * __NOTE:__ If the column contains nullable values and `skipNA` is set to `true`,
 * skips null values when computing the cumulative sum.
 * Otherwise, any null value encountered will propagate null values in the output from that point onward.
 *
 * {@get [CUMSUM_PARAM] @param [columns]
 * The names of the columns to apply cumSum operation.}
 *
 * @param [skipNA] Whether to skip null values (default: `true`).
 *
 * @return A new {@get DATA_TYPE} of the same type with the cumulative sum of the values.
 *
 * {@get [CUMSUM_PARAM] @see [Selecting Columns][SelectSelectingOptions].}
 * @see {@include [DocumentationUrls.CumSum]}
 */
@ExcludeFromSources
@Suppress("ClassName")
private interface CumSumDocs {
    interface CUMSUM_PARAM
}

/**
 * {@include [CumSumDocs]}
 * {@set DATA_TYPE [DataColumn]}.
 * {@set [CumSumDocs.CUMSUM_PARAM]}
 */
public fun <T : Number?> DataColumn<T>.cumSum(skipNA: Boolean = defaultCumSumSkipNA): DataColumn<T> =
    when (type()) {
        typeOf<Double>() -> cast<Double>().cumSum(skipNA).cast()

        typeOf<Double?>() -> cast<Double?>().cumSum(skipNA).cast()

        typeOf<Float>() -> cast<Float>().cumSum(skipNA).cast()

        typeOf<Float?>() -> cast<Float?>().cumSum(skipNA).cast()

        typeOf<Int>() -> cast<Int>().cumSum().cast()

        // TODO cumSum for Byte returns Int but is converted back to T: Byte, Issue #558
        typeOf<Byte>() -> cast<Byte>().cumSum().map { it.toByte() }.cast()

        // TODO cumSum for Short returns Int but is converted back to T: Short, Issue #558
        typeOf<Short>() -> cast<Short>().cumSum().map { it.toShort() }.cast()

        typeOf<Int?>() -> cast<Int?>().cumSum(skipNA).cast()

        // TODO cumSum for Byte? returns Int? but is converted back to T: Byte?, Issue #558
        typeOf<Byte?>() -> cast<Byte?>().cumSum(skipNA).map { it?.toByte() }.cast()

        // TODO cumSum for Short? returns Int? but is converted back to T: Short?, Issue #558
        typeOf<Short?>() -> cast<Short?>().cumSum(skipNA).map { it?.toShort() }.cast()

        typeOf<Long>() -> cast<Long>().cumSum().cast()

        typeOf<Long?>() -> cast<Long?>().cumSum(skipNA).cast()

        typeOf<BigInteger>() -> cast<BigInteger>().cumSum().cast()

        typeOf<BigInteger?>() -> cast<BigInteger?>().cumSum(skipNA).cast()

        typeOf<BigDecimal>() -> cast<BigDecimal>().cumSum().cast()

        typeOf<BigDecimal?>() -> cast<BigDecimal?>().cumSum(skipNA).cast()

        typeOf<Number?>(), typeOf<Number>() -> convertToDouble().cumSum(skipNA).cast()

        // Cumsum for empty column or column with just null is itself
        nothingType, nullableNothingType -> this

        else -> error("Cumsum for type ${type()} is not supported")
    }

private val supportedClasses = setOf(
    Double::class,
    Float::class,
    Int::class,
    Byte::class,
    Short::class,
    Long::class,
    BigInteger::class,
    BigDecimal::class,
)

// endregion

// region DataFrame

/**
 * {@include [CumSumDocs]}
 * {@set DATA_TYPE [DataFrame]}.
 */
public fun <T, C> DataFrame<T>.cumSum(
    skipNA: Boolean = defaultCumSumSkipNA,
    columns: ColumnsSelector<T, C>,
): DataFrame<T> =
    convert(columns).to { if (it.typeClass in supportedClasses) it.cast<Number?>().cumSum(skipNA) else it }

/**
 * {@include [CumSumDocs]}
 * {@set DATA_TYPE [DataFrame]}.
 */
public fun <T> DataFrame<T>.cumSum(vararg columns: String, skipNA: Boolean = defaultCumSumSkipNA): DataFrame<T> =
    cumSum(skipNA) { columns.toColumnSet() }

/**
 * {@include [CumSumDocs]}
 * {@set DATA_TYPE [DataFrame]}.
 */
@AccessApiOverload
public fun <T> DataFrame<T>.cumSum(
    vararg columns: AnyColumnReference,
    skipNA: Boolean = defaultCumSumSkipNA,
): DataFrame<T> = cumSum(skipNA) { columns.toColumnSet() }

/**
 * {@include [CumSumDocs]}
 * {@set DATA_TYPE [DataFrame]}.
 */
@AccessApiOverload
public fun <T> DataFrame<T>.cumSum(vararg columns: KProperty<*>, skipNA: Boolean = defaultCumSumSkipNA): DataFrame<T> =
    cumSum(skipNA) { columns.toColumnSet() }

/**
 * {@include [CumSumDocs]}
 * {@set DATA_TYPE [DataFrame]}.
 * {@set [CumSumDocs.CUMSUM_PARAM]}
 */
public fun <T> DataFrame<T>.cumSum(skipNA: Boolean = defaultCumSumSkipNA): DataFrame<T> =
    cumSum(skipNA) {
        colsAtAnyDepth { !it.isColumnGroup() }
    }

// endregion

// region GroupBy

/**
 * {@include [CumSumDocs]}
 * {@set DATA_TYPE [GroupBy]}.
 */
public fun <T, G, C> GroupBy<T, G>.cumSum(
    skipNA: Boolean = defaultCumSumSkipNA,
    columns: ColumnsSelector<G, C>,
): GroupBy<T, G> = updateGroups { cumSum(skipNA, columns) }

/**
 * {@include [CumSumDocs]}
 * {@set DATA_TYPE [GroupBy]}.
 */
public fun <T, G> GroupBy<T, G>.cumSum(vararg columns: String, skipNA: Boolean = defaultCumSumSkipNA): GroupBy<T, G> =
    cumSum(skipNA) { columns.toColumnSet() }

/**
 * {@include [CumSumDocs]}
 * {@set DATA_TYPE [GroupBy]}.
 */
@AccessApiOverload
public fun <T, G> GroupBy<T, G>.cumSum(
    vararg columns: AnyColumnReference,
    skipNA: Boolean = defaultCumSumSkipNA,
): GroupBy<T, G> = cumSum(skipNA) { columns.toColumnSet() }

/**
 * {@include [CumSumDocs]}
 * {@set DATA_TYPE [GroupBy]}.
 */
@AccessApiOverload
public fun <T, G> GroupBy<T, G>.cumSum(
    vararg columns: KProperty<*>,
    skipNA: Boolean = defaultCumSumSkipNA,
): GroupBy<T, G> = cumSum(skipNA) { columns.toColumnSet() }

/**
 * {@include [CumSumDocs]}
 * {@set DATA_TYPE [GroupBy]}.
 * {@set [CumSumDocs.CUMSUM_PARAM]}
 */
public fun <T, G> GroupBy<T, G>.cumSum(skipNA: Boolean = defaultCumSumSkipNA): GroupBy<T, G> =
    cumSum(skipNA) {
        colsAtAnyDepth { !it.isColumnGroup() }
    }

// endregion
