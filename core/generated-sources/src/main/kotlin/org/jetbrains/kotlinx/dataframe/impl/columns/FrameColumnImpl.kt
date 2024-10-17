package org.jetbrains.kotlinx.dataframe.impl.columns

import org.jetbrains.kotlinx.dataframe.AnyRow
import org.jetbrains.kotlinx.dataframe.BuildConfig
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.api.schema
import org.jetbrains.kotlinx.dataframe.columns.ColumnGroup
import org.jetbrains.kotlinx.dataframe.columns.ColumnResolutionContext
import org.jetbrains.kotlinx.dataframe.columns.FrameColumn
import org.jetbrains.kotlinx.dataframe.impl.anyNull
import org.jetbrains.kotlinx.dataframe.impl.createStarProjectedType
import org.jetbrains.kotlinx.dataframe.impl.schema.intersectSchemas
import org.jetbrains.kotlinx.dataframe.nrow
import org.jetbrains.kotlinx.dataframe.schema.DataFrameSchema
import kotlin.reflect.KType

internal open class FrameColumnImpl<T> constructor(
    name: String,
    values: List<DataFrame<T>>,
    columnSchema: Lazy<DataFrameSchema>? = null,
    distinct: Lazy<Set<DataFrame<T>>>? = null,
) : DataColumnImpl<DataFrame<T>>(
        values = values,
        name = name,
        type = DataFrame::class.createStarProjectedType(false),
        distinct = distinct,
    ),
    FrameColumn<T> {

    init {
        // Checks for nulls in the `values` list.
        // This only runs with `kotlin.dataframe.debug=true` in gradle.properties.
        if (BuildConfig.DEBUG) {
            require(!values.anyNull()) { "FrameColumn cannot null values." }

//            val schema = columnSchema?.value
//                ?: values.mapNotNull { it.takeIf { it.nrow > 0 }?.schema() }.intersectSchemas()
//
//            for (df in values) {
//                val dfSchema = df.schema()
//                if (dfSchema.columns.isEmpty()) continue
//                require(dfSchema.compare(schema).isDerivedOrEqual()) {
//                    "DataFrames in FrameColumn don't adhere to the given schema:\nGiven:\n$schema\n\nActual:\n$dfSchema"
//                }
//            }
        }
    }

    override fun rename(newName: String) = FrameColumnImpl(newName, values, schema, distinct)

    override fun defaultValue() = null

    override fun addParent(parent: ColumnGroup<*>) = FrameColumnWithParent(parent, this)

    override fun createWithValues(values: List<DataFrame<T>>, hasNulls: Boolean?) =
        DataColumn.createFrameColumn(name, values)

    override fun changeType(type: KType) = throw UnsupportedOperationException()

    override fun distinct() = FrameColumnImpl(name, distinct.value.toList(), schema, distinct)

    override val schema: Lazy<DataFrameSchema> = columnSchema ?: lazy {
        values.mapNotNull { it.takeIf { it.nrow > 0 }?.schema() }.intersectSchemas()
    }

    override fun forceResolve() = ResolvingFrameColumn(this)

    override fun get(indices: Iterable<Int>): FrameColumn<T> =
        DataColumn.createFrameColumn(
            name = name,
            groups = indices.map { values[it] },
        )

    override fun get(columnName: String) =
        throw UnsupportedOperationException("Can not get nested column '$columnName' from FrameColumn '$name'")
}

internal class ResolvingFrameColumn<T>(override val source: FrameColumn<T>) :
    FrameColumn<T> by source,
    ForceResolvedColumn<DataFrame<T>> {

    override fun resolve(context: ColumnResolutionContext) = super<FrameColumn>.resolve(context)

    override fun resolveSingle(context: ColumnResolutionContext) =
        context.df.getColumn<DataFrame<T>>(source.name(), context.unresolvedColumnsPolicy)?.addPath()

    override fun getValue(row: AnyRow) = super<FrameColumn>.getValue(row)

    override fun getValueOrNull(row: AnyRow) = super<FrameColumn>.getValueOrNull(row)

    override fun rename(newName: String) = ResolvingFrameColumn(source.rename(newName))

    override fun toString(): String = source.toString()

    override fun equals(other: Any?) = source.checkEquals(other)

    override fun hashCode(): Int = source.hashCode()
}
