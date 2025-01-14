package org.jetbrains.kotlinx.dataframe.impl.columns

import org.jetbrains.kotlinx.dataframe.AnyCol
import org.jetbrains.kotlinx.dataframe.AnyRow
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.DataRow
import org.jetbrains.kotlinx.dataframe.api.asColumnGroup
import org.jetbrains.kotlinx.dataframe.api.firstOrNull
import org.jetbrains.kotlinx.dataframe.api.getColumn
import org.jetbrains.kotlinx.dataframe.api.rows
import org.jetbrains.kotlinx.dataframe.columns.ColumnGroup
import org.jetbrains.kotlinx.dataframe.columns.ColumnResolutionContext
import org.jetbrains.kotlinx.dataframe.impl.DataFrameImpl
import org.jetbrains.kotlinx.dataframe.impl.createTypeWithArgument
import org.jetbrains.kotlinx.dataframe.impl.renderSchema
import org.jetbrains.kotlinx.dataframe.nrow
import kotlin.reflect.KType

internal val anyRowType = createTypeWithArgument<AnyRow>()

@org.jetbrains.annotations.Debug.Renderer(text = "\" ColumnGroup \"")
internal open class ColumnGroupImpl<T>(private val name: String, df: DataFrame<T>) :
    DataFrameImpl<T>(df.columns(), df.nrow),
    DataColumnInternal<DataRow<T>>,
    DataColumnGroup<T> {

    override fun values() = rows()

    override fun countDistinct() = distinct.value.size

    override fun type() = anyRowType

    private val distinct = lazy { df.rows().toSet() }

    override fun toSet() = distinct.value

    override fun size() = rowsCount()

    override fun get(index: Int) = super<DataFrameImpl>.get(index)

    override fun get(firstIndex: Int, vararg otherIndices: Int) =
        ColumnGroupImpl(name, super<DataFrameImpl>.get(firstIndex, *otherIndices))

    override fun rename(newName: String) = if (newName == name) this else ColumnGroupImpl(newName, this)

    override fun defaultValue() = null

    override fun get(indices: Iterable<Int>) = ColumnGroupImpl(name, super<DataFrameImpl>.get(indices))

    override fun addParent(parent: ColumnGroup<*>): DataColumn<DataRow<T>> = ColumnGroupWithParent(parent, this)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        val g = other as? ColumnGroup<*> ?: return false
        return name == g.name() && columns == other.columns()
    }

    private fun computeHashCode() = name.hashCode() * 31 + super.hashCode()

    private val hashCode by lazy { computeHashCode() }

    override fun hashCode() = hashCode

    override fun toString() = "$name: {${renderSchema(this)}}"

    override fun changeType(type: KType) = throw UnsupportedOperationException()

    override fun name() = name

    override fun distinct() = ColumnGroupImpl(name, get(distinct.value.map { it.index() }))

    override fun resolve(context: ColumnResolutionContext) = super<DataColumnInternal>.resolve(context)

    override fun resolveSingle(context: ColumnResolutionContext) = super<DataColumnInternal>.resolveSingle(context)

    override fun iterator() = super<DataFrameImpl>.iterator()

    override fun forceResolve() = ResolvingColumnGroup(this)

    override fun get(range: IntRange) = ColumnGroupImpl(name, super<DataFrameImpl>.get(range))

    override fun get(columnName: String): AnyCol = getColumn(columnName)

    override fun contains(value: DataRow<T>) =
        if (distinct.isInitialized()) {
            distinct.value.contains(value)
        } else {
            asColumnGroup()
                .firstOrNull { it == value } != null
        }
}

internal class ResolvingColumnGroup<T>(override val source: ColumnGroupImpl<T>) :
    DataColumnGroup<T> by source,
    ForceResolvedColumn<DataRow<T>> {

    override fun resolve(context: ColumnResolutionContext) = super<DataColumnGroup>.resolve(context)

    override fun resolveSingle(context: ColumnResolutionContext) =
        context.df.getColumn<DataRow<T>>(source.name(), context.unresolvedColumnsPolicy)?.addPath()

    override fun getValue(row: AnyRow) = super<DataColumnGroup>.getValue(row)

    override fun getValueOrNull(row: AnyRow) = super<DataColumnGroup>.getValueOrNull(row)

    override fun rename(newName: String) = ResolvingColumnGroup(source.rename(newName))

    override fun toString(): String = source.toString()

    override fun equals(other: Any?) = source.checkEquals(other)

    override fun hashCode(): Int = source.hashCode()
}
