package org.jetbrains.kotlinx.dataframe.impl.columns

import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataRow
import org.jetbrains.kotlinx.dataframe.columns.ColumnGroup
import kotlin.reflect.KProperty

internal interface DataColumnGroup<out T> :
    ColumnGroup<T>,
    DataColumn<DataRow<T>> {

    override operator fun getValue(thisRef: Any?, property: KProperty<*>): DataColumnGroup<T> =
        super<DataColumn>.getValue(thisRef, property) as DataColumnGroup<T>

    override fun iterator() = super<ColumnGroup>.iterator()

    override fun rename(newName: String): DataColumnGroup<T>

    override fun get(indices: Iterable<Int>): DataColumnGroup<T>

    override fun distinct(): DataColumnGroup<T>

    override fun get(range: IntRange): DataColumnGroup<T>
}
