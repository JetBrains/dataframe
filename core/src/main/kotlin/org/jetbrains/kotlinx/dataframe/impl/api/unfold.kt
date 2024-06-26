package org.jetbrains.kotlinx.dataframe.impl.api

import org.jetbrains.kotlinx.dataframe.AnyCol
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.api.CreateDataFrameDsl
import org.jetbrains.kotlinx.dataframe.api.asColumnGroup
import org.jetbrains.kotlinx.dataframe.api.asDataColumn
import org.jetbrains.kotlinx.dataframe.api.isPrimitive
import org.jetbrains.kotlinx.dataframe.columns.ColumnKind
import org.jetbrains.kotlinx.dataframe.typeClass

@PublishedApi
internal fun <T> DataColumn<T>.unfoldImpl(skipPrimitive: Boolean, body: CreateDataFrameDsl<T>.() -> Unit): AnyCol {
    return when (kind()) {
        ColumnKind.Group, ColumnKind.Frame -> this
        else -> when {
            skipPrimitive && isPrimitive() -> this
            else -> values().createDataFrameImpl(typeClass) {
                body((this as CreateDataFrameDsl<T>))
            }.asColumnGroup(name()).asDataColumn()
        }
    }
}
