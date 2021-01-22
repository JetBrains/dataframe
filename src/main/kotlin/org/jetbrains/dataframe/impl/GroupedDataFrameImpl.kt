package org.jetbrains.dataframe.impl

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.api.columns.FrameColumn

internal class GroupedDataFrameImpl<T, G>(val df: DataFrame<T>, override val groups: FrameColumn<G>): GroupedDataFrame<T, G> {

    override val keys by lazy { df - groups }

    override operator fun get(key: GroupKey): DataFrame<T> {

        require(key.size < df.ncol()) { "Invalid size of the key" }

        val keySize = key.size
        val filtered = df.filter { values.subList(0, keySize) == key }
        return filtered[groups].values.union().typed<T>()
    }

    override fun <R> updateGroups(transform: DataFrame<G>.() -> DataFrame<R>) =
            df.update(groups) { transform(it) }.toGrouped { tableColumn<R>(groups.name()) }

    override fun plain() = df

    override fun toString() = df.toString()
}