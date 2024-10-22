package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.impl.between

// region DataColumn

public fun <T : Comparable<T>> DataColumn<T>.between(
    left: T,
    right: T,
    includeBoundaries: Boolean = true,
): DataColumn<Boolean> = map { it.between(left, right, includeBoundaries) }

// endregion
