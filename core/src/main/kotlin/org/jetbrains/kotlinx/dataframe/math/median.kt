package org.jetbrains.kotlinx.dataframe.math

import kotlin.reflect.KType
import kotlin.reflect.typeOf

// TODO median always returns the same type, but this can be confusing for iterables of even length
// TODO (e.g. median of [1, 2] should be 1.5, but the type is Int, so it returns 1), Issue #558
@PublishedApi
internal inline fun <reified T : Comparable<T>> Iterable<T?>.median(type: KType = typeOf<T>()): T? =
    percentile(50.0, type)
