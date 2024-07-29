package org.jetbrains.kotlinx.dataframe.impl

import kotlin.reflect.KType

private val listTypes = mutableMapOf<KType, KType>()

@PublishedApi
internal fun getListType(valueType: KType): KType =
    listTypes.getOrPut(valueType) { List::class.createTypeWithArgument(valueType) }
