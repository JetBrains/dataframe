package org.jetbrains.dataframe.impl.codeGen

import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.AnyRow
import org.jetbrains.kotlinx.dataframe.api.GroupBy
import org.jetbrains.kotlinx.dataframe.codeGen.CodeWithConverter
import org.jetbrains.kotlinx.dataframe.impl.codeGen.ReplCodeGeneratorImpl
import org.jetbrains.kotlinx.jupyter.api.Code
import kotlin.reflect.KClass
import kotlin.reflect.KProperty

internal interface ReplCodeGenerator {

    fun process(
        df: AnyFrame,
        property: KProperty<*>? = null,
    ): CodeWithConverter<*>

    fun process(
        row: AnyRow,
        property: KProperty<*>? = null,
    ): CodeWithConverter<*>

    fun process(markerClass: KClass<*>): Code

    fun process(groupBy: GroupBy<*, *>): CodeWithConverter<*>

    companion object {
        fun create(): ReplCodeGenerator = ReplCodeGeneratorImpl()
    }
}

internal inline fun <reified T> ReplCodeGenerator.process(): Code = process(T::class)
