package org.jetbrains.kotlinx.dataframe.impl

import org.jetbrains.kotlinx.dataframe.AnyCol
import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.DataRow
import org.jetbrains.kotlinx.dataframe.api.ValueCount
import org.jetbrains.kotlinx.dataframe.api.count
import org.jetbrains.kotlinx.dataframe.api.map
import org.jetbrains.kotlinx.dataframe.api.namedValues
import org.jetbrains.kotlinx.dataframe.size

public class Counts(public val value: Any?, public val count: Int) {
    override fun toString(): String = "$value -> $count"
}

public fun nodeExpression(df: AnyFrame): String = "DataFrame ${df.size()}"

public class Info(public val df: AnyFrame)

public fun DataFrame<ValueCount>.render(): List<Counts> = map { Counts(it.get(0), it.count) }

@org.jetbrains.annotations.Debug.Renderer(childrenArray = "this.getColumn().toList().toArray()")
public class ValueColumnImplW(public val column: AnyCol) : List<Any?> by column.toList()

private fun f(v: DataRow<*>) {
    v.namedValues()
}
