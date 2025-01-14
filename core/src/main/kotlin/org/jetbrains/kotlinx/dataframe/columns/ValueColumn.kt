package org.jetbrains.kotlinx.dataframe.columns

import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.api.dataFrameOf
import org.jetbrains.kotlinx.dataframe.api.group
import org.jetbrains.kotlinx.dataframe.api.into
import org.jetbrains.kotlinx.dataframe.api.print
import org.jetbrains.kotlinx.dataframe.size
import kotlin.reflect.KProperty

/**
 * Column that stores values.
 *
 * Can be instantiated by [DataColumn.createValueColumn].
 *
 * @param T - type of values
 */
@org.jetbrains.annotations.Debug.Renderer(
    text = "this.name() + \": \" + org.jetbrains.kotlinx.dataframe.impl.RenderingKt.renderType(this)",
    childrenArray = "this.toList().toArray()",
)
public interface ValueColumn<out T> : DataColumn<T> {

    override fun kind(): ColumnKind = ColumnKind.Value

    override fun distinct(): ValueColumn<T>

    override fun get(indices: Iterable<Int>): ValueColumn<T>

    override fun rename(newName: String): ValueColumn<T>

    override operator fun getValue(thisRef: Any?, property: KProperty<*>): ValueColumn<T> =
        super.getValue(thisRef, property) as ValueColumn<T>

    public override operator fun get(range: IntRange): ValueColumn<T>
}

internal fun main() {
    val df = dataFrameOf("a", "b")(123, "aaa").group("a", "b").into("c")
    df.size().toString()
    df.print()
}
