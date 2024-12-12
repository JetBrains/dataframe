@file:Suppress("INVISIBLE_REFERENCE", "INVISIBLE_MEMBER")

package org.jetbrains.kotlinx.dataframe.plugin.impl.api

import org.jetbrains.kotlinx.dataframe.api.JoinType
import org.jetbrains.kotlinx.dataframe.plugin.impl.AbstractInterpreter
import org.jetbrains.kotlinx.dataframe.plugin.impl.Arguments
import org.jetbrains.kotlinx.dataframe.impl.ColumnNameGenerator
import org.jetbrains.kotlinx.dataframe.plugin.impl.PluginDataFrameSchema
import org.jetbrains.kotlinx.dataframe.plugin.impl.Present
import org.jetbrains.kotlinx.dataframe.plugin.impl.SimpleCol
import org.jetbrains.kotlinx.dataframe.plugin.impl.SimpleColumnGroup
import org.jetbrains.kotlinx.dataframe.plugin.impl.dataFrame
import org.jetbrains.kotlinx.dataframe.plugin.impl.enum
import org.jetbrains.kotlinx.dataframe.plugin.impl.makeNullable

internal class Join0 : AbstractInterpreter<PluginDataFrameSchema>() {
    val Arguments.receiver: PluginDataFrameSchema by dataFrame()
    val Arguments.other: PluginDataFrameSchema by dataFrame()
    val Arguments.selector: ColumnMatchApproximation by arg()
    val Arguments.type: JoinType by enum(defaultValue = Present(JoinType.Inner))

    override fun Arguments.interpret(): PluginDataFrameSchema {
        val nameGenerator = ColumnNameGenerator()
        val left = receiver.columns()
        val right = removeImpl(other.columns(), setOf(selector.right.resolve(receiver).single().path.path)).updatedColumns

        val rightColumnGroups = right.filterIsInstance<SimpleColumnGroup>().associateBy { it.name }

        val mergedGroups = buildMap {
            left.filterIsInstance<SimpleColumnGroup>().forEach { leftGroup ->
                val rightGroup = rightColumnGroups[leftGroup.name]
                if (rightGroup != null) {
                    val merge = ColumnNameGenerator()
                    val columns = (leftGroup.columns() + rightGroup.columns()).map { column ->
                        val uniqueName = merge.addUnique(column.name)
                        column.rename(uniqueName)
                    }
                    put(leftGroup.name, SimpleColumnGroup(leftGroup.name, columns))
                }
            }
        }

        fun MutableList<SimpleCol>.addColumns(columns: List<SimpleCol>) {
            for (column in columns) {
                val uniqueName = nameGenerator.addUnique(column.name)
                val colToAdd = mergedGroups[column.name] ?: column
                add(colToAdd.rename(uniqueName))
            }
        }

        // remove impl merged groups?
        val columns = buildList {
            when (type) {
                JoinType.Inner -> {
                    addColumns(left)
                    addColumns(right.filterNot { it is SimpleColumnGroup && it.name() in mergedGroups })
                }
                JoinType.Left -> {
                    addColumns(left)
                    addColumns(right.filterNot { it is SimpleColumnGroup && it.name() in mergedGroups }.map { makeNullable(it) })
                }
                JoinType.Right -> {
                    addColumns(left.filterNot { it is SimpleColumnGroup && it.name() in mergedGroups }.map { makeNullable(it) })
                    addColumns(right)
                }
                JoinType.Full -> {
                    addColumns(left.map { makeNullable(it) })
                    addColumns(right.filterNot { it is SimpleColumnGroup && it.name() in mergedGroups }.map { makeNullable(it) })
                }
                JoinType.Filter -> addColumns(left)
                JoinType.Exclude -> addColumns(left)
            }
        }
        return PluginDataFrameSchema(columns)
    }
}
