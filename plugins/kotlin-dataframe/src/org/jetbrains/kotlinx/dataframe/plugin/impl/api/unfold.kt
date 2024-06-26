package org.jetbrains.kotlinx.dataframe.plugin.impl.api

import org.jetbrains.kotlinx.dataframe.plugin.impl.AbstractInterpreter
import org.jetbrains.kotlinx.dataframe.plugin.impl.AbstractSchemaModificationInterpreter
import org.jetbrains.kotlinx.dataframe.plugin.impl.Arguments
import org.jetbrains.kotlinx.dataframe.plugin.impl.Interpreter
import org.jetbrains.kotlinx.dataframe.plugin.impl.PluginDataFrameSchema
import org.jetbrains.kotlinx.dataframe.plugin.impl.SimpleColumnGroup
import org.jetbrains.kotlinx.dataframe.plugin.impl.SimpleFrameColumn
import org.jetbrains.kotlinx.dataframe.plugin.impl.data.ColumnWithPathApproximation
import org.jetbrains.kotlinx.dataframe.plugin.impl.data.ReplaceClauseApproximation
import org.jetbrains.kotlinx.dataframe.plugin.impl.dataFrame
import org.jetbrains.kotlinx.dataframe.plugin.impl.dsl

class ReplaceUnfold1 : AbstractSchemaModificationInterpreter() {
    val Arguments.receiver: ReplaceClauseApproximation by arg()
    val Arguments.body by dsl()
    val Arguments.typeArg1: TypeApproximation by arg()

    override fun Arguments.interpret(): PluginDataFrameSchema {
        val configuration = CreateDataFrameDslImplApproximation()
        body(configuration, mapOf(Properties0.classExtraArgument to Interpreter.Success(typeArg1.type)))

        return receiver.df.map(receiver.columns.map { it.path.path }.toSet()) { a, column ->
            if (column is SimpleFrameColumn || column is SimpleColumnGroup) return@map column
            SimpleColumnGroup(column.name, configuration.columns)
        }
    }
}

class Replace0 : AbstractInterpreter<ReplaceClauseApproximation>() {
    val Arguments.receiver: PluginDataFrameSchema by dataFrame()
    val Arguments.columns: List<ColumnWithPathApproximation> by arg()

    override fun Arguments.interpret(): ReplaceClauseApproximation {
        return ReplaceClauseApproximation(receiver, columns)
    }
}
