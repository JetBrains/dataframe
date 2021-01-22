package org.jetbrains.dataframe.impl.columns

import org.jetbrains.dataframe.*
import org.jetbrains.dataframe.api.columns.ColumnWithPath

internal class RenamedColumnDef<C>(val source: ColumnDef<C>, val name: String) : ColumnDef<C> {

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<C>? {

        return source.resolveSingle(context)?.let { it.data.rename(name).addPath(it.path) }
    }

    override fun name() = name
}