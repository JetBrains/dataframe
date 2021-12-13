package org.jetbrains.kotlinx.dataframe.rendering

import org.jetbrains.kotlinx.dataframe.api.dataFrameOf
import org.jetbrains.kotlinx.dataframe.io.DisplayConfiguration
import org.jetbrains.kotlinx.dataframe.io.formatter
import org.jetbrains.kotlinx.dataframe.jupyter.DefaultCellRenderer
import org.jsoup.Jsoup
import org.jsoup.nodes.Element

abstract class RenderingTestsBase {
    protected fun rowOf(vararg pairs: Pair<String, Any?>) = dataFrameOf(pairs.map { it.first }).withValues(pairs.map { it.second })[0]

    protected fun Any?.truncate(limit: Int): String = format(limit).text()

    protected fun Any?.tooltip(limit: Int): String? = format(limit).children().singleOrNull()?.attr("title")

    protected fun Any?.format(limit: Int): Element = Jsoup.parse(formatter.format(this, DefaultCellRenderer, DisplayConfiguration(cellContentLimit = limit))).body()
}
