@file:Suppress("ktlint")

package org.jetbrains.kotlinx.dataframe.samples.api

import org.jetbrains.kotlinx.dataframe.api.reorderColumnsByName
import org.jetbrains.kotlinx.dataframe.api.sortBy
import org.jetbrains.kotlinx.dataframe.api.sortByDesc
import org.jetbrains.kotlinx.dataframe.explainer.TransformDataFrameExpressions
import org.jetbrains.kotlinx.dataframe.io.DataFrameHtmlData
import org.jetbrains.kotlinx.dataframe.io.DisplayConfiguration
import org.jetbrains.kotlinx.dataframe.io.toHTML
import org.jetbrains.kotlinx.dataframe.io.toStandaloneHTML
import org.junit.Ignore
import org.junit.Test
import java.io.File
import kotlin.io.path.Path

class Render : TestBase() {
    @Test
    @TransformDataFrameExpressions
    @Ignore
    fun useRenderingResult() {
        // SampleStart
        df.toStandaloneHTML(DisplayConfiguration(rowsLimit = null)).openInBrowser()
        df.toStandaloneHTML(DisplayConfiguration(rowsLimit = null)).writeHTML(File("/path/to/file"))
        df.toStandaloneHTML(DisplayConfiguration(rowsLimit = null)).writeHTML(Path("/path/to/file"))
        // SampleEnd
    }

    @Test
    fun composeTables() {
        // SampleStart
        val df1 = df.reorderColumnsByName()
        val df2 = df.sortBy { age }
        val df3 = df.sortByDesc { age }

        listOf(df1, df2, df3).fold(DataFrameHtmlData.tableDefinitions()) { acc, df -> acc + df.toHTML() }
        // SampleEnd
    }

    @Test
    @TransformDataFrameExpressions
    fun configureCellOutput() {
        // SampleStart
        df.toHTML(DisplayConfiguration(cellContentLimit = -1))
        // SampleEnd
    }
}
