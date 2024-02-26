package org.jetbrains.kotlinx.dataframe.testSets.person

import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldContain
import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.api.dataFrameOf
import org.jetbrains.kotlinx.dataframe.api.group
import org.jetbrains.kotlinx.dataframe.api.into
import org.jetbrains.kotlinx.dataframe.api.parse
import org.jetbrains.kotlinx.dataframe.io.toStandaloneHTML
import org.jetbrains.kotlinx.jupyter.util.findNthSubstring
import org.junit.Ignore
import org.junit.Test
import java.awt.Desktop
import java.io.File

class HtmlRenderingTests : BaseTest() {

    fun AnyFrame.browse() {
        val file = File("temp.html") // File.createTempFile("df_rendering", ".html")
        file.writeText(toStandaloneHTML().toString())
        val uri = file.toURI()
        val desktop = Desktop.getDesktop()
        desktop.browse(uri)
    }

    @Ignore
    @Test
    fun test() {
        typed.group { name and age }.into("temp").browse()
    }

    @Test
    fun `render url`() {
        val address = "http://www.google.com"
        val df = dataFrameOf("url")(address).parse()
        val html = df.toStandaloneHTML().toString()
        html shouldContain "href"
        html.findNthSubstring(address, 2) shouldNotBe -1
    }
}
