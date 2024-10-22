package org.jetbrains.kotlinx.dataframe.columns

import io.kotest.matchers.shouldBe
import org.jetbrains.kotlinx.dataframe.api.toColumn
import org.jetbrains.kotlinx.dataframe.api.toDataFrame
import org.junit.Test
import java.net.URI

class DataColumns {
    @Test
    fun `create column with platform type from Api`() {
        val df1 = listOf(1, 2, 3).toDataFrame {
            expr { URI.create("http://example.com") } into "text"
        }
        df1["text"].type().toString() shouldBe "java.net.URI"
    }

    @Test
    fun `create column with nullable platform type from Api`() {
        val df1 = listOf(1, 2, 3).toDataFrame {
            expr { i -> URI.create("http://example.com").takeIf { i == 2 } } into "text"
        }
        df1["text"].type().toString() shouldBe "java.net.URI?"
    }

    @Test
    fun `create column with nullable platform type from factory method`() {
        val col = listOf(URI.create("http://example.com"), null).toColumn("a")
        col.type().toString() shouldBe "java.net.URI?"
    }
}
