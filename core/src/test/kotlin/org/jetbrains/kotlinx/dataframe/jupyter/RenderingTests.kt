package org.jetbrains.kotlinx.dataframe.jupyter

import io.kotest.assertions.throwables.shouldNotThrow
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.comparables.shouldBeLessThan
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldNotContain
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.intellij.lang.annotations.Language
import org.jetbrains.kotlinx.dataframe.impl.io.SerializationKeys.DATA
import org.jetbrains.kotlinx.dataframe.impl.io.SerializationKeys.KOTLIN_DATAFRAME
import org.jetbrains.kotlinx.dataframe.impl.io.SerializationKeys.METADATA
import org.jetbrains.kotlinx.jupyter.api.MimeTypedResult
import org.jetbrains.kotlinx.jupyter.testkit.JupyterReplTestCase
import org.junit.BeforeClass
import org.junit.Test

class RenderingTests : JupyterReplTestCase() {
    @Test
    fun `dataframe is rendered to html`() {
        @Language("kts")
        val html = execHtml(
            """
            val name by column<String>()
            val height by column<Int>()
            val df = dataFrameOf(name, height)(
                "Bill", 135,
                "Charlie", 160
            )
            df
            """.trimIndent(),
        )
        html shouldContain "Bill"

        @Language("kts")
        val useRes = execRendered(
            """
            USE {
                render<Int> { (it * 2).toString() }
            }
            """.trimIndent(),
        )
        useRes shouldBe Unit

        val html2 = execHtml("df")
        html2 shouldContain (135 * 2).toString()
        html2 shouldContain (160 * 2).toString()
    }

    @Test
    fun `rendering options`() {
        @Language("kts")
        val html1 = execHtml(
            """
            data class Person(val age: Int, val name: String)
            val df = (1..70).map { Person(it, "A".repeat(it)) }.toDataFrame()
            df
            """.trimIndent(),
        )
        html1 shouldContain "showing only top 20 of 70 rows"

        @Language("kts")
        val html2 = execHtml(
            """
            dataFrameConfig.display.rowsLimit = 50
            df
            """.trimIndent(),
        )
        html2 shouldContain "showing only top 50 of 70 rows"
    }

    @Test
    fun `dark color scheme`() {
        fun execSimpleDf() = execHtml("""dataFrameOf("a", "b")(1, 2, 3, 4)""")

        val htmlLight = execSimpleDf()
        val r1 = execRendered("notebook.changeColorScheme(ColorScheme.DARK); 1")
        val htmlDark = execSimpleDf()

        r1 shouldBe 1
        val darkClassAttribute = """theme='dark'"""
        htmlLight shouldNotContain darkClassAttribute
        htmlDark shouldContain darkClassAttribute
    }

    @Test
    fun `test kotlin notebook plugin utils rows subset`() {
        val json = executeScriptAndParseDataframeResult(
            """
            data class Row(val id: Int)
            val df = (1..100).map { Row(it) }.toDataFrame()
            KotlinNotebookPluginUtils.getRowsSubsetForRendering(df, 20 , 50)
            """.trimIndent(),
        )

        assertDataFrameDimensions(json, 30, 1)

        val rows = json[KOTLIN_DATAFRAME]!!.jsonArray
        rows.getObj(0)["id"]?.jsonPrimitive?.int shouldBe 21
        rows.getObj(rows.lastIndex)["id"]?.jsonPrimitive?.int shouldBe 50
    }

    /**
     * Executes the given `script` and parses the resulting DataFrame as a `JsonObject`.
     *
     * @param script the script to be executed
     * @return the parsed DataFrame result as a `JsonObject`
     */
    private fun executeScriptAndParseDataframeResult(
        @Language("kts") script: String,
    ): JsonObject {
        val result = execRendered<MimeTypedResult>(script)
        return parseDataframeJson(result)
    }

    private fun assertDataFrameDimensions(json: JsonObject, expectedRows: Int, expectedColumns: Int) {
        json[METADATA]!!.jsonObject["nrow"]!!.jsonPrimitive.int shouldBe expectedRows
        json[METADATA]!!.jsonObject["ncol"]!!.jsonPrimitive.int shouldBe expectedColumns
    }

    private fun parseDataframeJson(result: MimeTypedResult): JsonObject =
        Json.decodeFromString<JsonObject>(result["application/kotlindataframe+json"]!!)

    private fun JsonArray.getObj(index: Int) = this[index].jsonObject

    @Test
    fun `test kotlin notebook plugin utils sort by one column asc`() {
        val json = executeScriptAndParseDataframeResult(
            """
            data class CustomRow(val id: Int, val category: String)
            val df = (1..100).map { CustomRow(it, if (it % 2 == 0) "even" else "odd") }.toDataFrame()
            KotlinNotebookPluginUtils.sortByColumns(df, listOf(listOf("id")), listOf(false))
            """.trimIndent(),
        )

        assertDataFrameDimensions(json, 100, 2)
        assertSortedById(json, false)
    }

    @Suppress("UNCHECKED_CAST")
    private fun assertSortedById(json: JsonObject, desc: Boolean) {
        val rows = json[KOTLIN_DATAFRAME]!!.jsonArray as List<JsonObject>
        var previousId = if (desc) 101 else 0
        rows.forEach { row: JsonObject ->
            val currentId = row["id"]!!.jsonPrimitive.int
            if (desc) currentId shouldBeLessThan previousId else currentId shouldBeGreaterThan previousId
            previousId = currentId
        }
    }

    @Test
    fun `test kotlin notebook plugin utils sort by one column desc`() {
        val json = executeScriptAndParseDataframeResult(
            """
            data class CustomRow(val id: Int, val category: String)
            val df = (1..100).map { CustomRow(it, if (it % 2 == 0) "even" else "odd") }.toDataFrame()
            KotlinNotebookPluginUtils.sortByColumns(df, listOf(listOf("id")), listOf(true))
            """.trimIndent(),
        )

        assertDataFrameDimensions(json, 100, 2)
        assertSortedById(json, true)
    }

    @Suppress("UNCHECKED_CAST")
    @Test
    fun `test kotlin notebook plugin utils sort by multiple columns`() {
        val json = executeScriptAndParseDataframeResult(
            """
            data class CustomRow(val id: Int, val category: String)
            val df = (1..100).map { CustomRow(it, if (it % 2 == 0) "even" else "odd") }.toDataFrame()
            KotlinNotebookPluginUtils.getRowsSubsetForRendering(
                KotlinNotebookPluginUtils.sortByColumns(df, listOf(listOf("category"), listOf("id")), listOf(true, false)),
                0, 100
            )
            """.trimIndent(),
        )

        assertDataFrameDimensions(json, 100, 2)

        val rows = json[KOTLIN_DATAFRAME]!!.jsonArray as List<JsonObject>
        assertSortedByCategory(rows)
        assertSortedById(rows)
    }

    private fun assertSortedByCategory(rows: List<JsonObject>) {
        rows.forEachIndexed { i, row ->
            val currentCategory = row["category"]!!.jsonPrimitive.content
            if (i < 50) {
                currentCategory shouldBe "odd"
            } else {
                currentCategory shouldBe "even"
            }
        }
    }

    private fun assertSortedById(rows: List<JsonObject>) {
        var previousCategory = "odd"
        var previousId = 0
        for (row in rows) {
            val currentCategory = row["category"]!!.jsonPrimitive.content
            val currentId = row["id"]!!.jsonPrimitive.int

            if (previousCategory == "odd" && currentCategory == "even") {
                previousId shouldBeGreaterThan currentId
            } else if (previousCategory == currentCategory) {
                previousId shouldBeLessThan currentId
            }

            previousCategory = currentCategory
            previousId = currentId
        }
    }

    @Test
    fun `json metadata contains schema metadata`() {
        val json = executeScriptAndParseDataframeResult(
            """
            val col1 by columnOf("a", "b", "c")
            val col2 by columnOf(1, 2, 3)
            val col3 by columnOf("Foo", "Bar", null)
            val df2 = dataFrameOf(Pair("header", listOf("A", "B", "C")))
            val col4 by columnOf(df2, df2, df2)
            var df = dataFrameOf(col1, col2, col3, col4)
            df.group("col1", "col2").into("group")            
            """.trimIndent(),
        )
        val expectedOutput =
            """
            {
              "${'$'}version": "2.1.1",
              "metadata": {
                "columns": ["group", "col3", "col4"],
                "types": [{
                  "kind": "ColumnGroup"
                }, {
                  "kind": "ValueColumn",
                  "type": "kotlin.String?"
                }, {
                  "kind": "FrameColumn"
                }],
                "nrow": 3,
                "ncol": 3
              },
              "kotlin_dataframe": [{
                "group": {
                  "data": {
                    "col1": "a",
                    "col2": 1
                  },
                  "metadata": {
                    "kind": "ColumnGroup",
                    "columns": ["col1", "col2"],
                    "types": [{
                      "kind": "ValueColumn",
                      "type": "kotlin.String"
                    }, {
                      "kind": "ValueColumn",
                      "type": "kotlin.Int"
                    }]
                  }
                },
                "col3": "Foo",
                "col4": {
                  "data": [{
                    "header": "A"
                  }, {
                    "header": "B"
                  }, {
                    "header": "C"
                  }],
                  "metadata": {
                    "kind": "FrameColumn",
                    "columns": ["header"],
                    "types": [{
                      "kind": "ValueColumn",
                      "type": "kotlin.String"
                    }],
                    "ncol": 1,
                    "nrow": 3
                  }
                }
              }, {
                "group": {
                  "data": {
                    "col1": "b",
                    "col2": 2
                  },
                  "metadata": {
                    "kind": "ColumnGroup",
                    "columns": ["col1", "col2"],
                    "types": [{
                      "kind": "ValueColumn",
                      "type": "kotlin.String"
                    }, {
                      "kind": "ValueColumn",
                      "type": "kotlin.Int"
                    }]
                  }
                },
                "col3": "Bar",
                "col4": {
                  "data": [{
                    "header": "A"
                  }, {
                    "header": "B"
                  }, {
                    "header": "C"
                  }],
                  "metadata": {
                    "kind": "FrameColumn",
                    "columns": ["header"],
                    "types": [{
                      "kind": "ValueColumn",
                      "type": "kotlin.String"
                    }],
                    "ncol": 1,
                    "nrow": 3
                  }
                }
              }, {
                "group": {
                  "data": {
                    "col1": "c",
                    "col2": 3
                  },
                  "metadata": {
                    "kind": "ColumnGroup",
                    "columns": ["col1", "col2"],
                    "types": [{
                      "kind": "ValueColumn",
                      "type": "kotlin.String"
                    }, {
                      "kind": "ValueColumn",
                      "type": "kotlin.Int"
                    }]
                  }
                },
                "col3": null,
                "col4": {
                  "data": [{
                    "header": "A"
                  }, {
                    "header": "B"
                  }, {
                    "header": "C"
                  }],
                  "metadata": {
                    "kind": "FrameColumn",
                    "columns": ["header"],
                    "types": [{
                      "kind": "ValueColumn",
                      "type": "kotlin.String"
                    }],
                    "ncol": 1,
                    "nrow": 3
                  }
                }
              }]
            }
            """.trimIndent()
        json shouldBe Json.parseToJsonElement(expectedOutput)
    }

    @Test
    fun `test kotlin dataframe conversion groupby`() {
        val json = executeScriptAndParseDataframeResult(
            """
            data class Row(val id: Int, val group: Int)
            val df = (1..20).map { Row(it, if (it <= 10) 1 else 2) }.toDataFrame()
            KotlinNotebookPluginUtils.convertToDataFrame(df.groupBy("group"))
            """.trimIndent(),
        )

        assertDataFrameDimensions(json, 2, 2)

        val rows = json[KOTLIN_DATAFRAME]!!.jsonArray
        rows.getObj(0)["group1"]!!
            .jsonObject[DATA]!!
            .jsonArray.size shouldBe 10
        rows.getObj(1)["group1"]!!
            .jsonObject[DATA]!!
            .jsonArray.size shouldBe 10
    }

    // Regression KTNB-424
    @Test
    fun `test kotlin dataframe conversion ReducedGroupBy`() {
        shouldNotThrow<Throwable> {
            val json = executeScriptAndParseDataframeResult(
                """
                data class Row(val id: Int, val group: Int)
                val df = (1..100).map { Row(it, if (it <= 50) 1 else 2) }.toDataFrame()
                KotlinNotebookPluginUtils.convertToDataFrame(df.groupBy("group").first())
                """.trimIndent(),
            )

            assertDataFrameDimensions(json, 2, 2)
        }
    }

    @Test
    fun `test sortByColumns by int column`() {
        val json = executeScriptAndParseDataframeResult(
            """
            val df = dataFrameOf("nums")(5, 4, 3, 2, 1)
            val res = KotlinNotebookPluginUtils.sortByColumns(df, listOf(listOf("nums")), listOf(false))
            KotlinNotebookPluginUtils.convertToDataFrame(res)
            """.trimIndent(),
        )

        val rows = json[KOTLIN_DATAFRAME]!!.jsonArray
        json.extractColumn<Int>(0, "nums") shouldBe 1
        json.extractColumn<Int>(rows.size - 1, "nums") shouldBe 5
    }

    internal inline fun <reified T> JsonObject.extractColumn(index: Int, fieldName: String): T {
        val element = this[KOTLIN_DATAFRAME]!!.jsonArray[index].jsonObject[fieldName]!!.jsonPrimitive
        return when (T::class) {
            String::class -> element.content as T
            Int::class -> element.int as T
            else -> throw IllegalArgumentException("Unsupported type")
        }
    }

    @Test
    fun `test sortByColumns by multiple int columns`() {
        val json = executeScriptAndParseDataframeResult(
            """
            data class Row(val a: Int, val b: Int)
            val df = listOf(Row(1, 1), Row(1, 2), Row(2, 3), Row(2, 4), Row(3, 5), Row(3, 6)).toDataFrame()
            val res = KotlinNotebookPluginUtils.sortByColumns(df, listOf(listOf("a"), listOf("b")), listOf(true, false))
            KotlinNotebookPluginUtils.convertToDataFrame(res)
            """.trimIndent(),
        )

        json.extractColumn<Int>(0, "a") shouldBe 3
        json.extractColumn<Int>(0, "b") shouldBe 5
        json.extractColumn<Int>(5, "a") shouldBe 1
        json.extractColumn<Int>(5, "b") shouldBe 2
    }

    @Test
    fun `test sortByColumns by single string column`() {
        val json = executeScriptAndParseDataframeResult(
            """
            val df = dataFrameOf("letters")("e", "d", "c", "b", "a")
            val res = KotlinNotebookPluginUtils.sortByColumns(df, listOf(listOf("letters")), listOf(true))
            KotlinNotebookPluginUtils.convertToDataFrame(res)
            """.trimIndent(),
        )

        json.extractColumn<String>(0, "letters") shouldBe "e"
        json.extractColumn<String>(4, "letters") shouldBe "a"
    }

    @Test
    fun `test sortByColumns by multiple string columns`() {
        val json = executeScriptAndParseDataframeResult(
            """
            data class Row(val first: String, val second: String)
            val df = listOf(Row("a", "b"), Row("a", "a"), Row("b", "b"), Row("b", "a")).toDataFrame()
            val res = KotlinNotebookPluginUtils.sortByColumns(df, listOf(listOf("first"), listOf("second")), listOf(false, true))
            KotlinNotebookPluginUtils.convertToDataFrame(res)
            """.trimIndent(),
        )

        json.extractColumn<String>(0, "first") shouldBe "a"
        json.extractColumn<String>(0, "second") shouldBe "b"
        json.extractColumn<String>(3, "first") shouldBe "b"
        json.extractColumn<String>(3, "second") shouldBe "a"
    }

    @Test
    fun `test sortByColumns by mix of int and string columns`() {
        val json = executeScriptAndParseDataframeResult(
            """
            data class Row(val num: Int, val letter: String)
            val df = listOf(Row(1, "a"), Row(1, "b"), Row(2, "a"), Row(2, "b"), Row(3, "a")).toDataFrame()
            val res = KotlinNotebookPluginUtils.sortByColumns(df, listOf(listOf("num"), listOf("letter")), listOf(true, false))
            KotlinNotebookPluginUtils.convertToDataFrame(res)
            """.trimIndent(),
        )

        json.extractColumn<Int>(0, "num") shouldBe 3
        json.extractColumn<String>(0, "letter") shouldBe "a"
        json.extractColumn<Int>(4, "num") shouldBe 1
        json.extractColumn<String>(4, "letter") shouldBe "b"
    }

    @Test
    fun `test sortByColumns by multiple non-comparable column`() {
        val json = executeScriptAndParseDataframeResult(
            """
            data class Person(val name: String, val age: Int) {
                override fun toString(): String {
                    return age.toString()
                }
            }
            val df = dataFrameOf("urls", "person")(
                URL("https://example.com/a"), Person("Alice", 10), 
                URL("https://example.com/b"), Person("Bob", 11),
                URL("https://example.com/a"), Person("Nick", 12),
                URL("https://example.com/b"), Person("Guy", 13),
            )
            val res = KotlinNotebookPluginUtils.sortByColumns(df, listOf(listOf("urls"), listOf("person")), listOf(false, true))
            KotlinNotebookPluginUtils.convertToDataFrame(res)
            """.trimIndent(),
        )

        json.extractColumn<Int>(0, "person") shouldBe 12
        json.extractColumn<Int>(3, "person") shouldBe 11
    }

    @Test
    fun `test sortByColumns by mix of comparable and non-comparable columns`() {
        val json = executeScriptAndParseDataframeResult(
            """
            val df = dataFrameOf("urls", "id")(
                URL("https://example.com/a"), 1, 
                URL("https://example.com/b"), 2,
                URL("https://example.com/a"), 2,
                URL("https://example.com/b"), 1,
            )
            val res = KotlinNotebookPluginUtils.sortByColumns(df, listOf(listOf("id"), listOf("urls")), listOf(true, true))
            KotlinNotebookPluginUtils.convertToDataFrame(res)
            """.trimIndent(),
        )

        json.extractColumn<String>(0, "urls") shouldBe "https://example.com/b"
        json.extractColumn<Int>(0, "id") shouldBe 2
        json.extractColumn<String>(3, "urls") shouldBe "https://example.com/a"
        json.extractColumn<Int>(3, "id") shouldBe 1
    }

    @Test
    fun `test sortByColumns by url column`() {
        val json = executeScriptAndParseDataframeResult(
            """
            val df = dataFrameOf("urls")(
                URL("https://example.com/a"),
                URL("https://example.com/c"),
                URL("https://example.com/b"),
                URL("https://example.com/d")
            )
            val res = KotlinNotebookPluginUtils.sortByColumns(df, listOf(listOf("urls")), listOf(false))
            KotlinNotebookPluginUtils.convertToDataFrame(res)
            """.trimIndent(),
        )

        json.extractColumn<String>(0, "urls") shouldBe "https://example.com/a"
        json.extractColumn<String>(1, "urls") shouldBe "https://example.com/b"
        json.extractColumn<String>(2, "urls") shouldBe "https://example.com/c"
        json.extractColumn<String>(3, "urls") shouldBe "https://example.com/d"
    }

    @Test
    fun `test sortByColumns by column group children`() {
        val json = executeScriptAndParseDataframeResult(
            """
            val df = dataFrameOf(
                "a" to listOf(5, 4, 3, 2, 1),
                "b" to listOf(1, 2, 3, 4, 5)
            )
            val res = KotlinNotebookPluginUtils.sortByColumns(df.group("a", "b").into("c"), listOf(listOf("c", "a")), listOf(false))
            KotlinNotebookPluginUtils.convertToDataFrame(res)
            """.trimIndent(),
        )

        fun JsonObject.extractBFields(): List<Int> {
            val dataframe = this[KOTLIN_DATAFRAME]!!.jsonArray
            return dataframe.map { it.jsonObject["c"]!!.jsonObject["data"]!!.jsonObject["b"]!!.jsonPrimitive.int }
        }

        val bFields = json.extractBFields()
        bFields shouldBe listOf(5, 4, 3, 2, 1)
    }

    @Test
    fun `test sortByColumns for column that contains string and int`() {
        val json = executeScriptAndParseDataframeResult(
            """
            val df = dataFrameOf("mixed")(
                5,
                "10",
                2,
                "4",
                "1"
            )
            val res = KotlinNotebookPluginUtils.sortByColumns(df, listOf(listOf("mixed")), listOf(true))
            KotlinNotebookPluginUtils.convertToDataFrame(res)
            """.trimIndent(),
        )

        json.extractColumn<String>(0, "mixed") shouldBe "5"
        json.extractColumn<String>(1, "mixed") shouldBe "4"
        json.extractColumn<String>(2, "mixed") shouldBe "2"
        json.extractColumn<String>(3, "mixed") shouldBe "10"
        json.extractColumn<String>(4, "mixed") shouldBe "1"
    }

    companion object {
        /**
         * Set the system property for the IDE version needed for specific serialization testing purposes.
         */
        @BeforeClass
        @JvmStatic
        internal fun setupOnce() {
            System.setProperty("KTNB_IDE_BUILD_NUMBER", "IU;241;14015")
        }
    }
}
