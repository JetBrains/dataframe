package org.jetbrains.kotlinx.dataframe.api

import io.kotest.matchers.shouldBe
import org.jetbrains.kotlinx.dataframe.DataRow
import org.jetbrains.kotlinx.dataframe.io.readJsonStr
import org.junit.Test
import kotlin.reflect.typeOf

class ReplaceTests {

    @Test
    fun `replace named`() {
        val df = dataFrameOf("a")(1)
        val conv = df.replace { "a"<Int>() named "b" }.with { it.convertToDouble() }
        conv.columnNames() shouldBe listOf("b")
        conv.columnTypes() shouldBe listOf(typeOf<Double>())
    }

    @Test
    fun `unfold primitive`() {
        val a by columnOf("123")
        val df = dataFrameOf(a)

        val conv = df.replace { a }.unfold {
            "b" from { it }
            "c" from { DataRow.readJsonStr("""{"prop": 1}""") }
        }

        val b = conv["a"]["b"]
        b.type() shouldBe typeOf<String>()
        b.values() shouldBe listOf("123")

        val c = conv["a"]["c"]["prop"]
        c.type() shouldBe typeOf<Int>()
        c.values() shouldBe listOf(1)
    }

    @Test
    fun `unfold properties`() {
        val col by columnOf(A("1", 123, B(3.0)))
        val df1 = dataFrameOf(col)
        val conv = df1.replace { col }.unfold(maxDepth = 2)

        val a = conv["col"]["a"]
        a.type() shouldBe typeOf<String>()
        a.values() shouldBe listOf("1")

        val b = conv["col"]["b"]
        b.type() shouldBe typeOf<Int>()
        b.values() shouldBe listOf(123)

        val d = conv["col"]["bb"]["d"]
        d.type() shouldBe typeOf<Double>()
        d.values() shouldBe listOf(3.0)
    }

    class B(val d: Double)
    class A(val a: String, val b: Int, val bb: B)

    @Test
    fun `skip primitive`() {
        val col1 by columnOf("1", "2")
        val col2 by columnOf(B(1.0), B(2.0))
        val df1 = dataFrameOf(col1, col2)
        val conv = df1.replace { nameStartsWith("col") }.unfold()

        val a = conv["col1"]
        a.type() shouldBe typeOf<String>()
        a.values() shouldBe listOf("1", "2")

        val b = conv["col2"]["d"]
        b.type() shouldBe typeOf<Double>()
        b.values() shouldBe listOf(1.0, 2.0)
    }
}
