package org.jetbrains.kotlinx.dataframe.api

import io.kotest.assertions.throwables.shouldNotThrow
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalTime
import org.jetbrains.kotlinx.dataframe.ColumnsContainer
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.annotations.DataSchema
import org.jetbrains.kotlinx.dataframe.exceptions.CellConversionException
import org.jetbrains.kotlinx.dataframe.exceptions.ColumnTypeMismatchesColumnValuesException
import org.jetbrains.kotlinx.dataframe.exceptions.TypeConversionException
import org.jetbrains.kotlinx.dataframe.exceptions.TypeConverterNotFoundException
import org.jetbrains.kotlinx.dataframe.hasNulls
import org.jetbrains.kotlinx.dataframe.impl.api.toBigDecimal
import org.jetbrains.kotlinx.dataframe.impl.api.toBigInteger
import org.junit.Test
import java.math.BigDecimal
import java.math.BigInteger
import kotlin.math.roundToInt
import kotlin.math.roundToLong
import kotlin.random.Random
import kotlin.reflect.full.starProjectedType
import kotlin.reflect.typeOf
import kotlin.time.Duration.Companion.hours
import java.time.LocalTime as JavaLocalTime

class ConvertTests {

    @Test
    fun `convert LocalTime Kotlin to Java and back`() {
        val time by columnOf(LocalTime(11, 22, 33))
        val converted = time.toDataFrame().convert { time }.to<JavaLocalTime>()
        converted[time][0] shouldBe JavaLocalTime.of(11, 22, 33)

        val convertedBack = converted.convert(time).to<LocalTime>()
        convertedBack[time][0] shouldBe time[0]
    }

    @Test
    fun `convert nullable strings to time`() {
        val time by columnOf("11?22?33", null)
        val converted = time.toDataFrame().convert { time }.toLocalTime("HH?mm?ss")[time]
        converted.hasNulls shouldBe true
        converted[0] shouldBe LocalTime(11, 22, 33)
    }

    @Test
    fun `nullability persistence after conversion`() {
        val col by columnOf("1", null)
        col.convertToInt().forEach {
        }
    }

    @DataSchema
    data class Schema(val time: Instant)

    @Test
    fun `Instant to LocalDateTime`() {
        val df = listOf(Schema(Clock.System.now())).toDataFrame()
        df.convert { time }.toLocalDateTime()
    }

    enum class EnumClass { A, B }

    @Test
    fun `convert string to enum`() {
        columnOf("A", "B").convertTo<EnumClass>() shouldBe columnOf(EnumClass.A, EnumClass.B)

        dataFrameOf(columnOf("A", "B") named "colA")
            .convert("colA").to<EnumClass>()
            .getColumn("colA") shouldBe columnOf(EnumClass.A, EnumClass.B).named("colA")
    }

    @Test
    fun `convert char to enum`() {
        // Char -> String -> Enum
        columnOf('A', 'B').convertTo<EnumClass>() shouldBe columnOf(EnumClass.A, EnumClass.B)

        dataFrameOf(columnOf('A', 'B') named "colA")
            .convert("colA").to<EnumClass>()
            .getColumn("colA") shouldBe columnOf(EnumClass.A, EnumClass.B).named("colA")
    }

    @JvmInline
    value class IntClass(val v: Int)

    @JvmInline
    value class StringClass(val s: String?)

    @JvmInline
    value class PrivateInt(private val v: Int)

    @Test
    fun `convert string to value class`() {
        columnOf("1").convertTo<IntClass>() shouldBe columnOf(IntClass(1))
    }

    @Test
    fun `convert double to value class`() {
        columnOf(1.0).convertTo<IntClass>() shouldBe columnOf(IntClass(1))
    }

    @Test
    fun `convert from value class `() {
        columnOf(IntClass(1)).convertTo<Double>() shouldBe columnOf(1.0)
        columnOf(StringClass("1"), StringClass(null)).convertTo<Double?>() shouldBe columnOf(1.0, null)
    }

    @Test
    fun `convert to value class exceptions`() {
        shouldThrow<TypeConversionException> {
            columnOf("a").convertTo<IntClass>()
        }

        shouldThrow<CellConversionException> {
            columnOf("1", "10", "a").convertTo<IntClass>()
        }.row shouldBe 2

        shouldThrow<CellConversionException> {
            columnOf("1", "x", "2.5").convertToDouble()
        }.row shouldBe 1

        shouldThrow<TypeConverterNotFoundException> {
            columnOf(EnumClass.A).convertTo<IntClass>()
        }
    }

    @Test
    fun `convert from value class exceptions`() {
        shouldThrow<TypeConversionException> {
            columnOf(StringClass("a")).convertTo<Int>()
        }.from shouldBe typeOf<String>()

        shouldThrow<TypeConverterNotFoundException> {
            columnOf(IntClass(1)).convertTo<EnumClass>()
        }

        shouldThrow<TypeConversionException> {
            columnOf(StringClass(null)).convertTo<Double>()
        }

        shouldThrow<TypeConversionException> {
            columnOf(PrivateInt(1)).convertTo<Double>()
        }
    }

    @Test
    fun `convert null strings`() {
        val col = columnOf("none")

        shouldThrow<TypeConversionException> {
            col.convertTo<Int>()
        }

        shouldThrow<TypeConversionException> {
            col.convertTo<Int?>()
        }

        DataFrame.parser.addNullString("none")

        shouldThrow<TypeConversionException> {
            col.convertTo<Int>()
        }

        col.convertTo<Int?>() shouldBe DataColumn.createValueColumn("", listOf(null), typeOf<Int?>())

        DataFrame.parser.resetToDefault()
    }

    @Test
    fun `convert to not nullable`() {
        val col = columnOf(1.0, null)

        col.convertToInt() shouldBe columnOf(1, null)

        shouldThrow<TypeConversionException> {
            col.cast<Double>().convertToInt()
        }

        col.convertTo<Int?>() shouldBe columnOf(1, null)
    }

    @Test
    fun `convert to nullable without nulls`() {
        val col = columnOf(1.0, 2.0)

        col.convertTo<Int?>().hasNulls() shouldBe false
    }

    @Test
    fun `convert instant`() {
        println(Clock.System.now().toEpochMilliseconds())
        val kotlinxInstants = columnOf(Instant.fromEpochMilliseconds(1657283006955))
        shouldNotThrow<TypeConverterNotFoundException> {
            val javaInstant = kotlinxInstants.convertTo<java.time.Instant>()
            javaInstant.convertTo<Instant>() shouldBe kotlinxInstants
        }
    }

    @Test
    fun `convert duration to string`() {
        val col = columnOf(1.hours)
        val res = col.convertTo<String>()
        res.print()
    }

    @Test
    fun `convert Int and Char by code`() {
        val col = columnOf(65, 66)
        col.convertTo<Char>() shouldBe columnOf('A', 'B')
        col.convertTo<Char>().convertTo<Int>() shouldBe col

        // this means
        columnOf('1', '2').convertToInt() shouldNotBe columnOf(1, 2)
        columnOf('1', '2').convertToInt() shouldBe columnOf(49, 50)

        // but
        columnOf('1', '2').convertToString().convertToInt() shouldBe columnOf(1, 2)
        // or
        columnOf('1', '2').parse() shouldBe columnOf(1, 2)
    }

    @Test
    fun `convert all number types to each other`() {
        val numberTypes: List<Number> = listOf(
            Random.nextInt().toByte(),
            Random.nextInt().toShort(),
            Random.nextInt(),
            Random.nextLong(),
            Random.nextFloat(),
            Random.nextDouble(),
            BigInteger.valueOf(Random.nextLong()),
            BigDecimal.valueOf(Random.nextDouble()),
        )
        for (a in numberTypes) {
            val aCol = columnOf(a)
            for (b in numberTypes) {
                val bCol = aCol.convertTo(b::class.starProjectedType)
                when (a) {
                    is Byte ->
                        when (b) {
                            is Byte -> bCol.first() shouldBe a.toByte()
                            is Short -> bCol.first() shouldBe a.toShort()
                            is Int -> bCol.first() shouldBe a.toInt()
                            is Long -> bCol.first() shouldBe a.toLong()
                            is Float -> bCol.first() shouldBe a.toFloat()
                            is Double -> bCol.first() shouldBe a.toDouble()
                            is BigInteger -> bCol.first() shouldBe a.toBigInteger()
                            is BigDecimal -> bCol.first() shouldBe a.toBigDecimal()
                        }

                    is Short ->
                        when (b) {
                            is Byte -> bCol.first() shouldBe a.toByte()
                            is Short -> bCol.first() shouldBe a.toShort()
                            is Int -> bCol.first() shouldBe a.toInt()
                            is Long -> bCol.first() shouldBe a.toLong()
                            is Float -> bCol.first() shouldBe a.toFloat()
                            is Double -> bCol.first() shouldBe a.toDouble()
                            is BigInteger -> bCol.first() shouldBe a.toBigInteger()
                            is BigDecimal -> bCol.first() shouldBe a.toBigDecimal()
                        }

                    is Int ->
                        when (b) {
                            is Byte -> bCol.first() shouldBe a.toByte()
                            is Short -> bCol.first() shouldBe a.toShort()
                            is Int -> bCol.first() shouldBe a.toInt()
                            is Long -> bCol.first() shouldBe a.toLong()
                            is Float -> bCol.first() shouldBe a.toFloat()
                            is Double -> bCol.first() shouldBe a.toDouble()
                            is BigInteger -> bCol.first() shouldBe a.toBigInteger()
                            is BigDecimal -> bCol.first() shouldBe a.toBigDecimal()
                        }

                    is Long ->
                        when (b) {
                            is Byte -> bCol.first() shouldBe a.toByte()
                            is Short -> bCol.first() shouldBe a.toShort()
                            is Int -> bCol.first() shouldBe a.toInt()
                            is Long -> bCol.first() shouldBe a.toLong()
                            is Float -> bCol.first() shouldBe a.toFloat()
                            is Double -> bCol.first() shouldBe a.toDouble()
                            is BigInteger -> bCol.first() shouldBe a.toBigInteger()
                            is BigDecimal -> bCol.first() shouldBe a.toBigDecimal()
                        }

                    is Float ->
                        when (b) {
                            is Byte -> bCol.first() shouldBe a.roundToInt().toByte()
                            is Short -> bCol.first() shouldBe a.roundToInt().toShort()
                            is Int -> bCol.first() shouldBe a.roundToInt()
                            is Long -> bCol.first() shouldBe a.roundToLong()
                            is Float -> bCol.first() shouldBe a.toFloat()
                            is Double -> bCol.first() shouldBe a.toDouble()
                            is BigInteger -> bCol.first() shouldBe a.toBigInteger()
                            is BigDecimal -> bCol.first() shouldBe a.toBigDecimal()
                        }

                    is Double ->
                        when (b) {
                            is Byte -> bCol.first() shouldBe a.roundToInt().toByte()
                            is Short -> bCol.first() shouldBe a.roundToInt().toShort()
                            is Int -> bCol.first() shouldBe a.roundToInt()
                            is Long -> bCol.first() shouldBe a.roundToLong()
                            is Float -> bCol.first() shouldBe a.toFloat()
                            is Double -> bCol.first() shouldBe a.toDouble()
                            is BigInteger -> bCol.first() shouldBe a.toBigInteger()
                            is BigDecimal -> bCol.first() shouldBe a.toBigDecimal()
                        }

                    is BigInteger ->
                        when (b) {
                            is Byte -> bCol.first() shouldBe a.toByte()
                            is Short -> bCol.first() shouldBe a.toShort()
                            is Int -> bCol.first() shouldBe a.toInt()
                            is Long -> bCol.first() shouldBe a.toLong()
                            is Float -> bCol.first() shouldBe a.toFloat()
                            is Double -> bCol.first() shouldBe a.toDouble()
                            is BigInteger -> bCol.first() shouldBe a.toBigInteger()
                            is BigDecimal -> bCol.first() shouldBe a.toBigDecimal()
                        }

                    is BigDecimal ->
                        when (b) {
                            is Byte -> bCol.first() shouldBe a.toByte()
                            is Short -> bCol.first() shouldBe a.toShort()
                            is Int -> bCol.first() shouldBe a.toInt()
                            is Long -> bCol.first() shouldBe a.toLong()
                            is Float -> bCol.first() shouldBe a.toFloat()
                            is Double -> bCol.first() shouldBe a.toDouble()
                            is BigInteger -> bCol.first() shouldBe a.toBigInteger()
                            is BigDecimal -> bCol.first() shouldBe a.toBigDecimal()
                        }
                }
            }
        }
    }

    private interface Marker

    private val ColumnsContainer<Marker>.a get() = this["a"] as DataColumn<String>

    @Test
    fun `convert with buggy extension property`() {
        shouldThrow<ColumnTypeMismatchesColumnValuesException> {
            dataFrameOf("a")(1, 2, 3).cast<Marker>().convert { a }.with { it }
        }
    }
}
