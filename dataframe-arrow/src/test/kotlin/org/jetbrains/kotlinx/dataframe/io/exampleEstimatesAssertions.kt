package org.jetbrains.kotlinx.dataframe.io

import io.kotest.matchers.shouldBe
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.datetime.toKotlinLocalDate
import kotlinx.datetime.toKotlinLocalDateTime
import kotlinx.datetime.toKotlinLocalTime
import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.api.forEachIndexed
import java.math.BigInteger
import java.time.ZoneOffset
import kotlin.math.absoluteValue
import kotlin.math.pow
import kotlin.reflect.full.withNullability
import kotlin.reflect.typeOf
import java.time.LocalDate as JavaLocalDate
import java.time.LocalDateTime as JavaLocalDateTime
import java.time.LocalTime as JavaLocalTime

/**
 * Assert that we have got the same data that was originally saved on example creation.
 * Example generation project is currently located at https://github.com/Kopilov/arrow_example
 */
internal fun assertEstimations(
    exampleFrame: AnyFrame,
    expectedNullable: Boolean,
    hasNulls: Boolean,
    fromParquet: Boolean = false,
) {
    /**
     * In [exampleFrame] we get two concatenated batches. To assert the estimations, we should transform frame row number to batch row number
     */
    fun iBatch(iFrame: Int): Int {
        val firstBatchSize = 100
        return if (iFrame < firstBatchSize) iFrame else iFrame - firstBatchSize
    }

    fun expectedNull(rowNumber: Int): Boolean = (rowNumber + 1) % 5 == 0

    fun assertValueOrNull(rowNumber: Int, actual: Any?, expected: Any) {
        if (hasNulls && expectedNull(rowNumber)) {
            actual shouldBe null
        } else {
            actual shouldBe expected
        }
    }

    val asciiStringCol = exampleFrame["asciiString"] as DataColumn<String?>
    asciiStringCol.type() shouldBe typeOf<String>().withNullability(expectedNullable)
    asciiStringCol.forEachIndexed { i, element ->
        assertValueOrNull(iBatch(i), element, "Test Example ${iBatch(i)}")
    }

    val utf8StringCol = exampleFrame["utf8String"] as DataColumn<String?>
    utf8StringCol.type() shouldBe typeOf<String>().withNullability(expectedNullable)
    utf8StringCol.forEachIndexed { i, element ->
        assertValueOrNull(iBatch(i), element, "Тестовый пример ${iBatch(i)}")
    }

    val largeStringCol = exampleFrame["largeString"] as DataColumn<String?>
    largeStringCol.type() shouldBe typeOf<String>().withNullability(expectedNullable)
    largeStringCol.forEachIndexed { i, element ->
        assertValueOrNull(iBatch(i), element, "Test Example Should Be Large ${iBatch(i)}")
    }

    val booleanCol = exampleFrame["boolean"] as DataColumn<Boolean?>
    booleanCol.type() shouldBe typeOf<Boolean>().withNullability(expectedNullable)
    booleanCol.forEachIndexed { i, element ->
        assertValueOrNull(iBatch(i), element, iBatch(i) % 2 == 0)
    }

    val byteCol = exampleFrame["byte"] as DataColumn<Byte?>
    byteCol.type() shouldBe typeOf<Byte>().withNullability(expectedNullable)
    byteCol.forEachIndexed { i, element ->
        assertValueOrNull(iBatch(i), element, (iBatch(i) * 10).toByte())
    }

    val shortCol = exampleFrame["short"] as DataColumn<Short?>
    shortCol.type() shouldBe typeOf<Short>().withNullability(expectedNullable)
    shortCol.forEachIndexed { i, element ->
        assertValueOrNull(iBatch(i), element, (iBatch(i) * 1000).toShort())
    }

    val intCol = exampleFrame["int"] as DataColumn<Int?>
    intCol.type() shouldBe typeOf<Int>().withNullability(expectedNullable)
    intCol.forEachIndexed { i, element ->
        assertValueOrNull(iBatch(i), element, iBatch(i) * 100000000)
    }

    val longCol = exampleFrame["longInt"] as DataColumn<Long?>
    longCol.type() shouldBe typeOf<Long>().withNullability(expectedNullable)
    longCol.forEachIndexed { i, element ->
        assertValueOrNull(iBatch(i), element, iBatch(i) * 100000000000000000L)
    }

    val unsignedByteCol = exampleFrame["unsigned_byte"] as DataColumn<Short?>
    unsignedByteCol.type() shouldBe typeOf<Short>().withNullability(expectedNullable)
    unsignedByteCol.forEachIndexed { i, element ->
        assertValueOrNull(iBatch(i), element, (iBatch(i) * 10 % (Byte.MIN_VALUE.toShort() * 2).absoluteValue).toShort())
    }

    val unsignedShortCol = exampleFrame["unsigned_short"] as DataColumn<Int?>
    unsignedShortCol.type() shouldBe typeOf<Int>().withNullability(expectedNullable)
    unsignedShortCol.forEachIndexed { i, element ->
        assertValueOrNull(iBatch(i), element, iBatch(i) * 1000 % (Short.MIN_VALUE.toInt() * 2).absoluteValue)
    }

    val unsignedIntCol = exampleFrame["unsigned_int"] as DataColumn<Long?>
    unsignedIntCol.type() shouldBe typeOf<Long>().withNullability(expectedNullable)
    unsignedIntCol.forEachIndexed { i, element ->
        assertValueOrNull(
            rowNumber = iBatch(i),
            actual = element,
            expected = iBatch(i).toLong() * 100000000 % (Int.MIN_VALUE.toLong() * 2).absoluteValue,
        )
    }

    val unsignedLongIntCol = exampleFrame["unsigned_longInt"] as DataColumn<BigInteger?>
    unsignedLongIntCol.type() shouldBe typeOf<BigInteger>().withNullability(expectedNullable)
    unsignedLongIntCol.forEachIndexed { i, element ->
        assertValueOrNull(
            rowNumber = iBatch(i),
            actual = element,
            expected = iBatch(i).toBigInteger() * 100000000000000000L.toBigInteger() %
                (Long.MIN_VALUE.toBigInteger() * 2.toBigInteger()).abs(),
        )
    }

    val floatCol = exampleFrame["float"] as DataColumn<Float?>
    floatCol.type() shouldBe typeOf<Float>().withNullability(expectedNullable)
    floatCol.forEachIndexed { i, element ->
        assertValueOrNull(iBatch(i), element, 2.0f.pow(iBatch(i).toFloat()))
    }

    val doubleCol = exampleFrame["double"] as DataColumn<Double?>
    doubleCol.type() shouldBe typeOf<Double>().withNullability(expectedNullable)
    doubleCol.forEachIndexed { i, element ->
        assertValueOrNull(iBatch(i), element, 2.0.pow(iBatch(i)))
    }

    val dateCol = exampleFrame["date32"] as DataColumn<LocalDate?>
    dateCol.type() shouldBe typeOf<LocalDate>().withNullability(expectedNullable)
    dateCol.forEachIndexed { i, element ->
        assertValueOrNull(iBatch(i), element, JavaLocalDate.ofEpochDay(iBatch(i).toLong() * 30).toKotlinLocalDate())
    }

    if (fromParquet) {
        // parquet format have only one type of date: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#date without time
        val datetimeCol = exampleFrame["date64"] as DataColumn<LocalDate?>
        datetimeCol.type() shouldBe typeOf<LocalDate>().withNullability(expectedNullable)
        datetimeCol.forEachIndexed { i, element ->
            assertValueOrNull(iBatch(i), element, JavaLocalDate.ofEpochDay(iBatch(i).toLong() * 30).toKotlinLocalDate())
        }
    } else {
        val datetimeCol = exampleFrame["date64"] as DataColumn<LocalDateTime?>
        datetimeCol.type() shouldBe typeOf<LocalDateTime>().withNullability(expectedNullable)
        datetimeCol.forEachIndexed { i, element ->
            assertValueOrNull(
                rowNumber = iBatch(i),
                actual = element,
                expected = JavaLocalDateTime.ofEpochSecond(
                    iBatch(i).toLong() * 60 * 60 * 24 * 30,
                    0,
                    ZoneOffset.UTC,
                ).toKotlinLocalDateTime(),
            )
        }
    }

    val timeSecCol = exampleFrame["time32_seconds"] as DataColumn<LocalTime?>
    timeSecCol.type() shouldBe typeOf<LocalTime>().withNullability(expectedNullable)
    timeSecCol.forEachIndexed { i, element ->
        assertValueOrNull(iBatch(i), element, JavaLocalTime.ofSecondOfDay(iBatch(i).toLong()).toKotlinLocalTime())
    }

    val timeMilliCol = exampleFrame["time32_milli"] as DataColumn<LocalTime?>
    timeMilliCol.type() shouldBe typeOf<LocalTime>().withNullability(expectedNullable)
    timeMilliCol.forEachIndexed { i, element ->
        assertValueOrNull(
            rowNumber = iBatch(i),
            actual = element,
            expected = JavaLocalTime.ofNanoOfDay(iBatch(i).toLong() * 1000_000).toKotlinLocalTime(),
        )
    }

    val timeMicroCol = exampleFrame["time64_micro"] as DataColumn<LocalTime?>
    timeMicroCol.type() shouldBe typeOf<LocalTime>().withNullability(expectedNullable)
    timeMicroCol.forEachIndexed { i, element ->
        assertValueOrNull(iBatch(i), element, JavaLocalTime.ofNanoOfDay(iBatch(i).toLong() * 1000).toKotlinLocalTime())
    }

    val timeNanoCol = exampleFrame["time64_nano"] as DataColumn<LocalTime?>
    timeNanoCol.type() shouldBe typeOf<LocalTime>().withNullability(expectedNullable)
    timeNanoCol.forEachIndexed { i, element ->
        assertValueOrNull(iBatch(i), element, JavaLocalTime.ofNanoOfDay(iBatch(i).toLong()).toKotlinLocalTime())
    }

    exampleFrame.getColumnOrNull("nulls")?.let { nullCol ->
        nullCol.type() shouldBe nothingType(hasNulls)
        assert(hasNulls)
        nullCol.values().forEach {
            assert(it == null)
        }
    }
}
