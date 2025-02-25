package org.jetbrains.kotlinx.dataframe.api

import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import kotlinx.datetime.DateTimeUnit
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.datetime.Month
import kotlinx.datetime.format.DateTimeComponents
import kotlinx.datetime.plus
import kotlinx.datetime.toJavaInstant
import kotlinx.datetime.toKotlinInstant
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.impl.api.Parsers
import org.jetbrains.kotlinx.dataframe.impl.catchSilent
import org.jetbrains.kotlinx.dataframe.type
import org.junit.Test
import java.util.Locale
import kotlin.random.Random
import kotlin.reflect.typeOf
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds
import java.time.Duration as JavaDuration
import java.time.Instant as JavaInstant

class ParseTests {

    @Test
    fun `parse chars to string`() {
        val char = columnOf('a', 'b', 'c')
        char.parse() shouldBe columnOf("a", "b", "c")
        char.tryParse() shouldBe columnOf("a", "b", "c")
        char.parse().cast<String>().parse() shouldBe char
    }

    @Test
    fun `parse chars to int`() {
        val char = columnOf('1', '2', '3')
        char.parse() shouldBe columnOf(1, 2, 3)
        char.tryParse() shouldBe columnOf(1, 2, 3)
    }

    @Test
    fun parseDate() {
        val currentLocale = Locale.getDefault()
        try {
            Locale.setDefault(Locale.forLanguageTag("en-US"))
            val date by columnOf("January 1, 2020")
            val pattern = "MMMM d, yyyy"

            val parsed = date.parse(ParserOptions(dateTimePattern = pattern)).cast<LocalDate>()

            parsed.type() shouldBe typeOf<LocalDate>()
            with(parsed[0]) {
                month shouldBe Month.JANUARY
                dayOfMonth shouldBe 1
                year shouldBe 2020
            }

            date.convertToLocalDate(pattern) shouldBe parsed
            with(date.toDataFrame()) {
                convert { date }.toLocalDate(pattern)[date] shouldBe parsed
                parse(ParserOptions(dateTimePattern = pattern))[date] shouldBe parsed
            }

            DataFrame.parser.addDateTimePattern(pattern)

            date.parse() shouldBe parsed
            date.convertToLocalDate() shouldBe parsed

            DataFrame.parser.resetToDefault()
        } finally {
            Locale.setDefault(currentLocale)
        }
    }

    @Test
    fun parseDateTime() {
        val currentLocale = Locale.getDefault()
        try {
            Locale.setDefault(Locale.forLanguageTag("en-US"))
            val dateTime by columnOf("3 Jun 2008 13:05:30")
            val pattern = "d MMM yyyy HH:mm:ss"
            val locale = Locale.forLanguageTag("en-US")

            val parsed = dateTime.parse(ParserOptions(dateTimePattern = pattern, locale = locale)).cast<LocalDateTime>()

            parsed.type() shouldBe typeOf<LocalDateTime>()
            with(parsed[0]) {
                month shouldBe Month.JUNE
                dayOfMonth shouldBe 3
                year shouldBe 2008
                hour shouldBe 13
                minute shouldBe 5
                second shouldBe 30
            }

            dateTime.convertToLocalDateTime(pattern, locale) shouldBe parsed
            with(dateTime.toDataFrame()) {
                convert { dateTime }.toLocalDateTime(pattern)[dateTime] shouldBe parsed
                parse(ParserOptions(dateTimePattern = pattern))[dateTime] shouldBe parsed
            }

            DataFrame.parser.addDateTimePattern(pattern)

            dateTime.parse(ParserOptions(locale = locale)) shouldBe parsed
            dateTime.convertToLocalDateTime(pattern, locale) shouldBe parsed

            DataFrame.parser.resetToDefault()
        } finally {
            Locale.setDefault(currentLocale)
        }
    }

    @Test
    fun parseTime() {
        val time by columnOf(" 13-05-30")
        val pattern = "HH-mm-ss"

        val parsed = time.parse(ParserOptions(dateTimePattern = pattern)).cast<LocalTime>()

        parsed.type() shouldBe typeOf<LocalTime>()
        with(parsed[0]) {
            hour shouldBe 13
            minute shouldBe 5
            second shouldBe 30
        }
        time.convertToLocalTime(pattern) shouldBe parsed
        with(time.toDataFrame()) {
            convert { time }.toLocalTime(pattern)[time] shouldBe parsed
            parse(ParserOptions(dateTimePattern = pattern))[time] shouldBe parsed
        }

        DataFrame.parser.addDateTimePattern(pattern)

        time.parse() shouldBe parsed
        time.convertToLocalTime() shouldBe parsed

        DataFrame.parser.resetToDefault()
    }

    @Test
    fun `parse date without formatter`() {
        val time by columnOf(" 2020-01-06", "2020-01-07 ")
        val df = dataFrameOf(time)
        val casted = df.convert(time).toLocalDate()
        casted[time].type() shouldBe typeOf<LocalDate>()
    }

    @Test
    fun `parse column group`() {
        val df = dataFrameOf("a", "b")("1", "2")
        df
            .group("a", "b")
            .into("c")
            .parse("c")
            .ungroup("c") shouldBe dataFrameOf("a", "b")(1, 2)
    }

    @Test
    fun `parse instant`() {
        columnOf("2022-01-23T04:29:40Z").parse().type shouldBe typeOf<Instant>()
        columnOf("2022-01-23T04:29:40+01:00").parse().type shouldBe typeOf<Instant>()

        columnOf("2022-01-23T04:29:40").parse().type shouldBe typeOf<LocalDateTime>()
    }

    @Test
    fun `can parse instants`() {
        val instantParser = Parsers[typeOf<Instant>()]!!.applyOptions(null)
        val javaInstantParser = Parsers[typeOf<JavaInstant>()]!!.applyOptions(null)

        // from the kotlinx-datetime tests, java instants treat leap seconds etc. like this
        fun parseInstantLikeJavaDoesOrNull(input: String): Instant? =
            catchSilent {
                DateTimeComponents.Formats.ISO_DATE_TIME_OFFSET.parseOrNull(input)?.apply {
                    when {
                        hour == 24 && minute == 0 && second == 0 && nanosecond == 0 -> {
                            setDate(toLocalDate().plus(1, DateTimeUnit.DAY))
                            hour = 0
                        }

                        hour == 23 && minute == 59 && second == 60 -> second = 59
                    }
                }?.toInstantUsingOffset()
            }

        fun formatTwoDigits(i: Int) = if (i < 10) "0$i" else "$i"

        for (hour in 23..25) {
            for (minute in listOf(0..5, 58..62).flatten()) {
                for (second in listOf(0..5, 58..62).flatten()) {
                    val input = "2020-03-16T$hour:${formatTwoDigits(minute)}:${formatTwoDigits(second)}Z"

                    val myParserRes = instantParser(input) as Instant?
                    val myJavaParserRes = javaInstantParser(input) as JavaInstant?
                    val instantRes = catchSilent { Instant.parse(input) }
                    val instantLikeJava = parseInstantLikeJavaDoesOrNull(input)
                    val javaInstantRes = catchSilent { JavaInstant.parse(input) }

                    // our parser has a fallback mechanism built in, like this
                    myParserRes shouldBe (instantRes ?: javaInstantRes?.toKotlinInstant())
                    myParserRes shouldBe instantLikeJava

                    myJavaParserRes shouldBe javaInstantRes

                    myParserRes?.toJavaInstant() shouldBe instantLikeJava?.toJavaInstant()
                    instantLikeJava?.toJavaInstant() shouldBe myJavaParserRes
                    myJavaParserRes shouldBe javaInstantRes
                }
            }
        }
    }

    @Test
    fun `parse duration`() {
        columnOf("1d 15m", "20h 35m 11s").parse() shouldBe
            columnOf(1.days + 15.minutes, 20.hours + 35.minutes + 11.seconds)
    }

    @Test
    fun `can parse duration isoStrings`() {
        val durationParser = Parsers[typeOf<Duration>()]!!.applyOptions(null) as (String) -> Duration?
        val javaDurationParser = Parsers[typeOf<JavaDuration>()]!!.applyOptions(null) as (String) -> JavaDuration?

        fun testSuccess(duration: Duration, vararg isoStrings: String) {
            isoStrings.first() shouldBe duration.toIsoString()
            for (isoString in isoStrings) {
                Duration.parse(isoString) shouldBe duration
                durationParser(isoString) shouldBe duration

                javaDurationParser(isoString) shouldBe catchSilent { JavaDuration.parse(isoString) }
            }
        }

        // zero
        testSuccess(Duration.ZERO, "PT0S", "P0D", "PT0H", "PT0M", "P0DT0H", "PT0H0M", "PT0H0S")

        // single unit
        testSuccess(1.days, "PT24H", "P1D", "PT1440M", "PT86400S")
        testSuccess(1.hours, "PT1H")
        testSuccess(1.minutes, "PT1M")
        testSuccess(1.seconds, "PT1S")
        testSuccess(1.milliseconds, "PT0.001S")
        testSuccess(1.microseconds, "PT0.000001S")
        testSuccess(1.nanoseconds, "PT0.000000001S", "PT0.0000000009S")
        testSuccess(0.9.nanoseconds, "PT0.000000001S")

        // rounded to zero
        testSuccess(0.1.nanoseconds, "PT0S")
        testSuccess(Duration.ZERO, "PT0S", "PT0.0000000004S")

        // several units combined
        testSuccess(1.days + 1.minutes, "PT24H1M")
        testSuccess(1.days + 1.seconds, "PT24H0M1S")
        testSuccess(1.days + 1.milliseconds, "PT24H0M0.001S")
        testSuccess(1.hours + 30.minutes, "PT1H30M")
        testSuccess(1.hours + 500.milliseconds, "PT1H0M0.500S")
        testSuccess(2.minutes + 500.milliseconds, "PT2M0.500S")
        testSuccess(90_500.milliseconds, "PT1M30.500S")

        // with sign
        testSuccess(-1.days + 15.minutes, "-PT23H45M", "PT-23H-45M", "+PT-24H+15M")
        testSuccess(-1.days - 15.minutes, "-PT24H15M", "PT-24H-15M", "-PT25H-45M")
        testSuccess(Duration.ZERO, "PT0S", "P1DT-24H", "+PT-1H+60M", "-PT1M-60S")

        // infinite
        testSuccess(
            Duration.INFINITE,
            "PT9999999999999H",
            "PT+10000000000000H",
            "-PT-9999999999999H",
            "-PT-1234567890123456789012S",
        )
        testSuccess(-Duration.INFINITE, "-PT9999999999999H", "-PT10000000000000H", "PT-1234567890123456789012S")

        fun testFailure(isoString: String) {
            catchSilent { Duration.parse(isoString) } shouldBe durationParser(isoString)
            catchSilent { JavaDuration.parse(isoString) } shouldBe javaDurationParser(isoString)
        }

        listOf(
            "",
            " ",
            "P",
            "PT",
            "P1DT",
            "P1",
            "PT1",
            "0",
            "+P",
            "+",
            "-",
            "h",
            "H",
            "something",
            "1m",
            "1d",
            "2d 11s",
            "Infinity",
            "-Infinity", // successful in kotlin, not in java
            "P+12+34D",
            "P12-34D",
            "PT1234567890-1234567890S",
            " P1D",
            "PT1S ",
            "P3W",
            "P1Y",
            "P1M",
            "P1S",
            "PT1D",
            "PT1Y",
            "PT1S2S",
            "PT1S2H",
            "P9999999999999DT-9999999999999H",
            "PT1.5H",
            "PT0.5D",
            "PT.5S",
            "PT0.25.25S",
        ).forEach(::testFailure)
    }

    @Test
    fun `can parse duration default kotlin strings`() {
        val durationParser = Parsers[typeOf<Duration>()]!!.applyOptions(null) as (String) -> Duration?

        fun testParsing(string: String, expectedDuration: Duration) {
            Duration.parse(string) shouldBe expectedDuration
            durationParser(string) shouldBe expectedDuration
        }

        fun testSuccess(duration: Duration, vararg expected: String) {
            val actual = duration.toString()
            actual shouldBe expected.first()

            if (duration.isPositive()) {
                if (' ' in actual) {
                    (-duration).toString() shouldBe "-($actual)"
                } else {
                    (-duration).toString() shouldBe "-$actual"
                }
            }

            for (string in expected) {
                testParsing(string, duration)
                if (duration.isPositive() && duration.isFinite()) {
                    testParsing("+($string)", duration)
                    testParsing("-($string)", -duration)
                    if (' ' !in string) {
                        testParsing("+$string", duration)
                        testParsing("-$string", -duration)
                    }
                }
            }
        }

        testSuccess(101.days, "101d", "2424h")
        testSuccess(45.3.days, "45d 7h 12m", "45.3d", "45d 7.2h") // 0.3d == 7.2h
        testSuccess(45.days, "45d")

        testSuccess(40.5.days, "40d 12h", "40.5d", "40d 720m")
        testSuccess(40.days + 20.minutes, "40d 0h 20m", "40d 20m", "40d 1200s")
        testSuccess(40.days + 20.seconds, "40d 0h 0m 20s", "40d 20s")
        testSuccess(40.days + 100.nanoseconds, "40d 0h 0m 0.000000100s", "40d 100ns")

        testSuccess(40.hours + 15.minutes, "1d 16h 15m", "40h 15m")
        testSuccess(40.hours, "1d 16h", "40h")

        testSuccess(12.5.hours, "12h 30m")
        testSuccess(12.hours + 15.seconds, "12h 0m 15s")
        testSuccess(12.hours + 1.nanoseconds, "12h 0m 0.000000001s")
        testSuccess(30.minutes, "30m")
        testSuccess(17.5.minutes, "17m 30s")

        testSuccess(16.5.minutes, "16m 30s")
        testSuccess(1097.1.seconds, "18m 17.1s")
        testSuccess(90.36.seconds, "1m 30.36s")
        testSuccess(50.seconds, "50s")
        testSuccess(1.3.seconds, "1.3s")
        testSuccess(1.seconds, "1s")

        testSuccess(0.5.seconds, "500ms")
        testSuccess(40.2.milliseconds, "40.2ms")
        testSuccess(4.225.milliseconds, "4.225ms")
        testSuccess(4.24501.milliseconds, "4.245010ms", "4ms 245us 10ns")
        testSuccess(1.milliseconds, "1ms")

        testSuccess(0.75.milliseconds, "750us")
        testSuccess(75.35.microseconds, "75.35us")
        testSuccess(7.25.microseconds, "7.25us")
        testSuccess(1.035.microseconds, "1.035us")
        testSuccess(1.005.microseconds, "1.005us")
        testSuccess(1800.nanoseconds, "1.8us", "1800ns", "0.0000000005h")

        testSuccess(950.5.nanoseconds, "951ns")
        testSuccess(85.23.nanoseconds, "85ns")
        testSuccess(8.235.nanoseconds, "8ns")
        testSuccess(1.nanoseconds, "1ns", "0.9ns", "0.001us", "0.0009us")
        testSuccess(1.3.nanoseconds, "1ns")
        testSuccess(0.75.nanoseconds, "1ns")
        testSuccess(0.7512.nanoseconds, "1ns")

        // equal to zero
//        testSuccess(0.023.nanoseconds, "0.023ns")
//        testSuccess(0.0034.nanoseconds, "0.0034ns")
//        testSuccess(0.0000035.nanoseconds, "0.0000035ns")

        testSuccess(Duration.ZERO, "0s", "0.4ns", "0000.0000ns")
        testSuccess(365.days * 10000, "3650000d")
        testSuccess(300.days * 100000, "30000000d")
        testSuccess(365.days * 100000, "36500000d")
        testSuccess(((Long.MAX_VALUE / 2) - 1).milliseconds, "53375995583d 15h 36m 27.902s") // max finite value

        // all infinite
//        val universeAge = Duration.days(365.25) * 13.799e9
//        val planckTime = Duration.seconds(5.4e-44)

//        testSuccess(universeAge, "5.04e+12d")
//        testSuccess(planckTime, "5.40e-44s")
//        testSuccess(Duration.nanoseconds(Double.MAX_VALUE), "2.08e+294d")
        testSuccess(Duration.INFINITE, "Infinity", "53375995583d 20h", "+Infinity")
        testSuccess(-Duration.INFINITE, "-Infinity", "-(53375995583d 20h)")

        fun testFailure(isoString: String) {
            catchSilent { Duration.parse(isoString) } shouldBe durationParser(isoString)
        }

        listOf(
            "",
            " ",
            "P",
            "PT",
            "P1DT",
            "P1",
            "PT1",
            "0",
            "+P",
            "+",
            "-",
            "h",
            "H",
            "something",
            "1234567890123456789012ns",
            "Inf",
            "-Infinity value",
            "1s ",
            " 1s",
            "1d 1m 1h",
            "1s 2s",
            "-12m 15s",
            "-12m -15s",
            "-()",
            "-(12m 30s",
            "+12m 15s",
            "+12m +15s",
            "+()",
            "+(12m 30s",
            "()",
            "(12m 30s)",
            "12.5m 11.5s",
            ".2s",
            "0.1553.39m",
            "P+12+34D",
            "P12-34D",
            "PT1234567890-1234567890S",
            " P1D",
            "PT1S ",
            "P1Y",
            "P1M",
            "P1S",
            "PT1D",
            "PT1Y",
            "PT1S2S",
            "PT1S2H",
            "P9999999999999DT-9999999999999H",
            "PT1.5H",
            "PT0.5D",
            "PT.5S",
            "PT0.25.25S",
        ).forEach(::testFailure)
    }

    @Test
    fun `Parse normal string column`() {
        val df = dataFrameOf(List(5_000) { "_$it" }).fill(100) {
            Random.nextInt().toChar().toString() + Random.nextInt().toChar()
        }

        df.parse()
    }

    /**
     * Asserts that all elements of the iterable are equal to each other
     */
    private fun <T> Iterable<T>.shouldAllBeEqual(): Iterable<T> {
        this should {
            it.reduce { a, b ->
                a shouldBe b
                b
            }
        }
        return this
    }
}
