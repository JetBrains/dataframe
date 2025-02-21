package org.jetbrains.kotlinx.dataframe.testSets.person

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.doubles.ToleranceMatcher
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.AnyRow
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.DataRow
import org.jetbrains.kotlinx.dataframe.RowExpression
import org.jetbrains.kotlinx.dataframe.alsoDebug
import org.jetbrains.kotlinx.dataframe.annotations.ColumnName
import org.jetbrains.kotlinx.dataframe.annotations.DataSchema
import org.jetbrains.kotlinx.dataframe.api.ExcessiveColumns
import org.jetbrains.kotlinx.dataframe.api.GroupBy
import org.jetbrains.kotlinx.dataframe.api.Infer
import org.jetbrains.kotlinx.dataframe.api.Merge
import org.jetbrains.kotlinx.dataframe.api.ParserOptions
import org.jetbrains.kotlinx.dataframe.api.add
import org.jetbrains.kotlinx.dataframe.api.addAll
import org.jetbrains.kotlinx.dataframe.api.addId
import org.jetbrains.kotlinx.dataframe.api.aggregate
import org.jetbrains.kotlinx.dataframe.api.all
import org.jetbrains.kotlinx.dataframe.api.allNulls
import org.jetbrains.kotlinx.dataframe.api.append
import org.jetbrains.kotlinx.dataframe.api.asColumnGroup
import org.jetbrains.kotlinx.dataframe.api.asDataFrame
import org.jetbrains.kotlinx.dataframe.api.asGroupBy
import org.jetbrains.kotlinx.dataframe.api.asIterable
import org.jetbrains.kotlinx.dataframe.api.at
import org.jetbrains.kotlinx.dataframe.api.between
import org.jetbrains.kotlinx.dataframe.api.by
import org.jetbrains.kotlinx.dataframe.api.cast
import org.jetbrains.kotlinx.dataframe.api.chunked
import org.jetbrains.kotlinx.dataframe.api.colsOf
import org.jetbrains.kotlinx.dataframe.api.column
import org.jetbrains.kotlinx.dataframe.api.columnGroup
import org.jetbrains.kotlinx.dataframe.api.columnOf
import org.jetbrains.kotlinx.dataframe.api.concat
import org.jetbrains.kotlinx.dataframe.api.convert
import org.jetbrains.kotlinx.dataframe.api.convertTo
import org.jetbrains.kotlinx.dataframe.api.corr
import org.jetbrains.kotlinx.dataframe.api.count
import org.jetbrains.kotlinx.dataframe.api.countDistinct
import org.jetbrains.kotlinx.dataframe.api.dataFrameOf
import org.jetbrains.kotlinx.dataframe.api.default
import org.jetbrains.kotlinx.dataframe.api.describe
import org.jetbrains.kotlinx.dataframe.api.digitize
import org.jetbrains.kotlinx.dataframe.api.distinct
import org.jetbrains.kotlinx.dataframe.api.distinctBy
import org.jetbrains.kotlinx.dataframe.api.div
import org.jetbrains.kotlinx.dataframe.api.drop
import org.jetbrains.kotlinx.dataframe.api.dropLast
import org.jetbrains.kotlinx.dataframe.api.dropNA
import org.jetbrains.kotlinx.dataframe.api.dropNulls
import org.jetbrains.kotlinx.dataframe.api.dropWhile
import org.jetbrains.kotlinx.dataframe.api.explode
import org.jetbrains.kotlinx.dataframe.api.expr
import org.jetbrains.kotlinx.dataframe.api.fill
import org.jetbrains.kotlinx.dataframe.api.fillNulls
import org.jetbrains.kotlinx.dataframe.api.filter
import org.jetbrains.kotlinx.dataframe.api.first
import org.jetbrains.kotlinx.dataframe.api.forEach
import org.jetbrains.kotlinx.dataframe.api.forEachIndexed
import org.jetbrains.kotlinx.dataframe.api.frameColumn
import org.jetbrains.kotlinx.dataframe.api.gather
import org.jetbrains.kotlinx.dataframe.api.getColumn
import org.jetbrains.kotlinx.dataframe.api.getColumnGroup
import org.jetbrains.kotlinx.dataframe.api.getColumns
import org.jetbrains.kotlinx.dataframe.api.getFrameColumn
import org.jetbrains.kotlinx.dataframe.api.getValue
import org.jetbrains.kotlinx.dataframe.api.group
import org.jetbrains.kotlinx.dataframe.api.groupBy
import org.jetbrains.kotlinx.dataframe.api.implode
import org.jetbrains.kotlinx.dataframe.api.indices
import org.jetbrains.kotlinx.dataframe.api.inplace
import org.jetbrains.kotlinx.dataframe.api.into
import org.jetbrains.kotlinx.dataframe.api.intoColumns
import org.jetbrains.kotlinx.dataframe.api.intoList
import org.jetbrains.kotlinx.dataframe.api.intoRows
import org.jetbrains.kotlinx.dataframe.api.isColumnGroup
import org.jetbrains.kotlinx.dataframe.api.isEmpty
import org.jetbrains.kotlinx.dataframe.api.isFrameColumn
import org.jetbrains.kotlinx.dataframe.api.isNA
import org.jetbrains.kotlinx.dataframe.api.isNumber
import org.jetbrains.kotlinx.dataframe.api.keysInto
import org.jetbrains.kotlinx.dataframe.api.last
import org.jetbrains.kotlinx.dataframe.api.leftJoin
import org.jetbrains.kotlinx.dataframe.api.lowercase
import org.jetbrains.kotlinx.dataframe.api.map
import org.jetbrains.kotlinx.dataframe.api.mapToFrame
import org.jetbrains.kotlinx.dataframe.api.match
import org.jetbrains.kotlinx.dataframe.api.matches
import org.jetbrains.kotlinx.dataframe.api.max
import org.jetbrains.kotlinx.dataframe.api.maxBy
import org.jetbrains.kotlinx.dataframe.api.mean
import org.jetbrains.kotlinx.dataframe.api.meanFor
import org.jetbrains.kotlinx.dataframe.api.meanOf
import org.jetbrains.kotlinx.dataframe.api.median
import org.jetbrains.kotlinx.dataframe.api.merge
import org.jetbrains.kotlinx.dataframe.api.min
import org.jetbrains.kotlinx.dataframe.api.minBy
import org.jetbrains.kotlinx.dataframe.api.minOf
import org.jetbrains.kotlinx.dataframe.api.minus
import org.jetbrains.kotlinx.dataframe.api.move
import org.jetbrains.kotlinx.dataframe.api.moveTo
import org.jetbrains.kotlinx.dataframe.api.moveToEnd
import org.jetbrains.kotlinx.dataframe.api.moveToStart
import org.jetbrains.kotlinx.dataframe.api.name
import org.jetbrains.kotlinx.dataframe.api.named
import org.jetbrains.kotlinx.dataframe.api.notNull
import org.jetbrains.kotlinx.dataframe.api.nullable
import org.jetbrains.kotlinx.dataframe.api.parse
import org.jetbrains.kotlinx.dataframe.api.pathOf
import org.jetbrains.kotlinx.dataframe.api.pivot
import org.jetbrains.kotlinx.dataframe.api.print
import org.jetbrains.kotlinx.dataframe.api.remove
import org.jetbrains.kotlinx.dataframe.api.rename
import org.jetbrains.kotlinx.dataframe.api.reorderColumnsByName
import org.jetbrains.kotlinx.dataframe.api.replace
import org.jetbrains.kotlinx.dataframe.api.rows
import org.jetbrains.kotlinx.dataframe.api.select
import org.jetbrains.kotlinx.dataframe.api.single
import org.jetbrains.kotlinx.dataframe.api.sortBy
import org.jetbrains.kotlinx.dataframe.api.sortByCount
import org.jetbrains.kotlinx.dataframe.api.sortByDesc
import org.jetbrains.kotlinx.dataframe.api.sortByKey
import org.jetbrains.kotlinx.dataframe.api.sortWith
import org.jetbrains.kotlinx.dataframe.api.split
import org.jetbrains.kotlinx.dataframe.api.sum
import org.jetbrains.kotlinx.dataframe.api.sumOf
import org.jetbrains.kotlinx.dataframe.api.take
import org.jetbrains.kotlinx.dataframe.api.takeLast
import org.jetbrains.kotlinx.dataframe.api.takeWhile
import org.jetbrains.kotlinx.dataframe.api.times
import org.jetbrains.kotlinx.dataframe.api.to
import org.jetbrains.kotlinx.dataframe.api.toColumn
import org.jetbrains.kotlinx.dataframe.api.toColumnAccessor
import org.jetbrains.kotlinx.dataframe.api.toColumnOf
import org.jetbrains.kotlinx.dataframe.api.toDataFrame
import org.jetbrains.kotlinx.dataframe.api.toDouble
import org.jetbrains.kotlinx.dataframe.api.toEnd
import org.jetbrains.kotlinx.dataframe.api.toInt
import org.jetbrains.kotlinx.dataframe.api.toList
import org.jetbrains.kotlinx.dataframe.api.toListOf
import org.jetbrains.kotlinx.dataframe.api.toMap
import org.jetbrains.kotlinx.dataframe.api.toStr
import org.jetbrains.kotlinx.dataframe.api.toValueColumn
import org.jetbrains.kotlinx.dataframe.api.transpose
import org.jetbrains.kotlinx.dataframe.api.under
import org.jetbrains.kotlinx.dataframe.api.ungroup
import org.jetbrains.kotlinx.dataframe.api.update
import org.jetbrains.kotlinx.dataframe.api.value
import org.jetbrains.kotlinx.dataframe.api.values
import org.jetbrains.kotlinx.dataframe.api.valuesAreComparable
import org.jetbrains.kotlinx.dataframe.api.valuesNotNull
import org.jetbrains.kotlinx.dataframe.api.where
import org.jetbrains.kotlinx.dataframe.api.with
import org.jetbrains.kotlinx.dataframe.api.withNull
import org.jetbrains.kotlinx.dataframe.api.withValues
import org.jetbrains.kotlinx.dataframe.api.withZero
import org.jetbrains.kotlinx.dataframe.api.xs
import org.jetbrains.kotlinx.dataframe.columns.ColumnKind
import org.jetbrains.kotlinx.dataframe.columns.UnresolvedColumnsPolicy
import org.jetbrains.kotlinx.dataframe.exceptions.ExcessiveColumnsException
import org.jetbrains.kotlinx.dataframe.exceptions.TypeConversionException
import org.jetbrains.kotlinx.dataframe.get
import org.jetbrains.kotlinx.dataframe.hasNulls
import org.jetbrains.kotlinx.dataframe.impl.DataFrameImpl
import org.jetbrains.kotlinx.dataframe.impl.DataFrameSize
import org.jetbrains.kotlinx.dataframe.impl.api.convertToImpl
import org.jetbrains.kotlinx.dataframe.impl.between
import org.jetbrains.kotlinx.dataframe.impl.columns.isMissingColumn
import org.jetbrains.kotlinx.dataframe.impl.emptyPath
import org.jetbrains.kotlinx.dataframe.impl.getColumnsImpl
import org.jetbrains.kotlinx.dataframe.impl.nothingType
import org.jetbrains.kotlinx.dataframe.impl.trackColumnAccess
import org.jetbrains.kotlinx.dataframe.index
import org.jetbrains.kotlinx.dataframe.io.renderValueForStdout
import org.jetbrains.kotlinx.dataframe.kind
import org.jetbrains.kotlinx.dataframe.math.mean
import org.jetbrains.kotlinx.dataframe.name
import org.jetbrains.kotlinx.dataframe.ncol
import org.jetbrains.kotlinx.dataframe.nrow
import org.jetbrains.kotlinx.dataframe.size
import org.jetbrains.kotlinx.dataframe.type
import org.jetbrains.kotlinx.dataframe.typeClass
import org.junit.Test
import java.math.BigDecimal
import java.time.LocalDate
import kotlin.reflect.KType
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.typeOf

@Suppress("ktlint:standard:argument-list-wrapping")
class DataFrameTests : BaseTest() {

    @Test
    fun `create with columns`() {
        dataFrameOf("name", "age", "city", "weight")(df.columns()) shouldBe df

        dataFrameOf("name", "age", "city", "weight")(
            typed.name named "bla",
            typed.age named "",
            typed.city.rename("qq"),
            typed.weight.named("asda"),
        ) shouldBe df

        val c1 = typed.name.toList().toValueColumn()
        val c2 = typed.age.toList().toValueColumn()
        val c3 = typed.city.toList().toValueColumn()
        val c4 = typed.weight.toList().toValueColumn()

        dataFrameOf("name", "age", "city", "weight")(c1, c2, c3, c4) shouldBe df
    }

    @Test
    fun `guess column type for type without classifier`() {
        val df = dataFrameOf("a", "b")({ 1 }, 2)
        df["a"].type() shouldBe typeOf<Function<*>>()
        (df["a"][0] as () -> Int).invoke() shouldBe 1
    }

    @Test
    fun `create with columnOf`() {
        val col = columnOf("Alice", "Bob")
        val d = dataFrameOf("name")(col)
        d.nrow shouldBe 2
        d.columnNames() shouldBe listOf("name")
    }

    @Test
    fun `create with unnamed columns`() {
        val a = columnOf("Alice", "Bob")
        val b = columnOf(1, 2)
        val d = dataFrameOf(a, b)
        d.nrow shouldBe 2
        d.ncol shouldBe 2
        d.columnNames() shouldBe listOf("untitled", "untitled1")
        d["untitled"] shouldBe d.getColumn(0)
    }

    @Test(expected = IllegalArgumentException::class)
    fun `create with duplicate columns`() {
        val a = columnOf("Alice", "Bob") named "col"
        val b = columnOf(1, 2) named "col"
        val d = dataFrameOf(a, b)
    }

    @Test
    fun `create column reference`() {
        val name by column<String>()
        val col = name.withValues("Alice", "Bob")
        val df = col.toDataFrame()
        df.nrow shouldBe 2
        df.columnNames() shouldBe listOf("name")
    }

    @Test
    fun `add values to column reference`() {
        val name by column<String>()
        val values = listOf("Alice", "Bob")
        val col1 = name.withValues(values)
        val col2 = values.toColumn(name)
        col1 shouldBe col2
    }

    @Test
    fun `create from map`() {
        val data = mapOf("name" to listOf("Alice", "Bob"), "age" to listOf(15, null))
        val df = data.toDataFrame()
        df.ncol shouldBe 2
        df.nrow shouldBe 2
        df.columnNames() shouldBe listOf("name", "age")
        df["name"].type() shouldBe typeOf<String>()
        df["age"].type() shouldBe typeOf<Int?>()
    }

    @Test
    fun `toMap`() {
        val map = df.toMap()
        map.size shouldBe 4
        map.forEach {
            it.value.size shouldBe df.nrow
        }
    }

    @Test
    fun `size`() {
        df.size() shouldBe DataFrameSize(df.ncol, df.nrow)
    }

    @Test
    fun `slicing`() {
        val sliced = typed[1..2]
        sliced.nrow shouldBe 2
        sliced[0].name shouldBe typed[1].name
    }

    @Test
    fun `access tracking`() {
        trackColumnAccess {
            typed[2].age
        } shouldBe listOf("age")
    }

    @Test
    fun `indexing`() {
        val expected = 45
        val i = 1

        fun Int.check() = this shouldBe expected

        typed[i].age.check()
        typed.age[i].check()

        df[i][age].check()
        df[age][i].check()

        df[i][Person::age].check()
        df[Person::age][i].check()

        df[i].getValue<Int>("age").check()
        (df[i]["age"] as Int).check()

        df["age"].cast<Int>()[i].check()
        (df["age"][i] as Int).check()
    }

    @Test
    fun `null indexing`() {
        val i = 5

        fun String?.check() = this shouldBe null

        typed[i].city.check()
        typed.city[i].check()

        df[i][city].check()
        df[city][i].check()

        df[i][Person::city].check()
        df[Person::city][i].check()

        df[i].getValue<String?>("city").check()
        (df[i]["city"] as String?).check()

        df["city"].cast<String?>()[i].check()
        (df["city"][i] as String?).check()
    }

    @Test
    fun `incorrect column nullability`() {
        // non-nullable column definition is incorrect here, because actual dataframe has nulls in this column
        val col = column<Int>().named("weight")

        shouldThrow<NullPointerException> {
            println(df[2][col])
        }
    }

    @Test
    fun `chunked`() {
        val res = df.chunked(2)
        res.size() shouldBe 4
        res.toList().dropLast(1).forEach {
            it.nrow shouldBe 2
        }
    }

    @Test
    fun `update`() {
        fun AnyFrame.check() {
            getColumn(1).name() shouldBe "age"
            ncol shouldBe typed.ncol
            this["age"].toList() shouldBe typed.rows().map { it.age * 2 }
        }

        typed.update { age }.with { it * 2 }.check()
        typed.update { age }.with { it * 2 }.check()
        typed.update(typed.age) { it * 2 }.check()

        df.update { age }.with { it * 2 }.check()
        df.update(age) { it * 2 }.check()
        df.update(age) { it * 2 }.check()

        df.update(Person::age) { it * 2 }.check()

        df.update("age") { "age"<Int>() * 2 }.check()
    }

    @Test
    fun `conditional update`() {
        fun AnyFrame.check() {
            getColumn(1).name() shouldBe "age"
            ncol shouldBe typed.ncol
            this["age"].toList() shouldBe typed.rows().map { if (it.age > 25) null else it.age }
        }

        typed.update { age }.where { it > 25 }.withNull().check()
        typed.update { age }.where { it > 25 }.withNull().check()
        typed.update(typed.age).where { it > 25 }.withNull().check()

        df.update { age }.where { it > 25 }.withNull().check()
        df.update(age).where { it > 25 }.withNull().check()
        df.update(age).where { it > 25 }.withNull().check()

        df.update(Person::age).where { it > 25 }.withNull().check()

        df.update("age").where { it as Int > 25 }.withNull().check()
        df.update("age").where { it as Int > 25 }.withNull().check()
    }

    @Test
    fun `update cells by index`() {
        val res = typed.update { age }.at(2, 4).with { 100 }
        val expected = typed.rows().map { if (it.index == 2 || it.index == 4) 100 else it.age }
        res.age.toList() shouldBe expected
    }

    @Test
    fun `update cells by index range`() {
        val res = typed.update { age }.at(2..4).with { 100 }
        val expected = typed.rows().map { if (it.index in 2..4) 100 else it.age }
        res.age.toList() shouldBe expected
    }

    @Test
    fun `null to zero`() {
        val expected = typed.weight.toList().map { it ?: 0 }

        fun AnyFrame.check() {
            this["weight"].toList() shouldBe expected
        }

        typed.fillNulls { weight }.with { 0 }.check()
        typed.fillNulls(typed.weight).with { 0 }.check()

        df.fillNulls { weight }.with { 0 }.check()
        df.fillNulls(weight).with { 0 }.check()

        df.fillNulls("weight").with { 0 }.check()

        typed.fillNulls { weight }.withZero().check()
        typed.fillNulls { weight }.withZero().check()
        typed.fillNulls(typed.weight).withZero().check()

        df.fillNulls { weight }.withZero().check()
        df.fillNulls { weight }.withZero().check()
    }

    @Test
    fun `resetToNull`() {
        val updated = typed.update { all() }.withNull()

        updated.columns().forEach {
            it.forEach { it shouldBe null }
        }
    }

    @Test
    fun `sort`() {
        val expected = listOf(null, "London", "Dubai", "Tokyo", "Milan", "Moscow", "Moscow")

        fun AnyFrame.check() = this[city].toList() shouldBe expected

        typed.sortBy { name and age.desc() }.check()
        typed.sortBy { it.name and it.age.desc() }.check()

        df.sortBy { name and age.desc() }.check()

        df.sortBy { Person::name and Person::age.desc() }.check()

        df.sortBy { "name"<String>() and "age".desc() }.check()
    }

    @Test
    fun `sort nulls first`() {
        val expected = typed.city.toList().sortedBy { it }

        fun AnyFrame.check() = this[city].toList() shouldBe expected

        typed.sortBy { city }.check()
        df.sortBy { city }.check()
        df.sortBy { col(Person::city) }.check()
        df.sortBy { get("city") }.check()
    }

    @Test
    fun `sort nulls last`() {
        val expected = typed.city.toList().filterNotNull().sortedBy { it } + listOf(null)

        fun AnyFrame.check() = this[city].toList() shouldBe expected

        typed.sortBy { city.nullsLast() }.check()
        df.sortBy { city.nullsLast() }.check()
        df.sortBy { Person::city.nullsLast() }.check()
        df.sortBy { "city".nullsLast() }.check()
    }

    @Test
    fun `equals`() {
        typed shouldBe typed.update { age }.with { age }
    }

    @Test
    fun `get group by single key`() {
        typed.groupBy { name }.xs("Charlie").concat() shouldBe typed.filter { name == "Charlie" }.remove { name }
    }

    @Test
    fun `get group by complex key`() {
        typed.groupBy { city and name }.xs("Tokyo", "Bob").concat() shouldBe
            typed.filter { name == "Bob" && city == "Tokyo" }
                .remove { name and city }
    }

    @Test
    fun `get group by partial key`() {
        typed.groupBy { city and name }.xs("Tokyo").toDataFrame() shouldBe
            typed.filter { city == "Tokyo" }
                .remove { city }
                .groupBy { name }.toDataFrame()
    }

    @Test
    fun `group and sort`() {
        val expected = typed.sortBy { name.desc() and age }
        val actual = typed.groupBy { name }.sortBy { name.desc() and age }.concat()
        actual shouldBe expected
    }

    @Test
    fun `filter`() {
        val expected = listOf("Bob", "Bob", "Charlie")

        fun AnyFrame.check() = this[name].toList() shouldBe expected

        val limit = 20

        typed.filter { it.age > limit && it.weight != null }.check()
        typed.filter { age > limit && it.weight != null }.check()

        df.filter { it[Person::age] > limit && it[Person::weight] != null }.check()

        df.filter { age > limit && weight() != null }.check()
        df.filter { it[age] > limit && this[weight] != null }.check()

        df.filter { "age"<Int>() > limit && "weight"<Int?>() != null }.check()
    }

    @Test
    fun `drop nulls 1`() {
        fun AnyFrame.check() = rows().forEach { get("weight") shouldNotBe null }

        typed.dropNulls(typed.weight).check()
        typed.dropNulls { weight }.check()
        typed.dropNulls { it.weight }.check()

        df.dropNulls(weight).check()
        df.dropNulls { weight }.check()

        df.dropNulls("weight").check()
    }

    @Test
    fun `drop where all null`() {
        val filtered = typed.update { weight }.where { name == "Alice" }.withNull()
        val expected = typed.nrow - 1

        fun AnyFrame.check() = nrow shouldBe expected

        filtered.dropNulls(typed.weight.toColumnAccessor(), typed.city.toColumnAccessor(), whereAllNull = true).check()
        filtered.dropNulls(whereAllNull = true) { weight and city }.check()
        filtered.dropNulls(whereAllNull = true) { it.weight and it.city }.check()

        filtered.dropNulls(Person::weight, Person::city, whereAllNull = true).check()

        filtered.dropNulls(weight, city, whereAllNull = true).check()
        filtered.dropNulls(whereAllNull = true) { weight and city }.check()

        filtered.dropNulls("weight", "city", whereAllNull = true).check()
    }

    @Test
    fun `drop where any null`() {
        val filtered = typed.update { weight }.where { name == "Alice" }.withNull()
        val expected = filtered.count { weight != null && city != null }

        fun AnyFrame.check() = nrow shouldBe expected

        filtered.dropNulls(typed.weight.toColumnAccessor(), typed.city.toColumnAccessor()).check()
        filtered.dropNulls { weight and city }.check()
        filtered.dropNulls { it.weight and it.city }.check()

        filtered.dropNulls(Person::weight, Person::city).check()

        filtered.dropNulls(weight, city).check()
        filtered.dropNulls { weight and city }.check()

        filtered.dropNulls("weight", "city").check()

        filtered.dropNulls().check()

        filtered.select { weight and city }.dropNulls().check()
    }

    @Test
    fun `select one `() {
        val expected = listOf(typed.age)

        fun AnyFrame.check() = columns() shouldBe expected

        typed.select { age }.check()
        typed.select { it.age }.check()
        typed.select(typed.age).check()

        df.select(Person::age).check()

        df.select { age }.check()
        df.select(age).check()

        df.select("age").check()

        df.select { it["age"] }.check()
        df.select { get("age") }.check()
        df.select { this["age"] }.check()
    }

    @Test
    fun `select if`() {
        val expected = listOf(typed.name, typed.city)

        fun AnyFrame.check() = columns() shouldBe expected

        typed.select { cols { it.name().length == 4 } }.check()
        df.select { cols { it.name().length == 4 } }.check()
    }

    @Test
    fun `select two`() {
        val expected = listOf(typed.age, typed.city)

        fun AnyFrame.check() = columns() shouldBe expected

        typed.select { age and city }.check()
        typed.select { it.age and it.city }.check()
        typed.select(typed.age, typed.city).check()

        typed.select(Person::age, Person::city).check()

        df.select { age and city }.check()
        df.select(age, city).check()
        df[age, city].check()

        df.select { "age" and "city" }.check()
        df.select("age", "city").check()
        df["age", "city"].check()
    }

    @Test
    fun `select by type`() {
        val selected = typed.select { colsOf<String?>() }
        selected shouldBe typed.select { name and city }
    }

    @Test
    fun `select by type not nullable`() {
        val selected = typed.select { colsOf<String> { !it.hasNulls() } }
        selected shouldBe typed.select { name }
    }

    @Test
    fun `move one column`() {
        val moved = typed.moveTo(1) { city }
        val expected = typed.select { cols(name, city, age, weight) }
        moved shouldBe expected
    }

    @Test
    fun `move several columns`() {
        val moved = typed.moveTo(2) { name and city }
        val expected = typed.select { cols(age, weight, name, city) }
        moved shouldBe expected
    }

    @Test
    fun `move several columns to left`() {
        val moved = typed.moveToStart { weight and age }
        val expected = typed.select { cols(weight, age, name, city) }
        moved shouldBe expected
    }

    @Test
    fun `move several columns to right`() {
        val moved = typed.moveToEnd { weight and name }
        val expected = typed.select { cols(age, city, weight, name) }
        moved shouldBe expected
    }

    @Test
    fun `select with rename 2`() {
        val res = typed.select { name named "Name" }
        res.columnNames() shouldBe listOf("Name")
        df.select { name named "Name" } shouldBe res
        df.select { it["name"] named "Name" } shouldBe res
    }

    @Test
    fun `select with map and rename`() {
        val res = typed.select { name.map { it.lowercase() } named "Name" }
        res.columnNames() shouldBe listOf("Name")
        res["Name"].values() shouldBe typed.name.values().map { it.lowercase() }
        df.select { name.map { it.lowercase() } named "Name" } shouldBe res
        df.select { it[Person::name].map { it.lowercase() } named "Name" } shouldBe res
        df.select { "name"<String>().map { it.lowercase() } named "Name" } shouldBe res
    }

    @Test
    fun `get column with map`() {
        val converted = name.map { it.lowercase() }
        val res = df[converted]
        res.values() shouldBe typed.name.values().map { it.lowercase() }
    }

    @Test
    fun `get column by accessor`() {
        val res = df[0..1][name]
        res.size() shouldBe 2
    }

    @Test
    fun `groupBy`() {
        fun AnyFrame.check() {
            nrow shouldBe 3
            this["name"].toList() shouldBe listOf("Alice", "Bob", "Charlie")
            this["n"].toList() shouldBe listOf(2, 2, 3)
            this["old count"].toList() shouldBe listOf(0, 2, 2)
            this["median age"].toList() shouldBe listOf(17, 37, 30)
            this["min age"].toList() shouldBe listOf(15, 30, 20)
            this["oldest origin"].toList() shouldBe listOf(null, "Dubai", "Milan")
            this["youngest origin"].toList() shouldBe listOf("London", "Tokyo", "Moscow")
            this["all with weights"].toList() shouldBe listOf(true, true, false)
            val cities = getColumnGroup("city")
            cities["from London"].toList() shouldBe listOf(1, 0, 0)
            cities["from Dubai"].toList() shouldBe listOf(0, 1, 0)
            cities["from Moscow"].toList() shouldBe listOf(0, 0, 2)
            cities["from Milan"].toList() shouldBe listOf(0, 0, 1)
            cities["from Tokyo"].toList() shouldBe listOf(0, 1, 0)
            cities["from null"].toList() shouldBe listOf(1, 0, 0)
            this["ages"].toList() shouldBe listOf(listOf(15, 20), listOf(45, 30), listOf(20, 40, 30))
        }

        typed.groupBy { name }.aggregate {
            count() into "n"
            count { age > 25 } into "old count"
            median { age } into "median age"
            min { age } into "min age"
            all { weight != null } into "all with weights"
            maxBy { age }.city into "oldest origin"
            sortBy { age }.first().city into "youngest origin"
            pivot { city.map { "from $it" } }.count()
            age.toList() into "ages"
        }.check()

        typed.groupBy { it.name }.aggregate {
            it.nrow into "n"
            it.count { it.age > 25 } into "old count"
            it.median { it.age } into "median age"
            it.min { it.age } into "min age"
            it.all { it.weight != null } into "all with weights"
            it.maxBy { it.age }.city into "oldest origin"
            it.sortBy { it.age }.first().city into "youngest origin"
            it.pivot { it.city.map { "from $it" } }.count()
            it.age.toList() into "ages"
        }.check()

        df.groupBy(name).aggregate {
            count() into "n"
            count { age > 25 } into "old count"
            median(age) into "median age"
            min(age) into "min age"
            all { weight() != null } into "all with weights"
            maxBy(age)[city] into "oldest origin"
            sortBy(age).first()[city] into "youngest origin"
            pivot(city.map { "from $it" }).count()
            it[age].toList() into "ages"
        }.check()

        df.groupBy(Person::name).aggregate {
            count() into "n"
            count { it[Person::age] > 25 } into "old count"
            median(Person::age) into "median age"
            min(Person::age) into "min age"
            all { it[Person::weight] != null } into "all with weights"
            maxBy(Person::age)[Person::city] into "oldest origin"
            sortBy(Person::age).first()[Person::city] into "youngest origin"
            pivot { it[Person::city].map { "from $it" } }.count()
            it[Person::age].toList() into "ages"
        }.check()

        df.groupBy("name").aggregate {
            count() into "n"
            count { "age"<Int>() > 25 } into "old count"
            median { "age"<Int>() } into "median age"
            min { "age"<Int>() } into "min age"
            all { it["weight"] != null } into "all with weights"
            maxBy { "age"<Int>() }["city"] into "oldest origin"
            sortBy("age").first()["city"] into "youngest origin"
            pivot { it["city"].map { "from $it" } }.count()
            it["age"].toList() into "ages"
        }.check()
    }

    @Test
    fun `groupBy meanOf`() {
        typed.groupBy { name }.meanOf { age * 2 } shouldBe typed
            .groupBy { name }.aggregate { mean { age } * 2 into "mean" }
    }

    @Test
    fun `min`() {
        val expected = 15

        fun Any?.check() = this shouldBe expected

        typed.minOf { age }.check()
        typed.min { it.age }.check()
        typed.age.min().check()

        df.min { age }.check()
        df.min(age).check()
        df[age].min().check()

        df.min { "age"<Int>() }.check()
        df.min("age").check()
        df["age"].cast<Int>().min().check()
    }

    @Test
    fun `nullable max`() {
        val expected = 90

        fun Int?.check() = this shouldBe expected

        typed.max { weight }.check()
        typed.max { it.weight }.check()
        typed.weight.max().check()

        df.max { weight }.check()
        df.max(weight).check()
        df[weight].max().check()

        df.max { "weight"<Int?>() }.check()
        df["weight"].cast<Int?>().max().check()
        (df.max("weight") as Int?).check()
    }

    @Test
    fun `nullable minBy`() {
        val expected = "Alice"

        fun AnyRow?.check() = this!![name] shouldBe expected

        typed.dropNulls { weight }.minBy { weight }.check()
        typed.dropNulls { it.weight }.minBy { it.weight }.check()
        typed.minBy { weight }.check()

        df.dropNulls(weight).minBy(weight).check()
        df.minBy(weight).check()

        df.dropNulls("weight").minBy { "weight"<Int?>() }.check()
        df.dropNulls("weight").minBy("weight").check()
        df.minBy("weight").check()
    }

    @Test
    fun `maxBy`() {
        val expected = "Bob"

        fun AnyRow?.check() = this!![name] shouldBe expected

        typed.maxBy { age }.check()
        typed.maxBy { it.age }.check()
        typed.maxBy(typed.age).check()

        df.maxBy { age() }.check()
        df.maxBy(age).check()

        df.maxBy { "age"<Int>() }.check()
        df.maxBy("age").check()
    }

    @Test
    fun `add one column`() {
        val now = 2020
        val expected = typed.rows().map { now - it.age }

        fun AnyFrame.check() = this["year"].toList() shouldBe expected

        typed.add("year") { now - age }.check()
        typed.add("year") { now - it.age }.check()

        df.add("year") { now - age }.check()

        df.add("year") { now - "age"<Int>() }.check()
    }

    @Test
    fun `add several columns`() {
        val now = 2020
        val expected = typed.rows().map { now - it.age }
        val g by columnGroup()

        val df = typed.add {
            "a" from { now - age }
            "b" from now - age
            now - age into "c"
            "d" {
                "f" from { now - age }
            }
            group {
                g from {
                    add(age.map { now - it }.named("h"))
                }
            } into "e"
        }.remove { allBefore("a") }

        df.columnNames() shouldBe listOf("a", "b", "c", "d", "e")
        df["d"].kind() shouldBe ColumnKind.Group
        df["e"].kind() shouldBe ColumnKind.Group
        df.getColumnGroup("d").columnNames() shouldBe listOf("f")
        df.getColumnGroup("e").getColumnGroup("g").columnNames() shouldBe listOf("h")
        val cols = df.getColumns { colsAtAnyDepth { !it.isColumnGroup() } }
        cols.size shouldBe 5
        cols.forEach {
            it.toList() shouldBe expected
        }
    }

    @Test
    fun `add several columns with type inference`() {
        val f: Any = 123
        val df = typed.add {
            expr(infer = Infer.Type) { f } into "f"
        }
        df["f"].type() shouldBe typeOf<Int>()
    }

    @Test
    fun `remove one column`() {
        val expected = listOf("name", "city", "weight")

        fun check(body: () -> AnyFrame) = body().columnNames() shouldBe expected

        check { typed.remove { age } }
        check { typed.remove { it.age } }
        check { df.remove(age) }
        check { df.remove("age") }
    }

    @Test
    fun `remove two columns`() {
        val expected = listOf("name", "city")

        fun check(body: () -> AnyFrame) = body().columnNames() shouldBe expected

        check { typed.remove { age }.remove { weight } }
        check { typed.remove { it.age }.remove { it.weight } }
        check { typed.remove { age and weight } }
        check { typed.remove { it.age and it.weight } }
        check { df.remove(age, weight) }

        check { df.remove { "age" and "weight" } }
        check { df.remove { "age"() }.remove { "weight"() } }
        check { df.remove("age", "weight") }
    }

    @Test
    fun `merge similar dataframes`() {
        val res = typed.concat(typed, typed)
        res.name.size() shouldBe 3 * typed.nrow
        res.rows().forEach { it.values() shouldBe typed[it.index % typed.nrow].values() }
    }

    @Test
    fun `merge different dataframes`() {
        val height by column<Int>()
        val heightOrNull = height.nullable()

        @Suppress("ktlint:standard:argument-list-wrapping")
        val other = dataFrameOf(name, height)(
            "Bill", 135,
            "Charlie", 160,
        ).cast<Unit>()

        val res = typed.concat(other)
        res.nrow shouldBe typed.nrow + other.nrow
        res.take(typed.nrow).rows().forEach { it[heightOrNull] == null }
        val q = res.takeLast(other.nrow)
        q.rows().forEach { it[name] shouldBe other[it.index][name] }
        q.rows().forEach { it[heightOrNull] shouldBe other[it.index][height] }
    }

    @Test
    fun `row to frame`() {
        typed[1].toDataFrame().name.size() shouldBe 1
    }

    @Test
    fun `compare comparable`() {
        val new = df.add("date") { LocalDate.now().minusDays(index.toLong()) }
        val date by column<LocalDate>()
        new.filter { date >= LocalDate.now().minusDays(3) }.nrow shouldBe 4
    }

    @Test
    fun `union dataframes with different type of the same column`() {
        val df2 = dataFrameOf("age")(32.6, 56.3, null)
        df2["age"].type() shouldBe typeOf<Double?>()
        val merged = df.concat(df2)
        merged["age"].type() shouldBe typeOf<Number?>()
        val updated = merged.convert("age") { "age"<Number?>()?.toDouble() }
        updated["age"].type() shouldBe typeOf<Double?>()
    }

    @Test
    fun `distinct`() {
        val expected = 6
        typed.countDistinct { name and city } shouldBe expected
        typed.select { name and city }.distinct().nrow shouldBe expected
        typed.select { name and city }.countDistinct() shouldBe expected
        val d = typed.distinct { name and city }
        d.nrow shouldBe expected
        d.ncol shouldBe 2
    }

    @Test
    fun `distinct by`() {
        typed.distinctBy { name }.nrow shouldBe 3
        typed.distinctBy { name and city }.nrow shouldBe 6
        typed.distinctBy { expr { age / 10 } }.nrow shouldBe 4
        typed.distinctBy { age / 10 }.nrow shouldBe 4
        typed.distinctBy { expr { city?.get(0) } }.nrow shouldBe 5
    }

    @Test
    fun `addRow`() {
        val res = typed.append("Bob", null, "Paris", null)
        res.nrow shouldBe typed.nrow + 1
        res.name.type() shouldBe typeOf<String>()
        res.age.type() shouldBe typeOf<Int?>()
        res.city.type() shouldBe typeOf<String?>()
        res.weight.type() shouldBe typeOf<Int?>()

        val row = res.last()
        row.name shouldBe "Bob"
        row["age"] shouldBe null
        row.city shouldBe "Paris"
        row.weight shouldBe null
    }

    @Test
    fun `rename`() {
        fun AnyFrame.check() {
            this["name2"].toList() shouldBe typed.name.toList()
            this["age2"].toList() shouldBe typed.age.toList()
            this.columnNames() shouldBe listOf("name2", "age2", "city", "weight")
            this.getColumnOrNull("age") shouldBe null
        }
        typed.rename("name" to "name2", "age" to "age2").check()
        typed.rename { name and age }.into("name2", "age2").check()
        typed.rename { name and age }.into { it.name + "2" }.check()
    }

    @Test
    fun `select with rename`() {
        val expected = typed.select { name and age }.rename { all() }.into { it.name + 2 }
        typed.select { name into "name2" and age.into("age2") } shouldBe expected
    }

    @Test
    fun `nunique`() {
        typed.name.countDistinct() shouldBe 3
    }

    @Test
    fun `encode names`() {
        val encoding = typed.name.distinct().addId("name_id")
        val res = typed.leftJoin(encoding)
        res["name_id"].toList() shouldBe listOf(0, 1, 2, 2, 1, 0, 2)
    }

    @Test
    fun `pivot matches`() {
        val pivoted = typed.pivot { city }.groupBy { name and age and weight }.matches()
        pivoted.ncol shouldBe 4
        typed.ncol + typed.city.countDistinct() - 1
        val data = pivoted.getColumnGroup("city")
        for (row in 0 until typed.nrow) {
            val city = typed[row][city].toString()
            data[city][row] shouldBe true
            for (col in 0 until data.ncol) {
                val column = data.getColumn(col)
                val pivotedValue = column.cast<Boolean>()[row]
                val colName = column.name()
                pivotedValue shouldBe (colName == city)
            }
        }
    }

    @Test
    fun `pivot matches equality`() {
        val res1 = typed.pivot { city }.groupBy { name }.matches()
        val res2 = typed.groupBy { name }.pivot { city }.matches()
        val res3 = typed.groupBy { name }.aggregate {
            pivot { city }.matches()
        }
        res2 shouldBe res1
        res3 shouldBe res1
    }

    @Test
    fun `pivot matches with conversion`() {
        val filtered = typed.dropNulls { city }
        val res = filtered.pivot(inward = false) { city.lowercase() }.groupBy { name and age }.matches()
        val cities = filtered.city.toList().map { it!!.lowercase() }
        val gathered =
            res.gather { colsOf<Boolean> { cities.contains(it.name()) } }.where { it }.keysInto("city")
        val expected = filtered.select { name and age and city.map { it!!.lowercase() } }.moveToEnd { city }
        gathered shouldBe expected
    }

    @Test
    fun `pivot matches distinct rows`() {
        val res = typed.pivot(inward = false) { city }.groupBy { name and age }.matches()
        res.ncol shouldBe 2 + typed.city.countDistinct()
        for (i in 0 until typed.nrow) {
            val city = typed[i][city]
            for (j in typed.ncol until res.ncol) {
                val col = res.getColumn(j)
                col.cast<Boolean>().get(i) shouldBe (col.name() == city.toString())
            }
        }
    }

    @Test
    fun `pivot matches merged rows`() {
        val selected = typed.select { name and city }
        val res = typed.pivot(inward = false) { city }.groupBy { name }.matches()

        res.ncol shouldBe selected.city.countDistinct() + 1
        res.nrow shouldBe selected.name.countDistinct()
        val trueValuesCount = res.columns().drop(1).sumOf { it.cast<Boolean>().toList().count { it } }
        trueValuesCount shouldBe selected.distinct().nrow

        val pairs = (1 until res.ncol).flatMap { i ->
            val col = res.getColumn(i).cast<Boolean>()
            res.filter { it[col] }.rows().map { it.name to col.name() }
        }.toSet()

        pairs shouldBe typed.rows().map { it.name to it.city.toString() }.toSet()
    }

    @Test
    fun `pivot to matrix`() {
        val other by column<String>()
        val others = other.cast<List<String>>()
        val sum by column<Int>()

        val names = typed.name.distinct().toList()

        val src = typed.select { name }
            .add(others) { names }
            .split { others }.intoRows()
            .add(sum) { name.length + other().length }

        val matrix = src.pivot { other }.groupBy { name }.with { sum }
        matrix.getColumnGroup(other.name()).ncol shouldBe names.size
    }

    @Test
    fun `gather bool`() {
        val pivoted = typed.pivot { city }.groupBy { name }.matches()
        val res = pivoted.gather { colsAtAnyDepth().colsOf<Boolean>() }.where { it }.keysInto("city")
        val sorted = res.sortBy { name and city }
        sorted shouldBe typed.select { name and city.map { it.toString() } }.distinct().sortBy { name and city }
    }

    @Test
    fun `gather nothing`() {
        val gat = typed.gather { city and name }

        gat.where { false }.into("key", "value")
            .print()
    }

    @Test
    fun `merge rows keep nulls`() {
        val merged = typed.select { name and city }.implode(dropNA = false) { city }

        val cityList = column<List<String?>>().named("city")
        merged[cityList].sumOf { it.size } shouldBe typed.city.size
        merged[cityList].type() shouldBe typeOf<List<String?>>()

        val expected = typed.groupBy { name }.aggregate { it.city.toSet() into "city" }
        val actual = merged.convert(cityList).with { it.toSet() }

        actual shouldBe expected

        // check that default value for 'dropNulls' is false
        typed.select { name and city }.implode { city } shouldBe merged
    }

    @Test
    fun `merge rows drop nulls`() {
        val merged = typed.select { name and city }.implode(dropNA = true) { city }

        val cityList = column<List<String>>().named("city")
        merged[cityList].sumOf { it.size } shouldBe typed.city.dropNulls().size
        merged[cityList].type() shouldBe typeOf<List<String>>()

        val expected =
            typed.dropNulls { city }.groupBy { name }.aggregate { it.city.toSet() as Set<String> into "city" }
        val actual = merged.convert { cityList }.with { it.toSet() }

        actual shouldBe expected
    }

    @Test
    fun splitRows() {
        val selected = typed.select { name and city }
        val nested = selected.implode(dropNA = false) { city }
        val mergedCity = column<List<String?>>("city")
        val res = nested.split { mergedCity }.intoRows()
        res.sortBy { name } shouldBe selected.sortBy { name }
    }

    @Test
    fun mergeCols() {
        val merged = typed.merge { age and city and weight }.into("info")
        merged.ncol shouldBe 2
        merged.nrow shouldBe typed.nrow
        for (row in 0 until typed.nrow) {
            val list = merged[row]["info"] as List<Any?>
            list.size shouldBe 3
            list[0] shouldBe typed.age[row]
            list[1] shouldBe typed.city[row]
            list[2] shouldBe typed.weight[row]
        }
    }

    @Test
    fun joinColsToString() {
        val merged = typed.merge { age and city and weight }.by(", ").into("info")
        merged.ncol shouldBe 2
        merged.nrow shouldBe typed.nrow
        for (row in 0 until typed.nrow) {
            val joined = merged[row]["info"] as String
            joined shouldBe typed.age[row].toString() + ", " + typed.city[row] + ", " + typed.weight[row]
        }
    }

    @Test
    fun mergeIntoList() {
        val parsed = typed
            .merge { age and city and weight }.by(", ").intoList()
            .toDataFrame { "data" from { it } }
            .split("data").by(", ").into(age, city, weight)
            .parse(ParserOptions(nullStrings = setOf("null")))

        val expected = typed[age, city, weight]
        parsed shouldBe expected
    }

    @Test
    fun mergeColsCustom() {
        val merged = typed
            .merge { name and city and age }
            .by { it[0].toString() + " from " + it[1] + " aged " + it[2] }
            .into("info")
        merged.ncol shouldBe 2
        merged.nrow shouldBe typed.nrow
        merged[0]["info"] shouldBe "Alice from London aged 15"
    }

    @Test
    fun mergeColsCustom2() {
        val merged = typed.merge { name and city and age }.by { "$name from $city aged $age" }.into("info")
        merged.ncol shouldBe 2
        merged.nrow shouldBe typed.nrow
        merged[0]["info"] shouldBe "Alice from London aged 15"
    }

    @Test
    fun splitCol() {
        val merged = typed.merge { age and city and weight }.into("info")
        val info by column<List<Any>>()
        val res = merged.split(info).into("age", "city", "weight")
        res shouldBe typed
    }

    @Test
    fun splitMergeFrameCol() {
        val groups by frameColumn()
        val grouped = typed.groupBy { name }.into(groups)
        val split = grouped.split(groups).into { "rec$it" }
        val merged = split.merge { drop(1) }.notNull().into(groups)
        merged shouldBe grouped
    }

    @Test
    fun splitStringCol() {
        val merged = typed.merge { age and city and weight }.by(" - ").into("info")
        val info by column<String>()
        val res = merged.split { info }.by("-").into("age", "city", "weight")
        val expected = typed.convert { age and city and weight }.with { it.toString() }
        res shouldBe expected
    }

    @Test
    fun splitStringCol2() {
        val merged = typed.merge { age and city and weight }.by(",").into("info")
        val info by column<String>()
        val res = merged.split(info).into("age", "city", "weight")
        val expected = typed.convert { age and city and weight }.with { it.toString() }
        res shouldBe expected
    }

    @Test
    fun splitStringColGenerateNames() {
        val merged = typed.merge { age and city and weight }.by(",").into("info")
        val info by column<String>()
        val res = merged.split(info).into("age") { "extra$it" }
        res.columnNames() shouldBe listOf("name", "age", "extra1", "extra2")
    }

    @Test
    fun splitStringColWithDefaultgenerator() {
        val merged = typed.merge { age and city and weight }.by(",").into("info")
        val info by column<String>()
        val res = merged.split(info).into("age")
        res.columnNames() shouldBe listOf("name", "age", "split1", "split2")
    }

    @Test
    fun splitAgeIntoDigits() {
        fun digits(num: Int) =
            sequence {
                var k = num
                while (k > 0) {
                    yield(k % 10)
                    k /= 10
                }
            }.toList()

        val res = typed.split { age }.by { digits(it) }.into { "digit$it" }
    }

    @Test
    fun splitStringCol3() {
        val merged = typed.merge { age and city and weight }.by(", ").into("info")
        val info by column<String?>()
        val res = merged.split(info).by(",").into("age", "city", "weight")
        val expected = typed.convert { age and city and weight }.with { it.toString() }
        res shouldBe expected
    }

    @Test
    fun splitStringCols() {
        val merged = typed
            .merge { name and city }.by(", ").into("nameAndCity")
            .merge { age and weight }.into("info")
        val nameAndCity by column<String>()
        val info by column<List<Number?>>()
        val res = merged
            .split { nameAndCity }.into("name", "city")
            .split(info).into("age", "weight")
        val expected = typed
            .update { city }.with { it.toString() }
            .move { city }.to(1)
        res shouldBe expected
    }

    @Test
    fun `split by with default`() {
        val res = typed.split { city }.by('o').default("--").into { "a$it" }
        res.sumOf { values().count { it == "--" } } shouldBe 7
    }

    @Test
    fun `merge cols with conversion`() {
        val pivoted = typed.groupBy { name }.pivot { city }.count()
        val res = pivoted.merge { city.asColumnGroup().colsOf<Int>() }.by { it.filterNotNull().sum() }.into("cities")
        val expected = typed.select { name and city }.groupBy { name }.count("cities")
        res shouldBe expected
    }

    @Test
    fun `merge into temp`() {
        dataFrameOf("a", "b", "temp")(1, null, 3)
            .merge { cols("a", "b") }.into("b")
    }

    inline fun <T, reified C, R> Merge<T, C, R>.typeOfElement() = typeOf<C>()

    @Test
    fun `merge not null`() {
        val merge = dataFrameOf("a", "b")(1, null).merge { col("a") }
        merge.typeOfElement() shouldBe typeOf<Any?>()
        merge.notNull().typeOfElement() shouldBe typeOf<Any>()
    }

    inline fun <reified T> List<T>.typeOfElement(): KType = typeOf<List<T>>().arguments[0].type!!

    @Test
    fun `merge cols into list`() {
        val merge = dataFrameOf("a", "b")(1, null).merge { col("a") }
        merge.intoList().typeOfElement() shouldBe typeOf<List<Any?>>()
        merge.by { it }.intoList().typeOfElement() shouldBe typeOf<List<Any?>>()
        // here we can safely narrow down List<Any?> to List<Any> after notNull because the default transformer creates a List from C
        merge.notNull().intoList().typeOfElement() shouldBe typeOf<List<Any>>()
        // if by notNull could go after by { },
        // we won't be able to do so because non-default transformer could introduce nulls itself:
        merge.notNull().by { listOf(1, null) }.intoList().typeOfElement() shouldBe typeOf<List<Int?>>()
    }

    @Test
    fun `generic column type`() {
        val d = typed.convert { city }.with { it?.toCharArray()?.toList() ?: emptyList() }
        println(d.city.type())
    }

    @Test
    fun `column group by`() {
        fun DataFrame<Person>.check() {
            ncol shouldBe 3
            nrow shouldBe typed.nrow
            columnNames() shouldBe listOf("name", "Int", "String")
            val intGroup = this["Int"].asColumnGroup()
            intGroup.columnNames() shouldBe listOf("age", "weight")

            val res = listOf(
                this.name,
                this["Int"]["age"],
                this["String"]["city"],
                this["Int"]["weight"],
            ).toDataFrame().cast<Person>()
            res shouldBe typed
        }
        typed.group { cols { it.data != name } }.into { it.type.jvmErasure.simpleName!! }.check()
        typed.group { age and city and weight }.into { it.type.jvmErasure.simpleName!! }.check()
    }

    @Test
    fun `column group`() {
        val grouped = typed.move { age and name and city }.under("info")
        grouped.ncol shouldBe 2
        grouped.columnNames() shouldBe listOf("info", "weight")
        val res = listOf(
            grouped["info"]["name"],
            grouped["info"]["age"],
            grouped["info"]["city"],
            grouped.weight,
        ).toDataFrame().cast<Person>()
        res shouldBe typed
    }

    @Test
    fun `column ungroup`() {
        val info by columnGroup()
        val res = typed.move { age and city }.under("info").ungroup { info }
        res shouldBe typed
    }

    @Test
    fun `empty group by`() {
        val ungrouped = typed.filter { false }.groupBy { name }.concat()
        ungrouped.nrow shouldBe 0
        ungrouped.ncol shouldBe 0
    }

    @Test
    fun `column stats`() {
        typed.age.mean() shouldBe typed.age.toList().mean()
        typed.age.min() shouldBe typed.age.toList().minOrNull()
        typed.age.max() shouldBe typed.age.toList().maxOrNull()
        typed.age.sum() shouldBe typed.age.toList().sum()
    }

    @Test
    fun `row to string`() {
        typed[0].toString() shouldBe "{ name:Alice, age:15, city:London, weight:54 }"
    }

    @Test
    fun `range slice`() {
        typed[3..5].name.toList() shouldBe typed.name.toList().subList(3, 6)
    }

    @Test
    fun `range slice two times`() {
        typed[3..5][1..2].name.toList() shouldBe typed.name.toList().subList(4, 6)
    }

    @Test
    fun `move to position`() {
        typed.getColumn(1) shouldBe typed.age
        val moved = typed.move { age }.to(2)
        moved.getColumn(2) shouldBe typed.age
        moved.ncol shouldBe typed.ncol
    }

    @Test
    fun `forEachIn`() {
        val pivoted = typed.pivot(inward = true) { city }.groupBy { name and weight }.with { age }
        val sum = pivoted
            .select { "city".allCols() }
            .values()
            .filterNotNull()
            .sumOf { it as Int }
        sum shouldBe typed.age.sum()
    }

    @Test
    fun `parse`() {
        val toStr = typed.convert { weight }.notNull { it.toString() }
        val weightStr = "weight".toColumnOf<String?>()
        val parsed = toStr.convert { weightStr }.toInt()
        parsed shouldBe typed
    }

    @Test
    fun digitize() {
        val a = 20
        val b = 40
        val expected = typed.age.toList().map {
            when {
                it < a -> 0
                it < b -> 1
                else -> 2
            }
        }
        typed.age.digitize(a, b).toList() shouldBe expected

        val expectedRight = typed.age.toList().map {
            when {
                it <= a -> 0
                it <= b -> 1
                else -> 2
            }
        }
        typed.age.digitize(a, b, right = true).toList() shouldBe expectedRight
    }

    @Test
    fun corr() {
        val fixed = typed.fillNulls { weight }.with { 60 }
        val res = fixed.corr()
        res.ncol shouldBe 3
        res.nrow shouldBe 2
        res["age"][0] shouldBe 1.0
        res["weight"][0] shouldBe res["age"][1]
        res["weight"][0] as Double should ToleranceMatcher(0.9, 1.0)
    }

    @Test
    fun `aggregate into grouped column`() {
        val d = typed.groupBy { name }.aggregate {
            val row = meanFor { age and weight }
            row into "mean"
        }
        d.ncol shouldBe 2
        d["mean"].isColumnGroup() shouldBe true
        val mean = d.getColumnGroup("mean")
        mean.ncol shouldBe 2
        mean.columnNames() shouldBe listOf("age", "weight")
        mean.columns().forEach {
            it.type() shouldBe typeOf<Double>()
        }
    }

    @Test
    fun `mean for all columns`() {
        val d = typed.groupBy { name }.mean()
        d.columnNames() shouldBe listOf("name", "age", "weight")
        d.nrow shouldBe typed.name.countDistinct()
        d["age"].type() shouldBe typeOf<Double>()
        d["weight"].type() shouldBe typeOf<Double>()
    }

    @Test
    fun `aggregate into table column`() {
        val d = typed.groupBy { name }.aggregate {
            val row = select { age and weight }
            row into "info"
        }
        d.ncol shouldBe 2
        d["info"].isFrameColumn() shouldBe true
        val info = d.getFrameColumn("info")
        info.forEach {
            it.ncol shouldBe 2
            it.columnNames() shouldBe listOf("age", "weight")
            it.columns().forEach {
                it.typeClass shouldBe Int::class
            }
        }
    }

    @Test
    fun `union table columns`() {
        val grouped = typed.addId("id").groupBy { name }.toDataFrame()
        val flattened = (0 until grouped.nrow).map {
            grouped[it..it]
        }
        val dst = flattened
            .concat().asGroupBy()
            .concat()
            .sortBy("id")
            .remove("id")
        dst shouldBe typed
    }

    @Test
    fun `columns sum`() {
        val name by columnOf("Alice", "Bob", "Charlie")
        val age by columnOf(15, 20, 24)
        val df = dataFrameOf(name, age)

        df.columnNames() shouldBe listOf("name", "age")
        df.nrow shouldBe 3
    }

    @Test
    fun convert1() {
        val res = typed.convert { age }.to<Double>()
        res.age.typeClass shouldBe Double::class
        res["age"].all { it is Double } shouldBe true
    }

    @Test
    fun convert2() {
        val res = typed.convert { weight }.to<BigDecimal?>()
        res.weight.typeClass shouldBe BigDecimal::class
        res["weight"].all { it == null || it is BigDecimal } shouldBe true
    }

    @Test
    fun convert3() {
        val res = typed.convert { all() }.to<String?>()
        res.columns().forEach { it.typeClass shouldBe String::class }
        res.columns().map { it.hasNulls() } shouldBe typed.columns().map { it.hasNulls() }
    }

    @Test
    fun replace() {
        val res = typed.replace { age }.with(2021 - typed.age)
        val expected = typed.update { age }.with { 2021 - age }
        res shouldBe expected
    }

    @Test
    fun `replace with rename`() {
        val res = typed.replace { age }.with { it.rename("age2") }
        res shouldBe typed.rename { age }.into("age2")
    }

    @Test(expected = IllegalArgumentException::class)
    fun `replace exception`() {
        typed.replace { colsOf<Int?>() }.with(typed.name)
    }

    @Test
    fun `replace two columns`() {
        val res = typed.replace { age and weight }.with(typed.age * 2, typed.weight * 2)
        val expected = typed.update { age and weight }.with { it?.times(2) }
        res shouldBe expected
    }

    @Test
    fun `replace with expression`() {
        val res = typed.replace { age }.with { 2021 - age named "year" }
        val expected = typed
            .convert { age }.with { 2021 - age }
            .rename { age }.into("year")
        res shouldBe expected
    }

    @Test
    fun `add dataframe`() {
        val first = typed.select { name and age }
        val second = typed.select { city and weight }
        first.add(second) shouldBe typed
        first.addAll(second.columns()) shouldBe typed
        first + second.columns() shouldBe typed
    }

    @Test
    fun explodeLists() {
        val df = dataFrameOf("lists")(listOf(1, 2), listOf(3))

        df.explode("lists") shouldBe dataFrameOf("lists")(1, 2, 3)
    }

    @Test
    fun splitUnequalLists() {
        val values by columnOf(1, 2, 3, 4)
        val list1 by columnOf(listOf(1, 2, 3), listOf(), listOf(1, 2), null)
        val list2 by columnOf(listOf(1, 2), listOf(1, 2), listOf(1, 2), listOf(1))
        val df = dataFrameOf(values, list1, list2)
        val res = df.explode { list1 and list2 }
        val expected = dataFrameOf(values.name(), list1.name(), list2.name())(
            1, 1, 1,
            1, 2, 2,
            1, 3, null,
            2, null, 1,
            2, null, 2,
            3, 1, 1,
            3, 2, 2,
            4, null, 1,
        )
        res shouldBe expected
    }

    @Test
    fun splitUnequalListAndFrames() {
        val values by columnOf(1, 2, 3)
        val list1 by columnOf(listOf(1, 2, 3), listOf(1), listOf(1, 2))
        val frames by listOf(listOf(1, 2), listOf(1, 2), listOf(1, 2)).map {
            val data = it.toColumn("data")
            val dataStr = it.map { it.toString() }.toColumn("dataStr")
            dataFrameOf(data, dataStr)
        }.toColumn()
        frames.kind shouldBe ColumnKind.Frame

        val df = dataFrameOf(values, list1, frames)
        val res = df.explode { list1 and frames }.ungroup(frames)
        val expected = dataFrameOf(values.name(), list1.name(), "data", "dataStr")(
            1, 1, 1, "1",
            1, 2, 2, "2",
            1, 3, null, null,
            2, 1, 1, "1",
            2, null, 2, "2",
            3, 1, 1, "1",
            3, 2, 2, "2",
        )
        res shouldBe expected
    }

    @Test
    fun `update nullable column with not null`() {
        val df = dataFrameOf("name", "value")("Alice", 1, null, 2)
        df.update("name").at(0).with { "ALICE" }
    }

    @Test(expected = IllegalArgumentException::class)
    fun `update with wrong type`() {
        typed.update("age").with { "string" }
    }

    @Test
    fun `update with null`() {
        val updated = typed.update { age }.at(2).withNull()
        updated.age[2] shouldBe null
        updated.age.hasNulls shouldBe true
    }

    @Test
    fun `update with two conditions`() {
        fun DataFrame<Person>.check() = indices { age == 100 } shouldBe listOf(1, 3)

        typed
            .update { age }.at(1..3).where { it > 20 }.with { 100 }
            .check()
        typed
            .update { age }.where { it > 20 }.at(1..3).with { 100 }
            .check()
    }

    @Test
    fun `update nulls`() {
        typed
            .update { weight }.where { it == null }.with { 15 }
            .weight.hasNulls shouldBe false
    }

    @Test
    fun `mean all columns`() {
        typed.mean().values() shouldBe listOf(typed.age.mean(), typed.weight.mean())
    }

    @Test
    fun `mean by string`() {
        typed.mean("weight") shouldBe typed.weight.mean()
    }

    @Test
    fun `create column with single string value`() {
        val frameCol by columnOf(typed, null, typed)
        frameCol.kind() shouldBe ColumnKind.Frame
        frameCol.name() shouldBe "frameCol"

        val mapCol by columnOf(typed.name, typed.city)
        mapCol.kind() shouldBe ColumnKind.Group
        mapCol.name() shouldBe "mapCol"

        val valueCol = columnOf("Alice") named "person"
        valueCol.kind() shouldBe ColumnKind.Value
        valueCol.name() shouldBe "person"
    }

    @Test
    fun `append many`() {
        val res = typed.append(
            "John", 22,
            "New York", 46,
            "Philip", 25,
            "Chelyabinsk", 36,
        )
        res.nrow shouldBe typed.nrow + 2
    }

    @Test
    fun `append wrong number of arguments`() {
        shouldThrow<IllegalStateException> {
            dataFrameOf("name", "age")(
                "Alice", 15,
                "Bob", 20,
            ).append("John")
        }
    }

    @Test
    fun `first last`() {
        typed.first() shouldBe typed[0]
        typed.last() shouldBe typed[typed.nrow - 1]
        typed.city.first() shouldBe typed[0].city
        typed.city.last() shouldBe typed[typed.nrow - 1].city
    }

    @Test
    fun `select several rows`() {
        df[2, 5].nrow shouldBe 2
        df[0, 3, 5] shouldBe df[listOf(0, 3, 5)]
        df[3, 5][name, age] shouldBe df[name, age][3, 5]
    }

    @Test
    fun `select several column values`() {
        typed.name[2, 5, 6] shouldBe typed.name[listOf(2, 5, 6)]
    }

    @Test
    fun `get by column accessors`() {
        val animal by columnOf("cat", "snake", "dog")
        val age by columnOf(2.5, 3.0, 0.5)
        val visits by columnOf(1, 3, 2)

        val df = dataFrameOf(animal, age, visits)

        val d1 = df[1..1][animal, age]
        d1.ncol shouldBe 2
        d1.nrow shouldBe 1

        val d2 = df[0..1]["animal", "age"]
        d2.ncol shouldBe 2
        d2.nrow shouldBe 2

        val r1 = df[1][animal, age]
        r1.values() shouldBe d1.single().values()

        val r2 = df[0]["animal", "age"]
        r2 shouldBe df[animal, age][0]
    }

    @Test
    fun between() {
        typed.filter { age.between(20, 40, false) }.nrow shouldBe 2

        typed.filter { age in 20..40 }.nrow shouldBe 5

        typed.age.between(20, 40).count { it } shouldBe 5
    }

    @Test
    fun iterators() {
        var counter = 0
        for (a in df) counter++
        counter shouldBe df.nrow

        var ageSum = 0
        for (a in typed.age) {
            ageSum += a
        }

        ageSum shouldBe typed.age.sum()
    }

    @Test
    fun `create with random`() {
        val df = dataFrameOf('a'..'f').randomInt(3)
        df.nrow shouldBe 3
        df.ncol shouldBe ('a'..'f').count()
        df.columns().forEach { it.type() shouldBe typeOf<Int>() }
    }

    @Test
    fun `create with list builder`() {
        val df = dataFrameOf(4..10 step 2) { h -> List(10) { h } }
        df.nrow shouldBe 10
        df.ncol shouldBe 4
        df.columns().forEach { col -> col.forEach { it shouldBe col.name().toInt() } }
    }

    @Test
    fun `create with vararg header and builder`() {
        val df = dataFrameOf("first", "secon", "third") { name -> name.toCharArray().toList() }
        df.nrow shouldBe 5
        df.ncol shouldBe 3
        df.columns().forEach { col -> col.name() shouldBe col.values().joinToString("") }
    }

    @Test
    fun `create with vararg doubles and fill equal`() {
        val df = dataFrameOf(1.0.toString(), 2.5.toString()).fill(5, true)
        df.nrow shouldBe 5
        df.ncol shouldBe 2
        df.columns().forEach { col -> col.forEach { it shouldBe true } }
    }

    @Test
    fun `create with list of names and fill nulls`() {
        val names = listOf("first", "second")
        val df = dataFrameOf(names).nulls<Double>(10)
        df.nrow shouldBe 10
        df.ncol shouldBe 2
        df.columns().forEach { col -> (col.type() == typeOf<Double?>() && col.allNulls()) shouldBe true }
    }

    @Test
    fun `create with list of names and fill true`() {
        val first by column<Boolean>()
        val second by column<Boolean>()
        val df = dataFrameOf(first, second).fill(5) { true }
        df.nrow shouldBe 5
        df.ncol shouldBe 2
        df.columns().forEach { col -> (col.type() == typeOf<Boolean>() && col.all { it == true }) shouldBe true }
    }

    @Test
    fun `create with int range header and int range data `() {
        val df = dataFrameOf(1..5) { 1..5 }
        df.nrow shouldBe 5
        df.ncol shouldBe 5
        df.columns().forEach { col -> col.forEachIndexed { row, value -> value shouldBe row + 1 } }
    }

    @Test
    fun `get typed column by name`() {
        val col = df.getColumn("name").cast<String>()
        col[0].substring(0, 3) shouldBe "Ali"
    }

    @Test
    fun `select all after`() {
        typed.select { allAfter(age) } shouldBe typed.select { city and weight }
        typed.select { allFrom(age) } shouldBe typed.select { age and city and weight }
        typed.select { allBefore(age) } shouldBe typed.select { name }
        typed.select { allUpTo(age) } shouldBe typed.select { name and age }
    }

    @Test
    fun `cols of type`() {
        val stringCols = typed.select { colsOf<String?>() }
        stringCols.columnNames() shouldBe listOf("name", "city")
    }

    @Test
    fun `get row value by expression`() {
        val expression: RowExpression<Person, Int> = { it.age * 2 }
        val added = typed.add("new") { it[expression] }
        added shouldBe typed.add("new") { age * 2 }
    }

    @Test
    fun `render nested data frames to string`() {
        val rendered = typed
            .drop(1)
            .groupBy { name }
            .groups
            .asIterable()
            .joinToString("\n") { renderValueForStdout(it).truncatedContent }
        rendered shouldBe
            """
            [2 x 4]
            [3 x 4]
            [1 x 4] { name:Alice, age:20, weight:55 }
            """.trimIndent()
    }

    @Test
    fun `drop where any na`() {
        val updated = typed.convert { weight }.with { if (name == "Alice") Double.NaN else it?.toDouble() }
        val expected = updated.count { city != null && !("weight"<Double?>()?.isNaN() ?: true) }

        fun AnyFrame.check() = nrow shouldBe expected

        updated.dropNA { city and weight }.check()
        updated.dropNA(city, weight).check()
        updated.dropNA("city", "weight").check()
        updated.dropNA(Person::city, Person::weight).check()
    }

    @Test
    fun `drop where all na`() {
        val updated = typed.convert { weight }.with { if (name == "Alice") Double.NaN else it?.toDouble() }
        val expected = updated.count { city != null || !("weight"<Double?>()?.isNaN() ?: true) }

        fun AnyFrame.check() = nrow shouldBe expected

        updated.dropNA(whereAllNA = true) { city and weight }.check()
        updated.dropNA(city, weight, whereAllNA = true).check()
        updated.dropNA("city", "weight", whereAllNA = true).check()
        updated.dropNA(Person::city, Person::weight, whereAllNA = true).check()
    }

    @Test
    fun sortWith() {
        typed.sortWith { r1, r2 ->
            when {
                r1.name < r2.name -> -1
                r1.name > r2.name -> 1
                else -> -r1.age.compareTo(r2.age)
            }
        } shouldBe typed.sortBy { name and age.desc() }

        val comparator = Comparator<DataRow<Person>> { r1, r2 -> -r1.name.compareTo(r2.name) }
        typed.sortWith(comparator) shouldBe typed.sortByDesc { name }
    }

    @Test
    fun sortByDescDesc() {
        typed.sortByDesc { name.desc() and age } shouldBe typed.sortBy { name and age.desc() }
    }

    @Test
    fun `get column by columnRef with data`() {
        val col by columnOf(1, 2, 3)
        val df = col.toDataFrame()
        df[1..2][col].values() shouldBe listOf(2, 3)
    }

    @Test
    fun `get by column`() {
        typed[1..2].get { typed.age }.size() shouldBe typed.age.size()
    }

    @Test
    fun `null column test`() {
        val df = dataFrameOf("col")(null, null)
        df["col"].kind() shouldBe ColumnKind.Value
        df["col"].type() shouldBe nothingType(true)
    }

    @Test
    fun `groupBy with map`() {
        typed
            .groupBy { name.map { it.lowercase() } }.toDataFrame()
            .name.values() shouldBe
            typed.name.distinct().lowercase().values()
    }

    @Test
    fun `groupBy none`() {
        val grouped = typed.groupBy { none() }
        grouped.keys.ncol shouldBe 0
        grouped.groups.size shouldBe 1
        val values = grouped.values()
        values.nrow shouldBe 1
        values.columns().forEach {
            it.typeClass shouldBe List::class
            (it[0] as List<*>).size shouldBe typed.nrow
        }
        values.explode() shouldBe typed
    }

    @Test
    fun `pivot max`() {
        val pivoted = typed.pivot(inward = false) { city }.groupBy { name }.max { age }
        pivoted.single { name == "Charlie" }["Moscow"] shouldBe 30
    }

    @Test
    fun `pivot all values`() {
        val pivoted = typed.pivot(inward = false) { city }.groupBy { name }.values()
        pivoted.ncol shouldBe 1 + typed.city.countDistinct()
        pivoted.columns().drop(1).forEach {
            it.kind() shouldBe ColumnKind.Group
            it.asColumnGroup().columnNames() shouldBe listOf("age", "weight")
        }
    }

    @Test
    fun `pivot mean values`() {
        val pivoted = typed.pivot { city }.groupBy { name }.mean()
        pivoted.getColumnGroup(1).columns().forEach {
            it.kind() shouldBe ColumnKind.Group
            val group = it.asColumnGroup()
            group.columnNames() shouldBe listOf("age", "weight")
            group.columns().forEach {
                it.type() shouldBe typeOf<Double?>()
            }
        }
    }

    @Test
    fun `aggregate dataframe with pivot`() {
        val summary = typed.aggregate {
            count() into "count"
            pivot { name }.max { age }
            sum { weight } into "total weight"
        }
        val expected = dataFrameOf("count", "Alice", "Bob", "Charlie", "total weight")(7, 20, 45, 40, 354)
        summary shouldBe expected.group { cols(1..3) }.into("name")[0]
    }

    @Test
    fun `pivot grouped max`() {
        val pivoted = typed.pivot { name }.groupBy { city }.max()
        pivoted.getColumnGroup("name").columns().forEach {
            it.kind() shouldBe ColumnKind.Group
            val group = it.asColumnGroup()
            group.columnNames() shouldBe listOf("age", "weight")
        }
    }

    @Test
    fun `find the longest string`() {
        val longestCityName = "Taumatawhakatangihangakoauauotamateaturipukakapikimaungahoronukupokaiwhenuakitanatahu"
        val updated = typed.update { city }.where { it == "Dubai" }.with { longestCityName }
        updated.valuesNotNull { colsOf<String?>() }.maxByOrNull { it.length } shouldBe longestCityName
    }

    @Test
    fun `sort by expression`() {
        val sorted = typed.sortBy { expr { name.length }.desc() }
        sorted.name.values() shouldBe typed.name.values().sortedByDescending { it.length }
    }

    @Test
    fun `grouped sort by count`() {
        val sorted = typed.groupBy { name }.sortByCount()
        sorted.toDataFrame().name.values() shouldBe
            typed.rows()
                .groupBy { it.name }
                .toList()
                .sortedByDescending { it.second.size }
                .map { it.first }
    }

    @Test
    fun `grouped sort by key`() {
        val sorted = typed.groupBy { name }.sortByKey()
        sorted.toDataFrame().name.values() shouldBe
            typed.name
                .distinct()
                .values()
                .sorted()
    }

    @Test
    fun `infer ColumnGroup type in convert with`() {
        val g by frameColumn()
        val grouped = typed
            .groupBy { name }.toDataFrame(g.name)
            .convert(g).with { it.first() }
        grouped[g.name].kind() shouldBe ColumnKind.Group
    }

    @Test
    fun `filter GroupBy by groups`() {
        val grouped = typed.groupBy { name }
        val filtered = grouped.filter { group.nrow > 2 }.concat()
        filtered shouldBe typed.filter { name == "Charlie" }
    }

    @Test
    fun `split inplace`() {
        val split = typed.split { name }.by { it.toCharArray().asIterable() }.inplace()
        split["name"] shouldBe typed.name.map { it.toCharArray().toList() }
    }

    @Test
    fun `split into rows with transform`() {
        val split = typed.split { city }.by { it.toCharArray().toList() }.intoRows()
        split.nrow shouldBe typed.city.sumOf { it?.length ?: 0 }
    }

    @Test
    fun `render to string`() {
        val expected =
            """
                 name age   city weight
            0   Alice  15 London     54
            1     Bob  45  Dubai     87
            2 Charlie  20 Moscow   null
            3 Charlie  40  Milan   null
            4     Bob  30  Tokyo     68
            5   Alice  20   null     55
            6 Charlie  30 Moscow     90
            """.trimIndent()

        typed.toString().trimIndent() shouldBe expected
    }

    @Test
    fun `isNumber`() {
        typed.age.isNumber() shouldBe true
        typed.weight.isNumber() shouldBe true

        DataColumn.createValueColumn("a", emptyList<Nothing>(), nothingType(false)).isNumber() shouldBe true
        DataColumn.createValueColumn("a", listOf(null), nothingType(true)).isNumber() shouldBe true
    }

    @Test
    fun `pivot null to default`() {
        val pivoted = typed
            .groupBy { name }.pivot { city }.default(0).min { weight }
        pivoted.columns().forEach {
            it.hasNulls() shouldBe false
        }
    }

    @Test
    fun `iterable to column`() {
        val ref by column<String>()
        val col = listOf("a", null).toColumn(ref)
        col.hasNulls() shouldBe true
    }

    @Test
    fun `columnAccessor map linear`() {
        // SampleStart
        val age by column<Int>()
        var counter = 0
        val year by age.map {
            counter++
            2021 - it
        }
        df.filter { year > 2000 }.nrow shouldBe 3
        counter shouldBe df.nrow
        // SampleEnd
    }

    @Test
    fun convertTo() {
        data class Target(
            val name: String,
            val age: Int,
            val city: String?,
            val weight: Int?,
        )

        df.convertTo<Target>() shouldBe df
        df.convert { age }.toStr().convertTo<Target>() shouldBe df
        df.add("col") { 1 }.convertTo<Target>(ExcessiveColumns.Remove) shouldBe df

        val added = df.add("col") { 1 }
        added.convertTo(typeOf<Target>(), ExcessiveColumns.Keep) shouldBe added

        df.remove { city }.convertTo<Target>() shouldBe
            df.update { city }.withNull()
                .move { city }.toEnd()

        shouldThrow<IllegalArgumentException> {
            df.remove { age }.convertTo<Target>()
        }

        df.remove { age }.convertTo<Target> {
            fill { age }.with { -1 }
        } shouldBe
            df.update { age }.with { -1 }
                .move { age }.toEnd()

        shouldThrow<TypeConversionException> {
            df.update { name }.at(2).withNull()
                .convertTo<Target>()
        }

        shouldThrow<IllegalArgumentException> {
            df.convert { age }.toStr().convertToImpl(
                typeOf<Target>(),
                allowConversion = false,
                ExcessiveColumns.Remove,
            )
        }

        shouldThrow<ExcessiveColumnsException> {
            df.add("col") { 1 }.convertTo<Target>(ExcessiveColumns.Fail) shouldBe df
        }

        val list = df.toListOf<Target>()
        list shouldBe df.convertTo<Target>().toList()

        val listDf = list.toDataFrame()
        listDf shouldBe df
        listDf.toList() shouldBe list
    }

    @Test
    fun typedFrameColumn() {
        @DataSchema
        data class Student(val name: String, val age: Int, val weight: Int?)

        @DataSchema
        data class Target(val city: String?, val students: List<Student>)

        val grouped = df.groupBy { city }.toDataFrame("students")

        val list = grouped.toListOf<Target>()
        list shouldBe grouped.convertTo<Target>().toList()

        val listDf = list.toDataFrame(maxDepth = 2)
        listDf shouldBe grouped.update { getFrameColumn("students") }.with { it.remove("city") }

        listDf.toList() shouldBe list
    }

    @Test
    fun reorderColumns() {
        typed.reorderColumnsByName().columnNames() shouldBe typed.columnNames().sorted()
        val grouped = typed.groupBy { city }.into("a").reorderColumnsByName()
        grouped.columnNames() shouldBe listOf("a", "city")
        grouped.getFrameColumn("a")[0].columnNames() shouldBe typed.columnNames().sorted()
    }

    @Test
    fun typedColumnGroup() {
        @DataSchema
        data class Info(val age: Int, val weight: Int?)

        @DataSchema
        data class Target(val name: String, val info: Info, val city: String?)

        val grouped = typed.group { age and weight }.into("info")

        val list = grouped.toListOf<Target>()
        list shouldBe grouped.convertTo<Target>().toList()

        val listDf = list.toDataFrame(maxDepth = 2)
        listDf shouldBe grouped
        listDf.toList() shouldBe list
    }

    @Test
    fun splitWithRegex() {
        val data by column<String>()
        val merged = typed.merge { name and city }.by("|").into(data)
        merged.split { data }.match("""(.*)\|(.*)""".toRegex()).into("name", "city") shouldBe
            typed.update { city }.with { it ?: "null" }
                .move { city }.to(1)
    }

    @Test
    fun splitIntoThisAndNewColumn() {
        val split = typed.split { name }.by { listOf(it.dropLast(1), it.last()) }.into("name", "lastChar")
        split.columnNames().sorted() shouldBe (typed.columnNames() + "lastChar").sorted()
    }

    @Test
    fun groupByAggregateSingleColumn() {
        val agg = typed.groupBy { name }.aggregate { city into "city" }
        agg shouldBe typed.groupBy { name }.values { city }
        agg["city"].type shouldBe typeOf<List<String?>>()
    }

    @Test
    fun implodeWithNulls() {
        val merged = typed
            .update { weight }.where { name == "Charlie" }.withNull()
            .select { name and weight }
            .implode(dropNA = true) { weight }

        merged["weight"].type() shouldBe typeOf<List<Int>>()
    }

    @Test
    fun updateWithZero() {
        val updated = typed
            .convert { weight }.toDouble()
            .update { colsOf<Number?>() }.where { name == "Charlie" }.withZero()
        updated.age.type shouldBe typeOf<Int>()
        updated["weight"].type shouldBe typeOf<Double>()
        val filtered = updated.filter { name == "Charlie" }
        filtered.nrow shouldBe 3
        filtered.age.forEach {
            it shouldBe 0
        }
        filtered["weight"].forEach {
            it shouldBe .0
        }
    }

    @Test
    fun map() {
        val mapped = typed.mapToFrame {
            name into "name"
            "year" from 2021 - age
            "CITY" from { city?.uppercase() }
        }
        mapped.columnNames() shouldBe listOf("name", "year", "CITY")
    }

    @Test
    fun `groupByGroup name clash`() {
        val groupName = GroupBy.groupedColumnAccessor.name()
        typed.add(groupName) { name }
            .groupBy(groupName).toDataFrame()
            .ncol shouldBe 2
    }

    @Test
    fun describe() {
        val desc = typed
            .group { age and weight }.into("info")
            .groupBy { city }.toDataFrame()
            .describe()
        desc.nrow shouldBe typed.ncol + 1
        desc["type"][0] shouldBe "String?"
        desc["path"][1] shouldBe listOf("group", "name")
        desc["name"][0] shouldBe "city"
        desc["count"][3] shouldBe 7
        desc["unique"][4] shouldBe 6
        desc["nulls"][3] shouldBe 2
        desc["top"][0] shouldBe "London"
        desc["mean"][2] shouldBe 28.571428571428573
        desc["std"][2] shouldBe 11.073348527841414
        desc["min"][2] shouldBe 15
        desc["median"][2] shouldBe 30
        desc["max"][2] shouldBe 45
        desc.print()
    }

    @DataSchema
    data class ComparableTest(
        val int: Int,
        val comparableInt: Comparable<Int>,
        val string: String,
        val comparableString: Comparable<String>,
        val comparableStar: Comparable<*>,
        val comparableNothing: Comparable<Nothing>,
    )

    @Test
    fun `is comparable`() {
        val df = listOf(
            ComparableTest(1, 1, "a", "a", 1, 1),
            ComparableTest(2, 2, "b", "b", "2", "2"),
        ).toDataFrame()

        df.int.valuesAreComparable() shouldBe true
        df.comparableInt.valuesAreComparable() shouldBe true
        df.string.valuesAreComparable() shouldBe true
        df.comparableString.valuesAreComparable() shouldBe true
        df.comparableStar.valuesAreComparable() shouldBe false
        df.comparableNothing.valuesAreComparable() shouldBe false
    }

    @Test
    fun `describe twice minimal`() {
        val df = dataFrameOf("a", "b")(1, "foo", 3, "bar")
        val desc1 = df.describe()
        val desc2 = desc1.describe()
        desc2::class shouldBe DataFrameImpl::class
    }

    @Test
    fun `describe twice`() {
        val df = typed
            .group { age and weight }.into("info")
            .groupBy { city }.toDataFrame()
        val desc1 = df.describe()
        val desc2 = desc1.describe()
        desc2::class shouldBe DataFrameImpl::class
    }

    @Test
    fun `index by column accessor`() {
        val col = listOf(1, 2, 3, 4, 5).toColumn("name")
        col.toDataFrame()[1..2][col].size shouldBe 2

        val col2 = columnOf(1, 2, 3, 4, 5) named "name"
        col2.toDataFrame()[1..2][col2].size shouldBe 2

        val col3 by columnOf(1, 2, 3, 4, 5)
        col3.toDataFrame()[1..2][col3].size shouldBe 2

        val col4 by listOf(1, 2, 3, 4, 5).toColumn()
        col4.toDataFrame()[1..2][col4].size shouldBe 2
    }

    @Test
    fun `take drop in columns selector`() {
        typed.select { take(3) } shouldBe typed.select { cols(0..2) }
        typed.select { takeLast(2) } shouldBe typed.select { cols(2..3) }
        typed.select { drop(1) } shouldBe typed.select { cols(1..3) }
        typed.select { dropLast(1) } shouldBe typed.select { cols(0..2) }
    }

    @Test
    fun `except in columns selector`() {
        typed.select { allExcept { age and weight } } shouldBe typed.select { name and city }

        typed
            .group { age and weight and city }.into("info")
            .alsoDebug()
            .select { allExcept { "info"["age"] } }
            .alsoDebug()
            .let {
                it.name shouldBe typed.name
                it["info"]["weight"] shouldBe typed.weight
                it["info"]["city"] shouldBe typed.city
            }
    }

    @Test
    fun `get by empty path`() {
        val all = typed[pathOf()]

        all.asColumnGroup().asDataFrame().columns() shouldBe typed.columns()

        typed.getColumn { emptyPath() } shouldBe all
    }

    @Test
    fun `update frame column to null`() {
        val grouped = typed.groupBy { name }.toDataFrame("group")
        grouped["group"].kind shouldBe ColumnKind.Frame
        val updated = grouped.update("group").at(2).withNull()
        updated["group"].kind shouldBe ColumnKind.Value
    }

    @Test
    fun `merge into same name`() {
        typed.merge { name and city }.into("age") shouldBe
            typed
                .merge { name and city }.into("data")
                .remove("age")
                .rename("data" to "age")
    }

    @Test
    fun `groupBy sort`() {
        typed
            .groupBy { name }.sortByDesc { age }.xs("Charlie").concat() shouldBe
            typed
                .filter { name == "Charlie" }
                .sortBy { age.desc() }
                .remove { name }
    }

    @Test
    fun `split into columns`() {
        val group by frameColumn()
        typed
            .groupBy { name }.into(group)
            .split(group).intoColumns()
    }

    @Test
    fun `takedrop for column`() {
        typed.age.take(2) shouldBe typed.age[0..1]
        typed.age.drop(2) shouldBe typed.age[2 until typed.nrow]
        typed.age.takeLast(2) shouldBe typed.age.drop(typed.nrow - 2)
        typed.age.dropLast(2) shouldBe typed.age.take(typed.nrow - 2)
    }

    @Test
    fun `transpose row`() {
        typed
            .select { age and weight }[1]
            .transpose()
            .maxBy { it.value as Int? }
            .name shouldBe "weight"
        typed[2]
            .transpose()
            .dropNulls { value }
            .name
            .toList() shouldBe listOf("name", "age", "city")
    }

    @Test
    fun xs() {
        typed.xs("Charlie") shouldBe typed.filter { name == "Charlie" }.remove { name }
        typed.xs("Charlie", 20).nrow shouldBe 1
        typed.xs(20) { age }.nrow shouldBe 2
        shouldThrow<java.lang.IllegalArgumentException> {
            typed.xs(20) { age and weight }
        }
        shouldThrow<java.lang.IllegalArgumentException> {
            typed.xs("Charlie", 20) { name }
        }
        shouldThrow<java.lang.IllegalArgumentException> {
            typed.xs("Charlie", 20, "Moscow", null, 1)
        }
    }

    @Test
    fun `groupBy xs`() {
        typed.groupBy { name }.xs("Charlie").concat() shouldBe typed.xs("Charlie")
        typed
            .groupBy { name }.xs("Moscow") { city }.concat()
            .print()
    }

    @Test
    fun getMissingColumn() {
        val col = typed.getColumnsImpl(UnresolvedColumnsPolicy.Create) { "unknown"<Int>() }
        col.size shouldBe 1
        col[0].name shouldBe "unknown"
        col[0].isMissingColumn() shouldBe true
    }

    @Test
    fun getMissingColumn2() {
        val col = typed.remove { city }.getColumnsImpl(UnresolvedColumnsPolicy.Create) { city }
        col.size shouldBe 1
        col[0].name shouldBe typed.city.name()
        col[0].isMissingColumn() shouldBe true
    }

    @Test
    fun `groupBy into accessor or kproperty`() {
        val n by column<Int>()

        data class Data(
            @ColumnName("total") val count: Int,
        )

        typed.groupBy { name }.aggregate {
            count() into n
            count() into Data::count
        } shouldBe
            typed
                .groupBy { name }.count(n.name())
                .add("total") { "n"<Int>() }
    }

    @Test
    fun `aggregate null row`() {
        val aggregated = typed.groupBy { name }.aggregate {
            (if (name.first().startsWith("A")) first() else null) into "agg"
        }["agg"]

        aggregated.kind shouldBe ColumnKind.Group
        aggregated.size shouldBe 3
        aggregated.count { it.isNA } shouldBe 2
    }

    @Test
    fun takeWhile() {
        typed.takeWhile { weight != null } shouldBe typed[0..1]
        typed.takeWhile { true } shouldBe typed
    }

    @Test
    fun dropWhile() {
        typed.dropWhile { weight != null } shouldBe typed.drop(2)
        typed.dropWhile { false } shouldBe typed
    }

    @Test
    fun takeLast() {
        typed.takeLast(2) shouldBe typed[5..6]
        shouldThrow<IllegalArgumentException> {
            typed.takeLast(-1)
        }
        typed.takeLast(20) shouldBe typed
    }

    @Test
    fun dropLast() {
        typed.dropLast(2) shouldBe typed[0..4]
        shouldThrow<IllegalArgumentException> {
            typed.dropLast(-1)
        }
        typed.dropLast(20) shouldBe typed.take(0)
    }

    @Test
    fun drop() {
        typed.drop(2) shouldBe typed[2..6]
        shouldThrow<IllegalArgumentException> {
            typed.drop(-1)
        }
        typed.drop(typed.nrow) shouldBe typed.filter { false }
        typed.drop(20) shouldBe typed.filter { false }
    }

    @Test
    fun take() {
        typed.take(2) shouldBe typed[0..1]
        shouldThrow<IllegalArgumentException> {
            typed.take(-1)
        }
        typed.take(typed.nrow) shouldBe typed
        typed.take(20) shouldBe typed
    }

    @Test
    fun `select into accessor`() {
        val newName by column<String>()
        typed.select { name into newName and age }.columnNames() shouldBe listOf("newName", "age")
    }

    @Test
    fun `api for creating GroupBy with empty groups which can be aggregated using statistics`() {
        val df1 = dataFrameOf("a", "b")(1, "c")
        val df2 = DataFrame.empty()
        val groupBy = dataFrameOf(columnOf("group1", "group2") named "group", columnOf(df1, df2)).asGroupBy()

        val exception = shouldThrow<IllegalStateException> {
            groupBy.aggregate {
                sum("a")
            }
        }

        exception.message shouldBe "Column 'a' not found among []."

        val groupBy1 = groupBy
            .updateGroups { if (it.isEmpty()) DataFrame.empty(groupBy.groups.schema.value) else it }

        val res = groupBy1.aggregate {
            sum("a")
        }

        res["aggregated"].values() shouldBe listOf(1, 0)
    }
}
