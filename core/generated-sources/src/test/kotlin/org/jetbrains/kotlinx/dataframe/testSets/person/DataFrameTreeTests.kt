package org.jetbrains.kotlinx.dataframe.testSets.person

import io.kotest.assertions.fail
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.jetbrains.dataframe.impl.codeGen.CodeGenerator
import org.jetbrains.dataframe.impl.codeGen.InterfaceGenerationMode
import org.jetbrains.dataframe.impl.codeGen.generate
import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.AnyRow
import org.jetbrains.kotlinx.dataframe.ColumnsContainer
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.DataRow
import org.jetbrains.kotlinx.dataframe.Selector
import org.jetbrains.kotlinx.dataframe.annotations.DataSchema
import org.jetbrains.kotlinx.dataframe.api.GroupBy
import org.jetbrains.kotlinx.dataframe.api.GroupWithKey
import org.jetbrains.kotlinx.dataframe.api.add
import org.jetbrains.kotlinx.dataframe.api.addId
import org.jetbrains.kotlinx.dataframe.api.after
import org.jetbrains.kotlinx.dataframe.api.aggregate
import org.jetbrains.kotlinx.dataframe.api.append
import org.jetbrains.kotlinx.dataframe.api.asColumnGroup
import org.jetbrains.kotlinx.dataframe.api.asDataFrame
import org.jetbrains.kotlinx.dataframe.api.asFrame
import org.jetbrains.kotlinx.dataframe.api.asGroupBy
import org.jetbrains.kotlinx.dataframe.api.at
import org.jetbrains.kotlinx.dataframe.api.by
import org.jetbrains.kotlinx.dataframe.api.cast
import org.jetbrains.kotlinx.dataframe.api.colsOf
import org.jetbrains.kotlinx.dataframe.api.column
import org.jetbrains.kotlinx.dataframe.api.columnGroup
import org.jetbrains.kotlinx.dataframe.api.columnOf
import org.jetbrains.kotlinx.dataframe.api.columnsCount
import org.jetbrains.kotlinx.dataframe.api.concat
import org.jetbrains.kotlinx.dataframe.api.convert
import org.jetbrains.kotlinx.dataframe.api.count
import org.jetbrains.kotlinx.dataframe.api.dataFrameOf
import org.jetbrains.kotlinx.dataframe.api.distinct
import org.jetbrains.kotlinx.dataframe.api.dropNulls
import org.jetbrains.kotlinx.dataframe.api.duplicate
import org.jetbrains.kotlinx.dataframe.api.duplicateRows
import org.jetbrains.kotlinx.dataframe.api.emptyDataFrame
import org.jetbrains.kotlinx.dataframe.api.explode
import org.jetbrains.kotlinx.dataframe.api.expr
import org.jetbrains.kotlinx.dataframe.api.filter
import org.jetbrains.kotlinx.dataframe.api.forEach
import org.jetbrains.kotlinx.dataframe.api.frameColumn
import org.jetbrains.kotlinx.dataframe.api.getColumnGroup
import org.jetbrains.kotlinx.dataframe.api.getColumnPath
import org.jetbrains.kotlinx.dataframe.api.getColumnWithPath
import org.jetbrains.kotlinx.dataframe.api.getColumns
import org.jetbrains.kotlinx.dataframe.api.getValue
import org.jetbrains.kotlinx.dataframe.api.group
import org.jetbrains.kotlinx.dataframe.api.groupBy
import org.jetbrains.kotlinx.dataframe.api.implode
import org.jetbrains.kotlinx.dataframe.api.indices
import org.jetbrains.kotlinx.dataframe.api.insert
import org.jetbrains.kotlinx.dataframe.api.into
import org.jetbrains.kotlinx.dataframe.api.intoRows
import org.jetbrains.kotlinx.dataframe.api.inward
import org.jetbrains.kotlinx.dataframe.api.isColumnGroup
import org.jetbrains.kotlinx.dataframe.api.isEmpty
import org.jetbrains.kotlinx.dataframe.api.isFrameColumn
import org.jetbrains.kotlinx.dataframe.api.join
import org.jetbrains.kotlinx.dataframe.api.last
import org.jetbrains.kotlinx.dataframe.api.map
import org.jetbrains.kotlinx.dataframe.api.max
import org.jetbrains.kotlinx.dataframe.api.maxBy
import org.jetbrains.kotlinx.dataframe.api.median
import org.jetbrains.kotlinx.dataframe.api.move
import org.jetbrains.kotlinx.dataframe.api.moveTo
import org.jetbrains.kotlinx.dataframe.api.moveToEnd
import org.jetbrains.kotlinx.dataframe.api.moveToStart
import org.jetbrains.kotlinx.dataframe.api.pathOf
import org.jetbrains.kotlinx.dataframe.api.perRowCol
import org.jetbrains.kotlinx.dataframe.api.pivot
import org.jetbrains.kotlinx.dataframe.api.remove
import org.jetbrains.kotlinx.dataframe.api.rename
import org.jetbrains.kotlinx.dataframe.api.rows
import org.jetbrains.kotlinx.dataframe.api.select
import org.jetbrains.kotlinx.dataframe.api.single
import org.jetbrains.kotlinx.dataframe.api.sortBy
import org.jetbrains.kotlinx.dataframe.api.split
import org.jetbrains.kotlinx.dataframe.api.sumOf
import org.jetbrains.kotlinx.dataframe.api.toColumnAccessor
import org.jetbrains.kotlinx.dataframe.api.toTop
import org.jetbrains.kotlinx.dataframe.api.under
import org.jetbrains.kotlinx.dataframe.api.ungroup
import org.jetbrains.kotlinx.dataframe.api.update
import org.jetbrains.kotlinx.dataframe.api.values
import org.jetbrains.kotlinx.dataframe.api.with
import org.jetbrains.kotlinx.dataframe.api.withNull
import org.jetbrains.kotlinx.dataframe.api.xs
import org.jetbrains.kotlinx.dataframe.columns.ColumnGroup
import org.jetbrains.kotlinx.dataframe.columns.ColumnKind
import org.jetbrains.kotlinx.dataframe.columns.FrameColumn
import org.jetbrains.kotlinx.dataframe.columns.depth
import org.jetbrains.kotlinx.dataframe.hasNulls
import org.junit.Test
import kotlin.reflect.typeOf

class DataFrameTreeTests : BaseTest() {

    @DataSchema
    interface NameAndCity {
        val name: String
        val city: String?
    }

    @DataSchema
    interface GroupedPerson {
        val nameAndCity: DataRow<NameAndCity>
        val age: Int
        val weight: Int?
    }

    val df2 = df.move { name and city }.under("nameAndCity")
    val typed2 = df2.cast<GroupedPerson>()

    val nameAndCity by columnGroup()
    val nameInGroup = nameAndCity.column<String>("name")

    @Test
    fun create() {
        val nameAndCity by columnOf(typed.name, typed.city)
        val df3 = dataFrameOf(nameAndCity, typed.age, typed.weight)
        df3 shouldBe df2
    }

    @Test
    fun createFrameColumn() {
        val rowsColumn by columnOf(typed[0..3], typed[4..5], typed[6..6])
        val df = dataFrameOf(rowsColumn).asGroupBy { rowsColumn }
        val res = df.concat()
        res shouldBe typed
    }

    @Test
    fun createFrameColumn2() {
        val id by column(typed.indices())
        val groups by id.map { typed[it..it] }
        val df = dataFrameOf(id, groups)
        df.rowsCount() shouldBe typed.rowsCount()
        df.forEach {
            val rowId = it[id]
            groups() shouldBe typed[rowId..rowId]
        }
    }

    @Test
    fun `select atAnyDepth under group`() {
        df2.select { nameAndCity.colsAtAnyDepth().colsOf<String>() } shouldBe typed2.select { nameAndCity.name }
        df2.select { nameAndCity.colsAtAnyDepth().colsOf<String?>() } shouldBe
            typed2.select { nameAndCity.name and nameAndCity.city }
    }

    @Test
    fun `selects`() {
        df2.select { nameAndCity.cols() } shouldBe typed2.nameAndCity.select { all() }
        df2.select { nameAndCity.cols { !it.hasNulls() } } shouldBe typed2.select { nameAndCity.name }
        df2.select { nameAndCity.cols(0..1) } shouldBe typed2.nameAndCity.select { all() }
        df2.select { nameAndCity.col(1) } shouldBe typed2.select { nameAndCity.city }
        df2.select { nameAndCity["city"] } shouldBe typed2.select { nameAndCity.city }
        df2.select { nameAndCity.cols("city", "name") } shouldBe typed2.select { nameAndCity.city and nameAndCity.name }
        df2.select { nameAndCity.cols(name, city) } shouldBe typed2.select { nameAndCity.allCols() }
        df2.select { nameAndCity[name] } shouldBe typed2.nameAndCity.select { name }
        df2.select { nameAndCity.cols().drop(1) } shouldBe typed2.nameAndCity.select { city }

        typed2.select { nameAndCity.cols() } shouldBe typed2.nameAndCity.select { all() }
        typed2.select { nameAndCity.cols { !it.hasNulls() } } shouldBe typed2.select { nameAndCity.name }
        typed2.select { nameAndCity.cols(0..1) } shouldBe typed2.nameAndCity.select { all() }
        typed2.select { nameAndCity.col(1) } shouldBe typed2.select { nameAndCity.city }
        typed2.select { nameAndCity["city"] } shouldBe typed2.select { nameAndCity.city }
        typed2.select { nameAndCity.cols("city", "name") } shouldBe
            typed2.select { nameAndCity.city and nameAndCity.name }
        typed2.select { nameAndCity.cols(name, city) } shouldBe typed2.select { nameAndCity.allCols() }
        typed2.select { nameAndCity[name] } shouldBe typed2.nameAndCity.select { name }
        typed2.select { nameAndCity.cols().drop(1) } shouldBe typed2.nameAndCity.select { city }

        df2.select { col(1) } shouldBe typed2.select { age }
        df2.select { nameInGroup } shouldBe typed2.nameAndCity.select { name }

        df2[nameInGroup] shouldBe typed2.nameAndCity.name
    }

    @Test
    fun getColumnPath() {
        typed2.getColumnPath { nameAndCity["city"] }.size shouldBe 2
        typed2.getColumnPath { nameAndCity.col(1) }.size shouldBe 2
    }

    @Test
    fun `group indexing`() {
        df2[nameAndCity][city] shouldBe typed.city
        typed2.nameAndCity.city shouldBe typed.city
        df2["nameAndCity"]["city"] shouldBe typed.city
    }

    @Test
    fun `convert column group`() {
        val expected = typed.select { city.rename("nameAndCity") and age and weight }

        df2.convert { nameAndCity }.with { it[city] } shouldBe expected
        df2.convert { nameAndCity }.with { this[nameAndCity][city] } shouldBe expected
        typed2.convert { nameAndCity }.with { nameAndCity.city } shouldBe expected
        typed2.convert { nameAndCity }.with { it.city } shouldBe expected
    }

    @Test
    fun `slice`() {
        val expected = typed[0..2].name
        val actual = typed2[0..2].nameAndCity.name
        actual shouldBe expected
    }

    @Test
    fun `filter`() {
        val expected = typed.filter { city == null }.select { weight }
        typed2.filter { nameAndCity.city == null }.select { weight } shouldBe expected
        df2.filter { it[nameAndCity][city] == null }.select { weight } shouldBe expected
    }

    @Test
    fun `select`() {
        val expected = typed.select { name and age }
        typed2.select { nameAndCity.name and age } shouldBe expected
        df2.select { it[nameAndCity][name] and age } shouldBe expected
    }

    @Test
    fun `sort`() {
        val expected = typed.sortBy { name and age }.moveTo(1) { city }
        typed2.sortBy { nameAndCity.name and age }.ungroup { nameAndCity } shouldBe expected
    }

    @Test
    fun `move`() {
        val actual = typed2.move { nameAndCity.name }.into("name")
        actual.columnNames() shouldBe listOf("nameAndCity", "name", "age", "weight")
        actual.getColumnGroup("nameAndCity").columnNames() shouldBe listOf("city")
    }

    @Test
    fun `groupBy`() {
        val expected = typed.groupBy { name }.max { age }
        typed2.groupBy { nameAndCity.name }.max { age } shouldBe expected
    }

    @Test
    fun `distinct`() {
        val duplicated = typed2.concat(typed2)
        duplicated.rowsCount() shouldBe typed2.rowsCount() * 2
        val dist = duplicated.nameAndCity.distinct()
        dist shouldBe typed2.nameAndCity.distinct()
        dist.rowsCount() shouldBe typed2.rowsCount() - 1
    }

    @Test
    fun `select atAnyDepth`() {
        val cols = typed2.select { colsAtAnyDepth { it.hasNulls } }
        cols shouldBe typed2.select { nameAndCity.city and weight }
    }

    @Test
    fun `get child column by accessor`() {
        val cityCol by column<String?>("city")
        val selected = typed2.getColumnWithPath {
            val g = nameAndCity
            val c = g.get(cityCol)
            c
        }
        selected.path shouldBe pathOf("nameAndCity", "city")
    }

    @Test
    fun splitRows() {
        val selected = typed2.select { nameAndCity }
        val nested = selected.implode(dropNA = false) { nameAndCity.city }
        val mergedCity = column<List<String?>>("city")
        val res = nested.split { nameAndCity[mergedCity] }.intoRows()
        val expected = selected.sortBy { nameAndCity.name }
        val actual = res.sortBy { nameAndCity.name }
        actual shouldBe expected
    }

    @Test
    fun pivot() {
        val modified = df.append("Alice", 55, "Moscow", 100)
        val df2 = modified.move { name and city }.under("nameAndCity")
        val typed2 = df2.cast<GroupedPerson>()

        fun <T, G, R> GroupBy<T, G>.map(body: Selector<GroupWithKey<T, G>, R>): List<R> =
            keys.rows().mapIndexedNotNull { index, row ->
                val group = groups[index]
                val g = GroupWithKey(row, group)
                body(g, g)
            }

        val expected = modified.cast<Person>()
            .groupBy { name and city }.map {
                val value = if (key.city == "Moscow") {
                    group.age.toList()
                } else {
                    group.age[0]
                }
                (key.name to key.city.toString()) to value
            }
            .plus("Bob" to "Moscow" to emptyList<Int>())
            .toMap()

        fun <T> DataFrame<T>.check() {
            columnsCount() shouldBe 2
            val cities = getColumnGroup("nameAndCity").getColumnGroup("city")
            cities.columnsCount() shouldBe typed2.nameAndCity.city.countDistinct()
            this[name] shouldBe typed.name.distinct()
            val data = cities.columns()
            data.forEach {
                if (it.name() == "Moscow") {
                    it.type() shouldBe typeOf<List<Int>>()
                } else {
                    it.type() shouldBe typeOf<Int?>()
                }
            }

            val actual = data.flatMap { col ->
                val city = col.name()
                rows().map { (it[name] to city) to col[it.index()] }.filter { it.second != null }
            }.toMap()
            actual shouldBe expected
        }

        typed2.pivot { nameAndCity.city }.groupBy { nameAndCity.name }.values { age }.check()
        df2.pivot(nameAndCity[city]).groupBy { nameAndCity[name] }.values(age).check()
        df2.pivot { it[GroupedPerson::nameAndCity][NameAndCity::city] }.groupBy {
            it[GroupedPerson::nameAndCity][NameAndCity::name]
        }.values(GroupedPerson::age).check()
        df2.pivot { it["nameAndCity"]["city"] }.groupBy { it["nameAndCity"]["name"] }.values("age").check()
    }

    @Test
    fun `pivot grouped column`() {
        val grouped = typed.group { age and weight }.into("info")
        val pivoted = grouped.pivot { city }.groupBy { name }.values("info")
        pivoted.columnsCount() shouldBe 2

        val expected = typed
            .rows()
            .groupBy { it.name to (it.city ?: "null") }
            .mapValues { it.value.map { it.age to it.weight } }
        val dataCols = pivoted.getColumns { col(1).asColumnGroup().allCols() }

        dataCols.forEach { (it.isColumnGroup() || it.isFrameColumn()) shouldBe true }

        val names = pivoted.name
        dataCols.forEach { col ->
            val city = col.name()
            pivoted.indices().forEach { row ->
                val name = names[row]
                val value = col[row]
                val expValues = expected[name to city]
                when {
                    expValues == null -> when (value) {
                        null -> {
                        }

                        is AnyRow -> value.isEmpty() shouldBe true

                        is AnyFrame -> value.columnsCount() shouldBe 0
                    }

                    expValues.size == 1 -> {
                        value shouldNotBe null
                        val single =
                            if (value is AnyRow) {
                                value
                            } else if (value is AnyFrame) {
                                value[0]
                            } else {
                                fail("invalid value type")
                            }
                        single.columnsCount() shouldBe 2
                        single.getValue<Int>("age") to single.getValue<Int?>("weight") shouldBe expValues[0]
                    }

                    else -> {
                        val df = value as? AnyFrame
                        df shouldNotBe null
                        df!!.rows()
                            .map { it["age"] as Int to it["weight"] as Int? }
                            .sortedBy { it.first } shouldBe expValues.sortedBy { it.first }
                    }
                }
            }
        }
    }

    @Test
    fun splitCols() {
        val split = typed2.split { nameAndCity.name }.by { it.toCharArray().toList() }.inward { "char$it" }
        split.columnNames() shouldBe typed2.columnNames()
        split.rowsCount() shouldBe typed2.rowsCount()
        split.nameAndCity.columnNames() shouldBe typed2.nameAndCity.columnNames()
        val nameGroup = split.nameAndCity.name.asColumnGroup()
        nameGroup.name() shouldBe "name"
        nameGroup.columnsCount() shouldBe
            typed2.nameAndCity.name
                .map { it.length }
                .max()
        nameGroup.columnNames() shouldBe (1..nameGroup.columnsCount()).map { "char$it" }
    }

    @Test
    fun `split into rows`() {
        val split = typed2.split { nameAndCity.name }.by { it.toCharArray().toList() }.intoRows()
        val merged = split.implode { nameAndCity.name }
        val joined = merged.convert { nameAndCity.name }.cast<List<Char>>().with { it.joinToString("") }
        joined shouldBe typed2
    }

    @Test
    fun `all except`() {
        val info by columnGroup()
        val moved = typed.group { allExcept(name) }.into(info)
        val actual = moved.select { allExcept(info) }
        actual shouldBe typed.select { name }
    }

    @Test
    fun `move and group`() {
        val info by columnGroup()
        val moved = typed.group { allExcept(name) }.into(info)
        val grouped = moved.groupBy { allExcept(info) }.toDataFrame()
        grouped.rowsCount() shouldBe typed.name.countDistinct()
    }

    @Test
    fun `merge rows into table`() {
        val info by columnGroup()
        val moved = typed.group { allExcept(name) }.into(info)
        val merged = moved.implode { info }
        val grouped = typed.groupBy { name }.updateGroups { remove { name } }
        val expected = grouped.toDataFrame().rename(grouped.groups).into(info)
        merged shouldBe expected
    }

    @Test
    fun `update grouped column to table`() {
        val info by columnGroup()
        val grouped = typed.group { age and weight }.into(info)
        val updated = grouped.convert(info).perRowCol { row, column -> column.asColumnGroup().asDataFrame() }
        val col = updated[info.name()]
        col.kind() shouldBe ColumnKind.Frame
        val table = col as FrameColumn<*>
        table.schema.value.columns
            .map { it.key }
            .sorted() shouldBe
            typed
                .select { age and weight }
                .columnNames()
                .sorted()
    }

    @Test
    fun extensionPropertiesTest() {
        val code = CodeGenerator.create()
            .generate<GroupedPerson>(
                interfaceMode = InterfaceGenerationMode.None,
                extensionProperties = true,
            ).declarations
        val columnsContainer = ColumnsContainer::class.qualifiedName
        val dataFrameRowBase = DataRow::class.qualifiedName
        val dataFrameRow = DataRow::class.qualifiedName
        val className = GroupedPerson::class.qualifiedName
        val shortName = GroupedPerson::class.simpleName!!
        val nameAndCity = NameAndCity::class.qualifiedName
        val groupedColumn = ColumnGroup::class.qualifiedName
        val columnData = DataColumn::class.qualifiedName
        val expected =
            """
            val $columnsContainer<$className>.age: $columnData<kotlin.Int> @JvmName("${shortName}_age") get() = this["age"] as $columnData<kotlin.Int>
            val $dataFrameRowBase<$className>.age: kotlin.Int @JvmName("${shortName}_age") get() = this["age"] as kotlin.Int
            val $columnsContainer<$className?>.age: $columnData<kotlin.Int?> @JvmName("Nullable${shortName}_age") get() = this["age"] as $columnData<kotlin.Int?>
            val $dataFrameRowBase<$className?>.age: kotlin.Int? @JvmName("Nullable${shortName}_age") get() = this["age"] as kotlin.Int?
            val $columnsContainer<$className>.nameAndCity: $groupedColumn<$nameAndCity> @JvmName("${shortName}_nameAndCity") get() = this["nameAndCity"] as $groupedColumn<$nameAndCity>
            val $dataFrameRowBase<$className>.nameAndCity: $dataFrameRow<$nameAndCity> @JvmName("${shortName}_nameAndCity") get() = this["nameAndCity"] as $dataFrameRow<$nameAndCity>
            val $columnsContainer<$className?>.nameAndCity: $groupedColumn<$nameAndCity?> @JvmName("Nullable${shortName}_nameAndCity") get() = this["nameAndCity"] as $groupedColumn<$nameAndCity?>
            val $dataFrameRowBase<$className?>.nameAndCity: $dataFrameRow<$nameAndCity?> @JvmName("Nullable${shortName}_nameAndCity") get() = this["nameAndCity"] as $dataFrameRow<$nameAndCity?>
            val $columnsContainer<$className>.weight: $columnData<kotlin.Int?> @JvmName("${shortName}_weight") get() = this["weight"] as $columnData<kotlin.Int?>
            val $dataFrameRowBase<$className>.weight: kotlin.Int? @JvmName("${shortName}_weight") get() = this["weight"] as kotlin.Int?
            val $columnsContainer<$className?>.weight: $columnData<kotlin.Int?> @JvmName("Nullable${shortName}_weight") get() = this["weight"] as $columnData<kotlin.Int?>
            val $dataFrameRowBase<$className?>.weight: kotlin.Int? @JvmName("Nullable${shortName}_weight") get() = this["weight"] as kotlin.Int?
            """.trimIndent()
        code shouldBe expected
    }

    @Test
    fun parentColumnTest() {
        val res = typed2.move { colsAtAnyDepth { it.depth > 0 } }.toTop { it.parentName + "-" + it.name }
        res.columnsCount() shouldBe 4
        res.columnNames() shouldBe listOf("nameAndCity-name", "nameAndCity-city", "age", "weight")
    }

    @Test
    fun `group cols`() {
        val joined = typed2.move { colsAtAnyDepth { !it.isColumnGroup() } }.into { pathOf(it.path.joinToString(".")) }
        val grouped = joined.group { nameContains(".") }.into { it.name().substringBefore(".") }
        val expected = typed2.rename { nameAndCity.allCols() }.into { it.path.joinToString(".") }
        grouped shouldBe expected
    }

    @Test
    fun `group into column`() {
        val grouped = typed2.group { age }.into { nameAndCity }
        grouped.nameAndCity.columnsCount() shouldBe 3
        grouped.columnsCount() shouldBe 2
    }

    @Test
    fun rename() {
        val res = typed2.rename { nameAndCity.allCols() }.into { it.name().capitalize() }
        res.nameAndCity.columnNames() shouldBe typed2.nameAndCity.columnNames().map { it.capitalize() }
    }

    @Test
    fun moveAfter() {
        val moved = typed2.move { age }.after { nameAndCity.name }
        moved.columnsCount() shouldBe 2
        moved.nameAndCity.columnsCount() shouldBe 3
        moved.nameAndCity.select { all() } shouldBe dataFrameOf(
            typed2.nameAndCity.name,
            typed2.age,
            typed2.nameAndCity.city,
        )
    }

    @Test
    fun moveAfter2() {
        val moved = typed2.move { nameAndCity.name }.after { age }
        moved.columnsCount() shouldBe 4
        moved.nameAndCity.columnsCount() shouldBe 1
        moved.remove { nameAndCity } shouldBe typed2.select { age and nameAndCity.name and weight }
    }

    @Test
    fun splitFrameColumnsIntoRows() {
        val grouped = typed.groupBy { city }
        val groupCol = grouped.groups.name()
        val plain = grouped.toDataFrame()
        val res = plain
            .split(grouped.groups).intoRows()
            .remove { it[groupCol]["city"] }
            .ungroup(groupCol)
            .sortBy { name and age }
        res shouldBe typed.sortBy { name and age }.moveToStart { city }
    }

    @Test
    fun splitFrameColumnIntoColumns() {
        val grouped = typed.groupBy { city }
        val groupCol = grouped.groups.name()
        val plain = grouped.toDataFrame()
        val res = plain
            .split(grouped.groups).intoRows()
            .remove { it[groupCol]["city"] }
            .ungroup(groupCol)
            .sortBy { name and age }
        res shouldBe typed.sortBy { name and age }.moveToStart { city }
    }

    @Test
    fun explodeFrameColumnWithNulls() {
        val grouped = typed.groupBy { city }
        val groupCol = grouped.groups.toColumnAccessor()
        val plain = grouped.toDataFrame()
            .update { groupCol }.at(1).withNull()
            .update { groupCol }.at(2).with { emptyDataFrame() }
            .update { groupCol }.at(3).with { it.filter { false } }
        val res = plain.explode(dropEmpty = false) { groupCol }
        val expected = plain[groupCol.name()].sumOf { Math.max((it as AnyFrame?)?.rowsCount() ?: 0, 1) }
        res.rowsCount() shouldBe expected
    }

    @Test
    fun `join with left path`() {
        val joined = typed2.remove { weight }.join(typed.remove { city }) { nameAndCity.name.match(right.name) and age }
        joined shouldBe typed2
    }

    @Test
    fun `join with right path`() {
        val joined = typed.remove { city }.join(typed2.remove { weight }) { name.match(right.nameAndCity.name) and age }
        val expected = typed.moveToEnd { city }.move { city }.under("nameAndCity")
        joined shouldBe expected
    }

    @Test
    fun `join by map column`() {
        val nameAndAge by columnGroup()
        val cityFirst by nameAndAge.column<Char?>()
        val grouped = typed.group { name and age }.into(nameAndAge).add(cityFirst) { city?.get(0) }
        grouped[nameAndAge].columnsCount() shouldBe 3

        val left = grouped.remove { weight }
        val right = grouped.remove { city }
        val joined = left.join(right) { nameAndAge }
        joined shouldBe grouped
    }

    @Test
    fun `join by frame column`() {
        val left = typed.groupBy { name }.updateGroups { it.remove { name and city } }
        val right =
            typed.update { name }.with { it.reversed() }
                .groupBy { name }.updateGroups { it.remove { name and city } }
        val groupCol = left.groups.toColumnAccessor()
        val joined = left.toDataFrame().join(right.toDataFrame()) { groupCol }
        joined.columnsCount() shouldBe 3
        val name1 by column<String>()
        joined.columnNames() shouldBe listOf(typed.name.name(), groupCol.name(), name1.name())
        joined[groupCol].kind() shouldBe ColumnKind.Frame
        joined.select { cols(0, 1) } shouldBe left.toDataFrame()
        joined.select { cols(2, 1) }.rename(name1).into(typed.name) shouldBe right.toDataFrame()
        joined.name shouldBe left.keys.name
        joined.forEach { it[name1] shouldBe it.name.reversed() }
    }

    @Test
    fun `add frame column`() {
        val frameCol by frameColumn()
        val added = typed2.add(frameCol) { nameAndCity.duplicate(3) }
        added[frameCol].kind() shouldBe ColumnKind.Frame
        added[frameCol].forEach { it.rowsCount() shouldBe 3 }
    }

    @Test
    fun `insert column`() {
        val colName = "reversed"

        fun DataFrame<GroupedPerson>.check() {
            nameAndCity.columnsCount() shouldBe 3
            nameAndCity.columnNames() shouldBe listOf(
                typed2.nameAndCity.name.name(),
                colName,
                typed2.nameAndCity.city.name(),
            )
        }

        typed2.insert(colName) { nameAndCity.name.reversed() }.after { nameAndCity.name }.check()
    }

    @Test
    fun append() {
        val res = typed2.append(listOf("Bill", "San Francisco"), null, 66)
        res.rowsCount() shouldBe typed2.rowsCount() + 1
        res.nameAndCity.last().values() shouldBe listOf("Bill", "San Francisco")
        res.age.hasNulls() shouldBe true
    }

    @Test
    fun `append nulls`() {
        val res = typed2.append(null, null, null)
        res.rowsCount() shouldBe typed2.rowsCount() + 1
        res.nameAndCity.last().values() shouldBe listOf(null, null)
        res.age.hasNulls() shouldBe true
        res.nameAndCity.name.hasNulls() shouldBe true
    }

    @Test
    fun `create data frame from map column`() {
        val df = dataFrameOf(typed.name, typed2.nameAndCity)
        df.rowsCount() shouldBe typed.rowsCount()
    }

    @Test
    fun `column group properties`() {
        typed2.nameAndCity.name() shouldBe "nameAndCity"
        val renamed = typed2.nameAndCity.rename("newName")
        renamed.name() shouldBe "newName"
        renamed.select { name } shouldBe typed2.select { nameAndCity.name }
        renamed.filter { name.startsWith("A") }.rowsCount() shouldBe typed.count { name.startsWith("A") }
    }

    @Test
    fun `distinct at column group`() {
        typed2.nameAndCity.distinct().filter { name.startsWith("A") }.columns() shouldBe
            typed
                .select { name and city }
                .distinct()
                .filter { name.startsWith("A") }
                .columns()
    }

    @Test
    fun `check column path`() {
        typed2.getColumnPath { nameAndCity.name }.size shouldBe 2
    }

    @Test
    fun `filter not null without arguments`() {
        typed2.dropNulls() shouldBe typed.dropNulls { weight }.group { name and city }.into("nameAndCity")
    }

    @Test
    fun `select group`() {
        val groupCol = typed2[nameAndCity]
        typed2.select { groupCol and age }.columnNames() shouldBe listOf("nameAndCity", "age")
    }

    @Test
    fun `select columns range`() {
        val added = typed2.move { age }.after { nameAndCity.name }
        val expected = typed2.select { nameAndCity.name and age and nameAndCity.city }

        added.select { nameAndCity.name..nameAndCity.city } shouldBe expected

        shouldThrow<IllegalArgumentException> {
            added.select { nameAndCity.name..weight }
        }

        shouldThrow<IllegalArgumentException> {
            added.select { weight..nameAndCity.name }
        }

        shouldThrow<IllegalArgumentException> {
            added.select { nameAndCity.city..nameAndCity.name }
        }

        added.select { nameAndCity.select { name..city } } shouldBe expected
    }

    @Test
    fun groupByAggregateSingleColumn() {
        val agg = typed2.groupBy { age }.aggregate { nameAndCity into "nameAndCity" }
        agg["nameAndCity"].kind() shouldBe ColumnKind.Frame
        typed2.groupBy { age }.aggregate { nameAndCity.asDataFrame() into "nameAndCity" } shouldBe agg
        typed2.groupBy { age }.values { nameAndCity } shouldBe agg
    }

    @Test
    fun `xs nested columns`() {
        typed2.xs("Bob", "Tokyo").rowsCount() shouldBe 1
    }

    @Test
    fun `duplicate dataframe`() {
        typed2.duplicate(2) shouldBe columnOf(typed2, typed2)
    }

    @Test
    fun `duplicate row`() {
        typed2[2].duplicate(2) shouldBe typed2[2, 2]
    }

    @Test
    fun `duplicate selected rows`() {
        typed2.duplicateRows(2) { nameAndCity.name == "Alice" } shouldBe typed2[0, 0, 1, 2, 3, 4, 5, 5, 6]
    }

    @Test
    fun `duplicate all rows`() {
        typed2.duplicateRows(2) shouldBe typed2.addId("id").let {
            it.concat(it).sortBy("id").remove("id")
        }
    }

    @Test
    fun `select column group`() {
        typed2.aggregate {
            nameAndCity()[2..3].name.distinct().single() into "name"
        }["name"] shouldBe "Charlie"
    }

    @Test
    fun `select frame column`() {
        val group by frameColumn<GroupedPerson>()

        typed2
            .groupBy { expr { age > 30 } into "isOld" }.into(group)
            .aggregate {
                group().maxBy { rowsCount() }.weight.median() into "m"
            }["m"] shouldBe 61
    }

    @Test
    fun `column group as DataFrame`() {
        val a: DataFrame<NameAndCity> = typed2["nameAndCity"].cast<DataRow<NameAndCity>>().asDataFrame()
        val b: DataFrame<NameAndCity> = typed2[nameAndCity].cast<NameAndCity>().asDataFrame()
    }

    @Test
    fun `convert ColumnGroup as Frame`() {
        val df = typed2.convert { nameAndCity }.asFrame { it.remove { city } }
        df.nameAndCity.columns() shouldBe typed2.nameAndCity.remove { city }.columns()
    }

    @Test
    fun `move under existing group`() {
        val df = typed2.move { age }.under { nameAndCity }
        df.nameAndCity.columnNames() shouldBe listOf("name", "city", "age")
    }
}
