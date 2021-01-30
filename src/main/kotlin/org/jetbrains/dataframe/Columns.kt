package org.jetbrains.dataframe

import org.jetbrains.dataframe.api.columns.*
import org.jetbrains.dataframe.impl.columns.ColumnWithPathImpl
import org.jetbrains.dataframe.impl.columns.ConvertedColumnDef
import org.jetbrains.dataframe.impl.createDataCollector
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.full.starProjectedType
import kotlin.reflect.full.withNullability
import kotlin.reflect.typeOf

enum class UnresolvedColumnsPolicy { Fail, Skip, Create }

class ColumnResolutionContext(val df: DataFrameBase<*>, val unresolvedColumnsPolicy: UnresolvedColumnsPolicy) {

    val allowMissingColumns = unresolvedColumnsPolicy == UnresolvedColumnsPolicy.Skip
}

internal fun <C> DataFrameBase<*>.getColumn(name: String, policy: UnresolvedColumnsPolicy) =
        tryGetColumn(name)?.typed()
                ?: when (policy) {
                    UnresolvedColumnsPolicy.Fail -> throw Exception("Column not found: $this")
                    UnresolvedColumnsPolicy.Skip -> null
                    UnresolvedColumnsPolicy.Create -> DataColumn.empty().typed<C>()
                }

internal val ColumnReference<*>.name get() = name()

interface ColumnReference<out C> : SingleColumn<C> {

    fun name(): String

    operator fun invoke(row: AnyRow) = row[this]

    override fun resolveSingle(context: ColumnResolutionContext): ColumnWithPath<C>? {
        return context.df.getColumn<C>(name(), context.unresolvedColumnsPolicy)?.addPath(listOf(name()))
    }
}

fun <TD, T: DataFrameBase<TD>, C> Selector<T, ColumnSet<C>>.toColumns(createReceiver: (ColumnResolutionContext) -> T) = createColumnSet {
    val receiver = createReceiver(it)
    val columnSet = this(receiver, receiver)
    columnSet.resolve(ColumnResolutionContext(receiver, it.unresolvedColumnsPolicy))
}

fun <C> createColumnSet(resolver: (ColumnResolutionContext) -> List<ColumnWithPath<C>>): ColumnSet<C> = ColumnsBySelector(resolver)

internal class ColumnsBySelector<C>(val resolver: (ColumnResolutionContext) -> List<ColumnWithPath<C>>) : ColumnSet<C> {

    override fun resolve(context: ColumnResolutionContext) = resolver(context)
}

inline fun <C, reified R> ColumnReference<C>.map(noinline transform: (C) -> R): SingleColumn<R> = map(getType<R>(), transform)

fun <C, R> ColumnReference<C>.map(targetType: KType?, transform: (C) -> R): SingleColumn<R> = ConvertedColumnDef(this, transform, targetType)

typealias Column = ColumnReference<*>

typealias MapColumnReference = ColumnReference<AnyRow>

fun String.toColumnDef(): ColumnReference<Any?> = ColumnDefinition(this)

internal fun KProperty<*>.getColumnName() = this.findAnnotation<ColumnName>()?.name ?: name

fun <T> KProperty<T>.toColumnDef() = ColumnDefinition<T>(name)

class ColumnDefinition<T>(val name: String) : ColumnReference<T> {

    override fun name() = name

    operator fun getValue(thisRef: Any?, property: KProperty<*>) = this

    fun <C> changeType() = this as ColumnDefinition<C>
}

inline fun <reified T> ColumnDefinition<T>.nullable() = ColumnDefinition<T?>(name())

interface NestedColumn<out T> {
    val df: DataFrame<T>
}

typealias ColumnPath = List<String>

internal fun ColumnPath.depth() = size - 1

internal fun <T> DataColumn<T>.addPath(path: ColumnPath): ColumnWithPath<T> = ColumnWithPathImpl(this, path)

internal fun <T> DataColumn<T>.addPath(): ColumnWithPath<T> = ColumnWithPathImpl(this, listOf(name()))

enum class ColumnKind {
    Value,
    Map,
    Frame
}

internal fun <T> Iterable<T>.equalsByElement(other: Iterable<T>): Boolean {
    val iterator1 = iterator()
    val iterator2 = other.iterator()
    while (iterator1.hasNext() && iterator2.hasNext()) {
        if (iterator1.next() != iterator2.next()) return false
    }
    if (iterator1.hasNext() || iterator2.hasNext()) return false
    return true
}

internal fun <T> Iterable<T>.rollingHash(): Int {
    val i = iterator()
    var hash = 0
    while (i.hasNext())
        hash = 31 * hash + (i.next()?.hashCode() ?: 5)
    return hash
}

internal fun <T> DataColumn<T>.checkEquals(other: Any?): Boolean {
    if (this === other) return true

    if (!(other is DataColumn<*>)) return false

    if (name() != other.name()) return false
    if (type != other.type) return false
    return values.equalsByElement(other.values)
}

internal fun <T> DataColumn<T>.getHashCode(): Int {
    var result = values.rollingHash()
    result = 31 * result + name().hashCode()
    result = 31 * result + type.hashCode()
    return result
}

@OptIn(ExperimentalStdlibApi::class)
inline fun <reified T> getType() = typeOf<T>()

val KType.fullName: String get() = toString()

fun KClass<*>.createStarProjectedType(nullable: Boolean) = this.starProjectedType.let { if (nullable) it.withNullability(true) else it }

inline fun <reified T> ColumnReference<T>.withValues(values: List<T>, hasNulls: Boolean) =
    column(name(), values, hasNulls)

// TODO: implement correct base schema computation
internal fun <T> Iterable<DataFrame<T>?>.getBaseSchema(): DataFrame<T> {
    return first { it != null && it.ncol() > 0 } ?: DataFrame.empty()
}

fun <T> DataColumn<T>.withValues(values: List<T>, hasNulls: Boolean) = when (this) {
    is FrameColumn<*> -> {
        val dfs = (values as List<AnyFrame>)
        DataColumn.createTable(name(), dfs, dfs.getBaseSchema()) as DataColumn<T>
    }
    else -> column(name(), values, type.withNullability(hasNulls))
}

fun AnyCol.toDataFrame() = dataFrameOf(listOf(this))

internal fun <T> AnyCol.typed() = this as DataColumn<T>

internal fun <T> AnyCol.asValues() = this as ValueColumn<T>

internal fun <T> ValueColumn<*>.typed() = this as ValueColumn<T>

internal fun <T> FrameColumn<*>.typed() = this as FrameColumn<T>

internal fun <T> MapColumn<*>.typed() = this as MapColumn<T>

internal fun <T> AnyCol.grouped() = this as org.jetbrains.dataframe.api.columns.ColumnGroup<T>

inline fun <reified T> AnyCol.cast(): DataColumn<T> = DataColumn.create(name(), toList() as List<T>, getType<T>().withNullability(hasNulls))

internal fun <T> MapColumn<*>.withDf(newDf: DataFrame<T>) = DataColumn.createGroup(name(), newDf)

internal fun <T> Iterable<T>.asList() = when (this) {
    is List<T> -> this
    else -> this.toList()
}

fun <C> DataColumn<C>.ensureUniqueName(nameGenerator: ColumnNameGenerator) = rename(nameGenerator.addUnique(name()))

class InplaceColumnBuilder(val name: String) {
    inline operator fun <reified T> invoke(vararg values: T) = column(name, values.toList())
}

fun column(name: String) = InplaceColumnBuilder(name)

inline fun <T, reified R> DataFrame<T>.newColumn(name: String, noinline expression: RowSelector<T, R>): DataColumn<R> {
    var nullable = false
    val values = (0 until nrow()).map { get(it).let { expression(it, it) }.also { if (it == null) nullable = true } }
    return column(name, values, nullable)
}

internal fun Array<out String>.toColumns(): ColumnSet<Any?> = map { it.toColumnDef() }.toColumnSet()
fun <C> Iterable<ColumnSet<C>>.toColumnSet(): ColumnSet<C> = Columns(asList())
internal fun <C> Array<out KProperty<C>>.toColumns() = map { it.toColumnDef() }.toColumnSet()
internal fun <T> Array<out ColumnReference<T>>.toColumns() = toList().toColumnSet()
internal fun <T, C> ColumnsSelector<T, C>.toColumns(): ColumnSet<C> = toColumns { SelectReceiverImpl(it.df.typed(), it.allowMissingColumns) }

@JvmName("toColumnSetForSort")
internal fun <T, C> SortColumnsSelector<T, C>.toColumns(): ColumnSet<C> = toColumns { SortReceiverImpl(it.df.typed(), it.allowMissingColumns) }


class Columns<C>(val columns: List<ColumnSet<C>>) : ColumnSet<C> {
    constructor(vararg columns: ColumnSet<C>) : this(columns.toList())

    override fun resolve(context: ColumnResolutionContext) = columns.flatMap { it.resolve(context) }
}

class ColumnDelegate<T> {
    operator fun getValue(thisRef: Any?, property: KProperty<*>) = ColumnDefinition<T>(property.name)
}

fun AnyCol.asFrame(): AnyFrame = when (this) {
    is MapColumn<*> -> df
    is ColumnWithPath<*> -> data.asFrame()
    else -> throw Exception()
}

fun AnyCol.isGroup(): Boolean = kind() == ColumnKind.Map

internal fun AnyCol.asGroup(): MapColumn<*> = this as MapColumn<*>

@JvmName("asGroupedT")
internal fun <T> DataColumn<DataRow<T>>.asGroup(): MapColumn<T> = this as MapColumn<T>

internal fun AnyCol.asTable(): FrameColumn<*> = this as FrameColumn<*>

@JvmName("asTableT")
internal fun <T> DataColumn<DataFrame<T>>.asTable(): FrameColumn<T> = this as FrameColumn<T>

internal fun AnyCol.isTable(): Boolean = kind() == ColumnKind.Frame


fun <T> column() = ColumnDelegate<T>()

fun mapColumn() = column<AnyRow>()

fun <T> columnList() = column<List<T>>()

fun <T> mapColumn(name: String) = column<DataRow<T>>(name)

fun <T> frameColumn(name: String) = column<DataFrame<T>>(name)

fun <T> columnList(name: String) = column<List<T>>(name)

fun <T> column(name: String) = ColumnDefinition<T>(name)

class DataColumnDelegate<T>(val values: List<T>, val type: KType) {
    operator fun getValue(thisRef: Any?, property: KProperty<*>) = column(property.name, values, type)
}

inline fun <reified T> column(vararg values: T) = DataColumnDelegate(values.toList(), getType<T>())

inline fun <reified T> column(name: String, values: List<T>): DataColumn<T> =
    column(name, values, values.any { it == null })

inline fun <reified T> column(name: String, values: List<T>, hasNulls: Boolean): DataColumn<T> = DataColumn.create(name, values, getType<T>().withNullability(hasNulls))

fun <T> column(name: String, values: List<T>, type: KType): DataColumn<T> = DataColumn.create(name, values, type)

class ColumnNameGenerator(columnNames: List<String> = emptyList()) {

    private val usedNames = columnNames.toMutableSet()

    private val colNames = columnNames.toMutableList()

    fun addUnique(preferredName: String): String {
        var name = preferredName
        var k = 2
        while (usedNames.contains(name)) {
            name = "${preferredName}_${k++}"
        }
        usedNames.add(name)
        colNames.add(name)
        return name
    }

    fun addIfAbsent(name: String) {
        if (!usedNames.contains(name)) {
            usedNames.add(name)
            colNames.add(name)
        }
    }

    val names: List<String>
        get() = colNames

    fun contains(name: String) = usedNames.contains(name)
}

fun AnyFrame.nameGenerator() = ColumnNameGenerator(columnNames())

fun <T, R> DataColumn<T>.map(transform: (T) -> R): DataColumn<R> {
    val collector = createDataCollector(size)
    values.forEach { collector.add(transform(it)) }
    return collector.toColumn(name()).typed()
}

fun <T, R> DataColumn<T>.map(type: KType?, transform: (T) -> R): DataColumn<R> {
    if (type == null) return map(transform)
    val collector = createDataCollector<R>(size, type)
    values.forEach { collector.add(transform(it)) }
    return collector.toColumn(name()) as DataColumn<R>
}

fun <C> DataColumn<C>.single() = values.single()

fun <T> FrameColumn<T>.toDefinition() = frameColumn<T>(name())
fun <T> MapColumn<T>.toDefinition() = mapColumn<T>(name())
fun <T> ValueColumn<T>.toDefinition() = column<T>(name())

internal abstract class MissingDataColumn<T> : DataColumn<T> {

    val name: String
        get() = throw UnsupportedOperationException()
    override val values: Iterable<T>
        get() = throw UnsupportedOperationException()
    override val ndistinct: Int
        get() = throw UnsupportedOperationException()
    override val type: KType
        get() = throw UnsupportedOperationException()
    override val size: Int
        get() = throw UnsupportedOperationException()

    override fun name() = name

    override fun get(index: Int) = throw UnsupportedOperationException()

    override fun defaultValue() = throw UnsupportedOperationException()

    override fun slice(range: IntRange) = throw UnsupportedOperationException()

    override fun get(columnName: String) = throw UnsupportedOperationException()

    override fun slice(indices: Iterable<Int>) = throw UnsupportedOperationException()

    override fun slice(mask: BooleanArray) = throw UnsupportedOperationException()

    override fun toSet() = throw UnsupportedOperationException()

    override fun resolve(context: ColumnResolutionContext) = emptyList<ColumnWithPath<T>>()
}

internal class MissingValueColumn<T> : MissingDataColumn<T>(), ValueColumn<T> {

    override fun distinct() = throw UnsupportedOperationException()
}

internal class MissingMapColumn<T> : MissingDataColumn<DataRow<T>>(), MapColumn<T> {

    override fun <R> get(column: ColumnReference<R>) = MissingValueColumn<R>()

    override fun <R> get(column: ColumnReference<DataRow<R>>) = MissingMapColumn<R>()

    override fun <R> get(column: ColumnReference<DataFrame<R>>) = MissingFrameColumn<R>()

    override val df: DataFrame<T>
        get() = throw UnsupportedOperationException()

    override fun tryGetColumn(columnName: String): AnyCol? {
        return null
    }

    override fun column(columnIndex: Int): AnyCol {
        return MissingValueColumn<Any?>()
    }

    override fun ncol(): Int = 0

    override fun get(index: Int) = throw UnsupportedOperationException()

    override fun get(columnName: String) = throw UnsupportedOperationException()

    override fun nrow(): Int = 0

    override fun columns(): List<AnyCol> = emptyList()

    override fun rows(): Iterable<DataRow<T>> = emptyList()

    override fun getColumnIndex(name: String) = -1

    override fun append(vararg values: Any?) = throw UnsupportedOperationException()

    override fun kind() = super.kind()

    override fun set(columnName: String, value: AnyCol) = throw UnsupportedOperationException()
}

internal class MissingFrameColumn<T>: MissingDataColumn<DataFrame<T>>(), FrameColumn<T> {
    override val df: DataFrame<T>
        get() = throw UnsupportedOperationException()


    override fun kind() = super.kind()

    override fun distinct(): FrameColumn<T> {
        throw UnsupportedOperationException()
    }
}

operator fun AnyCol.plus(other: AnyCol) = dataFrameOf(listOf(this, other))

typealias DoubleCol = DataColumn<Double?>
typealias IntCol = DataColumn<Int?>
typealias StringCol = DataColumn<String?>
typealias AnyCol = DataColumn<*>

internal fun <T> DataColumn<T>.assertIsComparable(): DataColumn<T> {
    if (!type.isSubtypeOf(getType<Comparable<*>?>()))
        throw RuntimeException("Column '$name' has type '$type' that is not Comparable")
    return this
}

internal class TransformedColumnSet<C>(val src: ColumnSet<C>, val transform: (List<ColumnWithPath<C>>) -> List<ColumnWithPath<C>>) : ColumnSet<C> {

    override fun resolve(context: ColumnResolutionContext) = transform(src.resolve(context))
}

internal fun <C> ColumnSet<C>.transform(transform: (List<ColumnWithPath<C>>) -> List<ColumnWithPath<C>>): ColumnSet<C> = TransformedColumnSet(this, transform)

fun StringCol.len() = map { it?.length }
fun StringCol.lower() = map { it?.toLowerCase() }
fun StringCol.upper() = map { it?.toUpperCase() }