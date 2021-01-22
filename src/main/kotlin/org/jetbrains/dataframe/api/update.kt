package org.jetbrains.dataframe

import org.jetbrains.dataframe.api.columns.DataCol
import org.jetbrains.dataframe.impl.createDataCollector
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType

fun <T, C> DataFrame<T>.update(selector: ColumnsSelector<T, C>) = UpdateClause(this, null, selector, null, null)
fun <T, C> DataFrame<T>.update(cols: Iterable<ColumnDef<C>>) = update { cols.toColumnSet() }
fun <T> DataFrame<T>.update(vararg cols: String) = update { cols.toColumns() }
fun <T, C> DataFrame<T>.update(vararg cols: KProperty<C>) = update { cols.toColumns() }
fun <T, C> DataFrame<T>.update(vararg cols: ColumnDef<C>) = update { cols.toColumns() }

data class UpdateClause<T, C>(val df: DataFrame<T>, val filter: UpdateExpression<T, C, Boolean>?, val selector: ColumnsSelector<T, C>, val targetType: KType?, val typeSuggestions: ((KClass<*>) -> KType)?){
    fun <R> cast() = UpdateClause(df, filter as UpdateExpression<T, R, Boolean>?, selector as ColumnsSelector<T, R>, targetType, typeSuggestions)

    inline fun <reified R> toType() = UpdateClause(df, filter, selector, getType<R>(), typeSuggestions)
}

fun <T, C> UpdateClause<T, C>.where(predicate: UpdateExpression<T, C, Boolean>) = copy(filter = predicate)

fun <T, C> UpdateClause<T, C>.guessTypes() = copy(targetType = null) { it.createStarProjectedType(false) }

fun <T, C> UpdateClause<T, C>.toType(type: KType) = copy(targetType = type)

fun <T, C> UpdateClause<T, C>.suggestTypes(vararg suggestions: Pair<KClass<*>, KType>): UpdateClause<T, C> {
    val map = suggestions.toMap()
    return copy(targetType = null) { map[it] ?: it.createStarProjectedType(false) }
}

typealias UpdateExpression<T, C, R> = DataRow<T>.(C) -> R

typealias UpdateByColumnExpression<T, C, R> = (DataRow<T>, DataCol<C>) -> R

fun <T, C, R> doUpdate(clause: UpdateClause<T, C>, expression: (DataRow<T>, DataCol<C>) -> R): DataFrame<T> {

    val removeResult = clause.df.doRemove(clause.selector)

    val toInsert = removeResult.removedColumns.map {
        val srcColumn = it.data.column as DataCol<C>
        val collector = if (clause.typeSuggestions != null) createDataCollector(clause.df.nrow(), clause.typeSuggestions) else createDataCollector(clause.df.nrow(), clause.targetType!!)
        if(clause.filter == null)
            clause.df.forEach { row ->
                collector.add(expression(row, srcColumn))
            }
        else
            clause.df.forEach { row ->
                val currentValue = srcColumn[row.index]
                val newValue = if (clause.filter.invoke(row, currentValue)) expression(row, srcColumn) else currentValue as R
                collector.add(newValue)
            }

        val newColumn = collector.toColumn(srcColumn.name())

        ColumnToInsert(it.pathFromRoot(), it, newColumn)
    }
    return removeResult.df.doInsert(toInsert)
}

// TODO: rename
inline infix fun <T, C, reified R> UpdateClause<T, C>.with2(noinline expression: UpdateByColumnExpression<T, C, R>) = doUpdate(copy(targetType = targetType ?: getType<R>()), expression)

inline infix fun <T, C, reified R> UpdateClause<T, C>.with(noinline expression: UpdateExpression<T, C, R>) = doUpdate(copy(filter = null, targetType = targetType ?: getType<R>())) { row, column ->
    val currentValue = column[row.index]
    if (filter?.invoke(row, currentValue) == false)
        currentValue as R
    else expression(row, currentValue)
}

inline fun <T, C, reified R> UpdateClause<T, C?>.notNull(noinline expression: UpdateExpression<T, C, R>) = doUpdate(copy(filter = null, targetType =  targetType ?: getType<R>())) { row, column ->
    val currentValue = column[row.index]
    if (currentValue == null)
        null
    else expression(row, currentValue)
}

inline fun <T, C, reified R> DataFrame<T>.update(firstCol: ColumnDef<C>, vararg cols: ColumnDef<C>, noinline expression: UpdateExpression<T, C, R>) =
        update(*headPlusArray(firstCol, cols)).with(expression)

inline fun <T, C, reified R> DataFrame<T>.update(firstCol: KProperty<C>, vararg cols: KProperty<C>, noinline expression: UpdateExpression<T, C, R>) =
        update(*headPlusArray(firstCol, cols)).with(expression)

inline fun <T, reified R> DataFrame<T>.update(firstCol: String, vararg cols: String, noinline expression: UpdateExpression<T, Any?, R>) =
        update(*headPlusArray(firstCol, cols)).with(expression)

fun <T, C> UpdateClause<T, C>.withNull() = with { null as Any? }
inline infix fun <T, C, reified R> UpdateClause<T, C>.with(value: R) = with { value }
