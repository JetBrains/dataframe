[//]: # (title: Modify)

<tip> 

`DataFrame` object is immutable and all operations return a new instance of `DataFrame`.

</tip>

## Naming conventions

`DataFrame` is a columnar data structure and is more oriented to column-wise operations. Most transformation operations start with [column selector](ColumnSelectors.md) that selects target columns for the operation.
Syntax of most column operations assumes that they are applied to columns, so they don't include word `column` in their naming.    

On the other hand, `Kotlin dataframe` follows `Koltin Collections` naming for row-wise operations as `DataFrame` can be interpreted as a `Collection` of rows. The slight naming difference with `Kotlin Collection` is that all operations are named in imperative way: `sortBy`, `shuffle` etc. 

**Pairs of column/row operations:**
* [add](add.md) columns / [append](append.md) rows
* [remove](remove.md) columns / [drop](drop.md) rows
* [select](select.md) columns / [filter](filter.md) rows
* [group](group.md) for columns / [groupBy](groupBy.md) for rows
* [sortColumnsBy](sortColumnsBy.md) for columns / [sortBy](sortBy.md) for rows
* [join](join.md) to unite columns / [concat](concat.md) to unite rows

**Horizontal (column) operations:**
* [add](add.md) — add columns
* [flatten](flatten.md) — remove column groupings recursively
* [group](group.md) — group columns into [`ColumnGroup`](DataColumn.md#columngroup)
* [insert](insert.md) — insert column
* [map](map.md) — map columns into new [`DataFrame`](DataFrame.md) or [`DataColumn`](DataColumn.md)
* [merge](merge.md) — merge several columns into one
* [move](move.md) — move columns or change column groupings
* [remove](remove.md) — remove columns
* [rename](rename.md) — rename columns
* [replace](replace.md) — replace columns
* [select](select.md) — select subset of columns
* [sortColumnsBy](sortColumnsBy.md) — sort columns
* [split](split.md) — split values into new columns
* [ungroup](ungroup.md) — remove column grouping

**Vertical (row) operations:**
* [append](append.md) — add rows
* [concat](concat.md) — union rows from several dataframes
* [distinct](distinct.md) / [distinctBy](distinct.md#distinctby) — remove duplicated rows
* [drop](drop.md) / [dropLast](sliceRows.md#droplast) / [dropNulls](drop.md#dropnulls) / [dropNA](drop.md#dropna) — remove rows by condition
* [explode](explode.md) — spread lists and dataframes vertically into new rows
* [filter](filter.md) / [filterBy](filter.md#filterby) — filter rows
* [implode](implode.md) — merge column values into lists grouping by other columns
* [shuffle](shuffle.md) — reorder rows randomly
* [sortBy](sortBy.md) / [sortByDesc](sortBy.md#sortbydesc) / [sortWith](sortBy.md#sortwith) — sort rows
* [split](split.md) — split values into new rows

**Value modification:**
* [convert](convert.md) — convert values into new types
* [parse](parse.md) — try to convert `String` values into appropriate types
* [update](update.md) — update values preserving column types
* [fillNulls](fill.md#fillnulls) / [fillNaNs](fill.md#fillnans) / [fillNA](fill.md#fillna) — replace missing values

**Reshaping:**
* [pivot](pivot.md) / [pivotCounts](pivot.md#pivotcounts) / [pivotMatches](pivot.md#pivotmatches) — convert values into new columns
* [gather](gather.md) — convert pairs of column names and values into `key` and `value` columns

**Learn how to:**
* [Slice rows](sliceRows.md)
* [Filter rows](filterRows.md)
* [Reorder rows](reorderRows.md)
* [Select columns](select.md)
* [Update/convert values](updateConvert.md)
* [Split/merge values](splitMerge.md)
* [Group rows by keys](groupBy.md)
* [Append values](append.md)
* [Add/map/remove columns](addRemove.md)
* [Move/rename columns](moveRename.md)
* [Insert/replace columns](insertReplace.md)
* [Explode/implode columns](explodeImplode.md)
* [Pivot/gather columns](pivotGather.md)
