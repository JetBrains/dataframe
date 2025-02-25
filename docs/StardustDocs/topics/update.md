[//]: # (title: update)

<!---IMPORT org.jetbrains.kotlinx.dataframe.samples.api.Modify-->

Returns [`DataFrame`](DataFrame.md) with changed values in some cells. Column types can not be changed.

```text
update { columns }
    [.where { rowCondition } ]
    [.at(rowIndices) ] 
     .with { rowExpression } | .notNull { rowExpression } | .perCol { colExpression } | .perRowCol { rowColExpression } | .withNull() | .withZero() | .asFrame { frameExpression } 

rowCondition: DataRow.(OldValue) -> Boolean
rowExpression: DataRow.(OldValue) -> NewValue
colExpression: DataColumn.(DataColumn) -> NewValue
rowColExpression: (DataRow, DataColumn) -> NewValue
frameExpression: DataFrame.(DataFrame) -> DataFrame
```

See [column selectors](ColumnSelectors.md) and [row expressions](DataRow.md#row-expressions)

<!---FUN update-->

```kotlin
df.update { age }.with { it * 2 }
df.update { colsAtAnyDepth().colsOf<String>() }.with { it.uppercase() }
df.update { weight }.at(1..4).notNull { it / 2 }
df.update { name.lastName and age }.at(1, 3, 4).withNull()
```

<dataFrame src="org.jetbrains.kotlinx.dataframe.samples.api.Modify.update.html"/>
<!---END-->

Update with constant value:

<!---FUN updateWithConst-->

```kotlin
df.update { city }.where { name.firstName == "Alice" }.with { "Paris" }
```

<dataFrame src="org.jetbrains.kotlinx.dataframe.samples.api.Modify.updateWithConst.html"/>
<!---END-->

Update with mapping:

<!---FUN replaceAll-->

```kotlin
functions.replaceAll(
    "DataFrame<*>" to "DataFrame",
    "DataRow<*>" to "DataRow",
    "AnyRow" to "DataRow",
) { receiverType and returnType }
```

<dataFrame src="org.jetbrains.kotlinx.dataframe.samples.api.Modify.replaceAll.html"/>
<!---END-->

Update with value depending on row:

<!---FUN updateWith-->

```kotlin
df.update { city }.with { name.firstName + " from " + it }
```

<dataFrame src="org.jetbrains.kotlinx.dataframe.samples.api.Modify.updateWith.html"/>
<!---END-->

Update with value depending on column:

<!---FUN updatePerColumn-->

```kotlin
df.update { colsOf<Number?>() }.perCol { mean(skipNA = true) }
```

<!---END-->

Update with value depending on row and column:

<!---FUN updatePerRowCol-->

```kotlin
df.update { colsOf<String?>() }.perRowCol { row, col -> col.name() + ": " + row.index() }
```

<dataFrame src="org.jetbrains.kotlinx.dataframe.samples.api.Modify.updatePerRowCol.html"/>
<!---END-->

Update [ColumnGroup](DataColumn.md#columngroup) as [DataFrame](DataFrame.md):

<!---FUN updateAsFrame-->

```kotlin
df.update { name }.asFrame { select { lastName } }
```

<dataFrame src="org.jetbrains.kotlinx.dataframe.samples.api.Modify.updateAsFrame.html"/>
<!---END-->
