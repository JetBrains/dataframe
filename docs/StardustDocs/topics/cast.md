[//]: # (title: cast)
<!---IMPORT org.jetbrains.kotlinx.dataframe.samples.api.Modify-->

Changes the type argument of the [`DataFrame`](DataFrame.md) instance without changing its contents.

```kotlin
cast<T>(verify = false)
```

**Parameters:**
* `verify: Boolean = false` —
  when `true`, the function throws an exception if the [`DataFrame`](DataFrame.md) instance doesn't match the given schema. 
Otherwise, it just changes the format type without actual data checks.

Use this operation to change the formal type of a [`DataFrame`](DataFrame.md) instance
to match the expected schema and enable generated [extension properties](extensionPropertiesApi.md) for it.

```kotlin
@DataSchema
interface Person {
    val age: Int
    val name: String
}

df.cast<Person>()
```

To convert [`DataFrame`](DataFrame.md) columns to match given schema, use [`convertTo`](convertTo.md) operation.
