package org.jetbrains.kotlinx.dataframe.io

import org.jetbrains.kotlinx.dataframe.codeGen.FieldType
import org.jetbrains.kotlinx.dataframe.codeGen.GeneratedField
import org.jetbrains.kotlinx.dataframe.codeGen.ValidFieldName
import org.jetbrains.kotlinx.dataframe.impl.toCamelCaseByDelimiters
import org.jetbrains.kotlinx.dataframe.schema.ColumnSchema
import kotlin.reflect.typeOf

internal fun String.withTopInterfaceName(topInterfaceName: ValidFieldName): String =
    if (startsWith("${topInterfaceName.quotedIfNeeded}.")) this else "${topInterfaceName.quotedIfNeeded}.$this"

internal fun String.withoutTopInterfaceName(topInterfaceName: ValidFieldName): String =
    if (startsWith("${topInterfaceName.quotedIfNeeded}.")) {
        substringAfter("${topInterfaceName.quotedIfNeeded}.")
    } else {
        this
    }

internal fun String.snakeToLowerCamelCase(): String = toCamelCaseByDelimiters()

internal fun String.snakeToUpperCamelCase(): String =
    snakeToLowerCamelCase()
        .replaceFirstChar { it.uppercaseChar() }

internal fun String.toNullable() = if (this.last() == '?') this else "$this?"

internal interface IsObject {
    val isObject: Boolean
}

/** Helper function to create a [GeneratedField] without [GeneratedField.columnSchema]. */
internal fun generatedFieldOf(
    fieldName: ValidFieldName,
    columnName: String,
    overrides: Boolean,
    fieldType: FieldType,
): GeneratedField =
    GeneratedField(
        fieldName = fieldName,
        columnName = columnName,
        overrides = overrides,
        columnSchema = ColumnSchema.Value(typeOf<Any?>()), // unused
        fieldType = fieldType,
    )

/** Helper function to create a [GeneratedField] for enums. */
internal fun generatedEnumFieldOf(fieldName: ValidFieldName, columnName: String): GeneratedField =
    generatedFieldOf(
        fieldName = fieldName,
        columnName = columnName,
        overrides = false,
        fieldType = FieldType.ValueFieldType(typeOf<String>().toString()), // all enums will be of type String
    )

/** Small helper function to produce a new enum Marker. */
internal fun produceNewEnum(
    name: String,
    topInterfaceName: ValidFieldName,
    values: List<String>,
    nullable: Boolean,
    produceAdditionalMarker: ProduceAdditionalMarker,
): OpenApiMarker.Enum {
    val enumName = ValidFieldName.of(name.snakeToUpperCamelCase())
    val enumMarker = OpenApiMarker.Enum(
        name = enumName.quotedIfNeeded,
        fields = values.map {
            generatedEnumFieldOf(
                fieldName = ValidFieldName.of(it),
                columnName = it,
            )
        },
        nullable = nullable,
        topInterfaceName = topInterfaceName,
    )
    val newName = produceAdditionalMarker(enumName, enumMarker, isTopLevelObject = false)

    return enumMarker.withName(newName)
}
