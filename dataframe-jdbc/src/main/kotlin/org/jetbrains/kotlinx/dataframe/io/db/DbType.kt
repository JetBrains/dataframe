package org.jetbrains.kotlinx.dataframe.io.db

import org.jetbrains.kotlinx.dataframe.io.TableColumnMetadata
import org.jetbrains.kotlinx.dataframe.io.TableMetadata
import org.jetbrains.kotlinx.dataframe.schema.ColumnSchema
import java.sql.ResultSet
import kotlin.reflect.KType

/**
 * The `DbType` class represents a database type used for reading dataframe from the database.
 *
 * @property [dbTypeInJdbcUrl] The name of the database as specified in the JDBC URL.
 */
public abstract class DbType(public val dbTypeInJdbcUrl: String) {
    /**
     * Represents the JDBC driver class name for a given database type.
     *
     * NOTE: It's important for usage in dataframe-gradle-plugin for force class loading.
     *
     * @return The JDBC driver class name as a [String].
     */
    public abstract val driverClassName: String

    /**
     * Returns a [ColumnSchema] produced from [tableColumnMetadata].
     */
    public abstract fun convertSqlTypeToColumnSchemaValue(tableColumnMetadata: TableColumnMetadata): ColumnSchema?

    /**
     * Checks if the given table name is a system table for the specified database type.
     *
     * @param [tableMetadata] the table object representing the table from the database.
     * @param [dbType] the database type to check against.
     * @return True if the table is a system table for the specified database type, false otherwise.
     */
    public abstract fun isSystemTable(tableMetadata: TableMetadata): Boolean

    /**
     * Builds the table metadata based on the database type and the ResultSet from the query.
     *
     * @param [tables] the ResultSet containing the table's meta-information.
     * @return the TableMetadata object representing the table metadata.
     */
    public abstract fun buildTableMetadata(tables: ResultSet): TableMetadata

    /**
     * Converts SQL data type to a Kotlin data type.
     *
     * @param [tableColumnMetadata] The metadata of the table column.
     * @return The corresponding Kotlin data type, or null if no mapping is found.
     */
    public abstract fun convertSqlTypeToKType(tableColumnMetadata: TableColumnMetadata): KType?

    /**
     * Converts a Kotlin type ([KType]) to its corresponding SQL type as a String.
     *
     * @param kType The Kotlin type to be converted.
     * @return The corresponding SQL type as a String, or null if no matching SQL type exists.
     */
    public abstract fun convertKTypeToSqlType(kType: KType): String?

    /**
     * Constructs a SQL query with a limit clause.
     *
     * @param sqlQuery The original SQL query.
     * @param limit The maximum number of rows to retrieve from the query. Default is 1.
     * @return A new SQL query with the limit clause added.
     */
    public open fun sqlQueryLimit(sqlQuery: String, limit: Int = 1): String = "$sqlQuery LIMIT $limit"

    /**
     * Handles optional type conversion for nullable values.
     */
    public open fun handleNullable(sqlType: String, isNullable: Boolean): String {
        return if (isNullable) "$sqlType NULL" else "$sqlType NOT NULL"
    }
}
