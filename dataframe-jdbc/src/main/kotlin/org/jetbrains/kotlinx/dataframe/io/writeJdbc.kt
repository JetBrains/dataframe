package org.jetbrains.kotlinx.dataframe.io

import java.sql.Connection
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.io.db.DbType

enum class IfExists {
    FAIL,
    REPLACE,
    APPEND
}

public fun <T> DataFrame<T>.writeToSqlTable(
    con: Connection,
    name: String,
    schema: String? = null,
    inferNullability: Boolean = true,
    ifExists: IfExists = IfExists.FAIL,
    batchSize: Int = 1000,
    dbType: DbType? = null,
) {
    val qualifiedName = if (schema != null) "$schema.$name" else name
    val tableExists = doesTableExist(con, qualifiedName, dbType)

    when (ifExists) {
        IfExists.FAIL -> if (tableExists) throw IllegalArgumentException("Table $qualifiedName already exists.")
        IfExists.REPLACE -> {
            if (tableExists) dropTable(con, qualifiedName)
            createTable(this, con, qualifiedName, dbType)
        }
        IfExists.APPEND -> if (!tableExists) createTable(this, con, qualifiedName, dbType)
    }

    val batchLimit = batchSize ?: this.rowsCount()
    val insertQuery = buildInsertQuery(qualifiedName, columnNames())
    val preparedStatement: PreparedStatement = con.prepareStatement(insertQuery)

    con.autoCommit = false
    try {
        forEachBatch(this, batchLimit) { batch ->
            batch.forEach { row ->
                columnNames().forEachIndexed { index, columnName ->
                    val value = row[columnName]
                    preparedStatement.setObject(index + 1, value)
                }
                preparedStatement.addBatch()
            }
            preparedStatement.executeBatch()
        }
        con.commit()
    } catch (exception: Exception) {
        con.rollback()
        throw exception
    } finally {
        preparedStatement.close()
    }
}


public fun doesTableExist(connection: Connection, tableName: String, dbType: DbType?): Boolean {
    val query = "SELECT 1 FROM information_schema.tables WHERE table_name = ?"
    connection.prepareStatement(query).use { statement ->
        statement.setString(1, tableName)
        statement.executeQuery().use { resultSet ->
            return resultSet.next()
        }
    }
}

public fun <T> forEachBatch(dataFrame: DataFrame<T>, batchSize: Int, action: (DataFrame<T>) -> Unit) {
    val totalRows = dataFrame.rowsCount()
    for (start in 0 until totalRows step batchSize) {
        val end = minOf(start + batchSize, totalRows)
        val batch = dataFrame[start until end]
        action(batch)
    }
}

public fun <T> createTable(dataFrame: DataFrame<T>, connection: Connection, tableName: String, dbType: DbType?) {
    val columnsDefinition = dataFrame.columnNames().zip(dataFrame.columnTypes()).joinToString(", ") { (name, type) ->
        val sqlType = dbType.convertKTypeToSqlType(type)
        dbType?.handleNullable(sqlType, type.isMarkedNullable) ?: throw IllegalArgumentException ("dbType is not specified")
        "$name $sqlType"
    }
    val createQuery = "CREATE TABLE $tableName ($columnsDefinition)"
    connection.prepareStatement(createQuery).use { it.executeUpdate() }
}

public fun dropTable(connection: Connection, tableName: String) {
    val query = "DROP TABLE $tableName"
    connection.prepareStatement(query).use { it.executeUpdate() }
}

public fun buildInsertQuery(tableName: String, columnNames: List<String>): String {
    val placeholders = columnNames.joinToString(", ") { "?" }
    val columns = columnNames.joinToString(", ")
    return "INSERT INTO $tableName ($columns) VALUES ($placeholders)"
}





