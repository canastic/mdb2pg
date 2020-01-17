import com.healthmarketscience.jackcess.DataType
import com.healthmarketscience.jackcess.DatabaseBuilder
import com.healthmarketscience.jackcess.Table
import config.mdbFilePath
import config.postgresConnString
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement
import java.sql.Types

object config {
    val postgresConnString = System.getenv("MDB2PG_POSTGRES_CONNSTRING")
    val mdbFilePath = System.getenv("MDB2PG_FILE_PATH")
}

fun main() {
    DriverManager.getConnection(postgresConnString).use { pgConn ->
        val pgMeta = pgConn.metaData

        DatabaseBuilder.open(File(mdbFilePath)). use { db ->
            db.tableNames.map { db.getTable(it) }.forEach { table ->
                val tableName = "${db.file.name}.${table.name}"
                println("Mapping $tableName...")
                val tableExists = pgMeta.getTables(null, "public", tableName, null).use { it.next() }
                if (!tableExists) {
                    pgConn.createTable(tableName, table)
                } else {
                    // val pgColumns = pgMeta.getColumns(null, "public", tableName, null).iterable()
                    //     .map { it.getString("COLUMN_NAME") to it.getInt("DATA_TYPE") }
                    //     .sortedBy { (name, _) -> name }
                    // val mdbColumns = table.columns
                    //     .map { it.name to it.type.asSql() }
                    //     .sortedBy { (name, _) -> name }
                    // if (pgColumns != mdbColumns) {
                    //     println("Table $tableName already existed in Postgres with different columns.")
                    //     for ((source, columns) in arrayOf("mdb" to mdbColumns, "Postgres" to pgColumns)) {
                    //         println("\t$source columns:\n${columns
                    //             .map { (name, type) -> "\t\t$name\t$type" }
                    //             .joinToString(separator = "\n")
                    //         }")
                    //     }
                    //     return
                    // }
                }

                for (row in table) {
                    pgConn.prepareStatement(
                        "INSERT INTO \"$tableName\" VALUES (${
                        (0 until table.columnCount).joinToString { "?" }
                        });"
                    ).use { stmt ->
                        for ((i, column) in table.columns.withIndex()) {
                            stmt.setObject(i + 1, column.getRowValue(row), column.sqlType)
                        }
                        stmt.execute()
                    }
                }
            }
        }
    }
}

fun ResultSet.iterable(): Iterable<ResultSet> = object : Iterable<ResultSet> {
    override fun iterator(): Iterator<ResultSet> = iterator {
        use {
            while (next()) {
                yield(this@iterable)
            }
        }
    }
}

fun DataType.asSql(): Int = when (this) {
    DataType.MEMO -> Types.LONGVARCHAR
    else -> this.sqlType
}

fun Connection.createTable(tableName: String, table: Table) {
    val columns = table.columns.map { column ->
        "\"${column.name}\" ${when (column.type.asSql()) {
            Types.BOOLEAN -> "BOOLEAN"
            Types.SMALLINT -> "INT2"
            Types.INTEGER -> "INT4"
            Types.BIGINT -> "INT8"
            Types.DECIMAL -> "NUMERIC"
            Types.DOUBLE -> "DOUBLE PRECISION"
            Types.TIMESTAMP -> "TIMESTAMP"
            Types.VARCHAR -> "VARCHAR"
            Types.NUMERIC -> "NUMERIC"
            Types.LONGVARCHAR -> "TEXT"
            else -> throw Exception("unsupported SQL type ${column.type.asSql()} for mdb type ${column.type} for column $tableName.${column.name}")
        }} NULL"
    }.joinToString(separator = ",\n")
    createStatement().use { stmt ->
        stmt.printAndExecute("CREATE TABLE \"$tableName\" ($columns);")
    }

    for (index in table.indexes) {
        createStatement().use { stmt ->
            stmt.printAndExecute(
                "CREATE ${if (index.isUnique) {
                    "UNIQUE "
                } else {
                    ""
                }}INDEX \"index_${tableName}_${index.name}\" ON \"$tableName\" (${
                index.columns.joinToString { "\"${it.name}\" ${if (it.isAscending) { "ASC"} else { "DESC" }}" }
                });"
            )
        }
        if (index.isPrimaryKey) {
            createStatement().use { stmt ->
                stmt.printAndExecute("ALTER TABLE \"$tableName\" ADD PRIMARY KEY USING INDEX \"${index.name}\";")
            }
        }
    }
}

private fun Statement.printAndExecute(sql: String): Boolean {
    println(sql)
    return execute(sql)
}
