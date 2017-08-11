package kdbc

import java.math.BigDecimal
import java.sql.ResultSet
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*
import kotlin.reflect.KClass

@Suppress("UNCHECKED_CAST")
abstract class Table(private val name: String? = null) : ColumnOrTable {
    abstract val columns: List<Column<*>>

    var tableAlias: String? = null
    var rs: ResultSet? = null
    val tableName: String get() = name ?: javaClass.simpleName.toLowerCase()

    inline protected fun <reified T : Any> column(ddl: String? = null, noinline getter: (ResultSet.(String) -> T)? = null) = ColumnDelegate(ddl, getter ?: DefaultGetter(T::class))

    fun <T : Any> DefaultGetter(kClass: KClass<T>): ResultSet.(String) -> T = {
        when (kClass) {
            Int::class -> getInt(it) as T
            String::class -> getString(it) as T
            Double::class -> getDouble(it) as T
            Float::class -> getFloat(it) as T
            BigDecimal::class -> getBigDecimal(it) as T
            Date::class -> getDate(it) as T
            Long::class -> getLong(it) as T
            LocalDate::class -> getDate(it)?.toLocalDate() as T
            LocalDateTime::class -> getTimestamp(it)?.toLocalDateTime() as T
            else -> throw IllegalArgumentException("Default Column Getter cannot handle $kClass - supply a custom getter for this column")
        }
    }

    override fun toString() = if (tableAlias.isNullOrBlank() || tableAlias == tableName) tableName else "$tableName $tableAlias"

    // Generate DDL for this table. All columns that have a DDL statement will be included.
    fun ddl(dropIfExists: Boolean): String {
        val s = StringBuilder()
        if (dropIfExists) s.append("DROP TABLE IF EXISTS $tableName;\n")
        s.append("CREATE TABLE $tableName (\n")
        val ddls = columns.filter { it.ddl != null }.iterator()
        while (ddls.hasNext()) {
            val c = ddls.next()
            s.append("\t").append(c.name).append(" ").append(c.ddl)
            if (ddls.hasNext()) s.append(",\n")
        }
        s.append(")")
        return s.toString()
    }

    // Execute create table statement. Creates a query to be able to borrow a connection from the data source factory.
    fun create(dropIfExists: Boolean = false) {
        object : Query<Void>() {
            init {
                add(StringExpr(ddl(dropIfExists), this))
                execute()
            }
        }
    }
}

infix fun <T : Table> T.alias(alias: String): T {
    tableAlias = alias
    return this
}