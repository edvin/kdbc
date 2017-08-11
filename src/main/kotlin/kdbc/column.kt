package kdbc

import java.sql.ResultSet

interface ColumnOrTable

class Column<out T>(val table: Table, val name: String, val ddl: String?, val getter: ResultSet.(String) -> T?, var rs: () -> ResultSet) : ColumnOrTable {
    override fun toString() = fullName
    val fullName: String get() = if (table.tableAlias != null) "${table.tableAlias}.$name" else name
    val alias: String get() = fullName.replace(".", "_")
    val asAlias: String get() = if (table.tableAlias != null) "$fullName $alias" else alias
    val value: T? get() = getter(rs(), alias)
    val v: T get() = getter(rs(), alias)!!
    val isNull: Boolean get() = value == null
    val isNotNull: Boolean get() = !isNull
    operator fun invoke(): T = v
}