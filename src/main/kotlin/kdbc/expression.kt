package kdbc

import kotlin.reflect.KClass

abstract class Expr(val parent: Expr?) {
    companion object {
        val NoSpaceWhenLastChar = arrayOf(' ', '(', ')', '\n')
    }

    val query: Query<*> get() = if (this is Query<*>) this else parent as? Query<*> ?: parent!!.query

    val expressions = mutableListOf<Expr>()

    fun gatherParams(params: MutableList<Param>) {
        if (this is ComparisonExpr) params.add(param)
        expressions.forEach { it.gatherParams(params) }
    }

    internal fun <T : Expr> add(expression: T, op: (T.() -> Unit)? = null): T {
        op?.invoke(expression)
        expressions.add(expression)
        return expression
    }

    fun render(): String {
        val s = StringBuilder()
        render(s)
        return s.toString()
    }

    open fun render(s: StringBuilder) {
        renderChildren(s)
    }

    protected fun renderChildren(s: StringBuilder) {
        expressions.forEach {
            prefixWithSpace(s)
            it.render(s)
        }
    }

    protected fun prefixWithSpace(s: StringBuilder) {
        if (s.isNotEmpty() && !NoSpaceWhenLastChar.contains(s.last()))
            s.append(" ")
    }

    fun append(sql: String) = add(StringExpr(sql, this))
    operator fun String.unaryPlus() = append(this)

    infix fun Expr.join(table: Table): JoinExpr {
        query.addTable(table)
        return add(JoinExpr(table, this))
    }

    val Expr.left: JoinDiscriminatorExpr get() = add(JoinDiscriminatorExpr("LEFT", this))
    val Expr.right: JoinDiscriminatorExpr get() = add(JoinDiscriminatorExpr("RIGHT", this))
    val Expr.outer: JoinDiscriminatorExpr get() = add(JoinDiscriminatorExpr("OUTER", this))
    val Expr.inner: JoinDiscriminatorExpr get() = add(JoinDiscriminatorExpr("INNER", this))

    infix fun JoinExpr.outer(joinExpr: JoinExpr): JoinExpr {
        joinExpr.expressions.add(0, JoinDiscriminatorExpr("OUTER", joinExpr))
        return joinExpr
    }

    infix fun JoinExpr.inner(joinExpr: JoinExpr): JoinExpr {
        joinExpr.expressions.add(0, JoinDiscriminatorExpr("INNER", joinExpr))
        return joinExpr
    }

    fun groupby(sql: String) = add(GroupByExpr(sql, this))

    fun having(op: HavingExpr.() -> Unit) = add(HavingExpr(this), op)

    fun <T> batch(entities: Iterable<T>, large: Boolean = false, op: (BatchExpr<T>).(T) -> Unit) =
            add(BatchExpr(entities, large, op, this))

    fun select(vararg columns: ColumnOrTable, op: (SelectExpr.() -> Unit)? = null) =
            add(SelectExpr(columns.flatMap { (it as? Table)?.columns ?: listOf(it as Column<*>) }, this), op)

    fun select(columns: Iterable<ColumnOrTable>, op: (SelectExpr.() -> Unit)? = null) =
            add(SelectExpr(columns.flatMap { (it as? Table)?.columns ?: listOf(it as Column<*>) }, this), op)

    fun <T : Table> update(table: T, op: (SetExpr.() -> Unit)? = null): UpdateExpr {
        val updateExpr = add(UpdateExpr(table, this))
        updateExpr.add(SetExpr(updateExpr), op)
        return updateExpr
    }

    fun set(op: SetExpr.() -> Unit) = add(SetExpr(this), op)

    fun <T : Table> insert(table: T, op: InsertExpr.() -> Unit)
            = add(InsertExpr(table, this), op)

    fun delete(table: Table, op: (DeleteExpr.() -> Unit)? = null) =
            add(DeleteExpr(table, this), op)

    fun from(vararg tables: Table, op: (FromExpr.() -> Unit)? = null) =
            add(FromExpr(tables.toList(), this), op)

    fun where(op: WhereExpr.() -> Unit) =
            add(WhereExpr(this), op)

    infix fun Column<*>.`in`(op: InExpr.() -> Unit) {
        add(InExpr(this, parent!!), op)
    }

    fun and(op: AndExpr.() -> Unit) = add(AndExpr(this), op)

    fun or(op: OrExpr.() -> Unit) = add(OrExpr(this), op)

    infix fun ComparisonExpr.type(type: Int): ComparisonExpr {
        param.type = type
        return this
    }

    infix fun <T> ComparisonExpr.handler(handler: TypeHandler<T>): ComparisonExpr {
        // Same issues as above
        param.handler = handler as TypeHandler<Any?>
        return this
    }

    infix fun <T : Any> ComparisonExpr.handler(type: KClass<T>): ComparisonExpr {
        // Same issues as above
        param.handler = typeHandlers[type] as TypeHandler<Any?>
        return this
    }

    infix fun Any.`=`(param: Any?) = createComparison(this, param, "=")
    infix fun Any.eq(param: Any?) = createComparison(this, param, "=")
    infix fun Any.like(param: Any?) = createComparison(this, param, "LIKE")
    infix fun Any.gt(param: Any?) = createComparison(this, param, ">")
    infix fun Any.gte(param: Any?) = createComparison(this, param, ">=")
    infix fun Any.lt(param: Any?) = createComparison(this, param, "<")
    infix fun Any.lte(param: Any?) = createComparison(this, param, "<=")

    fun upper(value: Any?) = TextTransform(TextTransform.Type.UPPER, value)
    fun lower(value: Any?) = TextTransform(TextTransform.Type.LOWER, value)

    private fun createComparison(receiver: Any?, param: Any?, sign: String)
            = add(ComparisonExpr(receiver, sign, param, this@Expr))

}

class ComparisonExpr(val column: Any?, val sign: String, val value: Any?, parent: Expr) : Expr(parent = parent) {
    val param = Param(if (value is TextTransform) value.value else value)
    val transform: TextTransform.Type? = if (value is TextTransform) value.type else null

    override fun render(s: StringBuilder) {
        s.append(column).append(" ").append(sign)
        val spacer = if (transform != null) "" else " "
        if (transform != null) s.append(" ").append(transform).append("(")
        if (value is Column<*>) s.append(spacer).append(value.fullName)
        else s.append(spacer).append("?")
        if (transform != null) s.append(")")
    }
}

class OrExpr(parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        if (expressions.isEmpty()) return
        if ((parent is WhereExpr || parent is AndExpr || parent is OrExpr) && parent.expressions.indexOf(this) == 0) {
            // Skip adding OR for the first parent of WHERE/AND/OR
        } else {
            s.append("OR ")
        }
        val wrap = expressions.size > 1
        if (wrap) s.append("(")
        super.render(s)
        if (wrap) s.append(")")
    }

}

class StringExpr(val sql: String, parent: Expr) : Expr(parent = parent) {
    override fun render(s: StringBuilder) {
        s.append(sql)
    }
}

class AndExpr(parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        if (expressions.isEmpty()) return
        if ((parent is WhereExpr || parent is AndExpr || parent is OrExpr) && parent.expressions.indexOf(this) == 0) {
            // Skip adding AND for the first parent of WHERE/AND/OR
        } else {
            s.append("AND ")
        }

        val wrap = expressions.size > 1
        if (wrap) s.append("(")
        super.render(s)
        if (wrap) s.append(")")
    }
}


class JoinDiscriminatorExpr(val discriminator: String, parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        s.append("\n$discriminator")
        super.render(s)
    }
}

class JoinOnExpr(parent: Expr) : Expr(parent)

class JoinExpr(val table: Table, parent: Expr) : Expr(parent) {
    init {
        query.addTable(table)
    }

    infix fun on(op: (JoinOnExpr) -> Unit): JoinExpr {
        add(StringExpr("ON", this))
        add(JoinOnExpr(this), op)
        return this
    }

    override fun render(s: StringBuilder) {
        if (parent !is JoinDiscriminatorExpr) s.append("\n")
        s.append("JOIN ").append(table)
        super.render(s)
    }
}

class BatchExpr<T>(val entities: Iterable<T>, val large: Boolean, val op: (BatchExpr<T>).(T) -> Unit, parent: Expr) : Expr(parent)

class SelectExpr(val columns: Iterable<Column<*>>, parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        s.append("SELECT ")
        s.append(columns.map { it.asAlias }.joinToString(", "))
        super.render(s)
    }
}

class InsertExpr(val table: Table, parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        s.append("INSERT INTO ${table.tableName} (")
        val pairs = expressions.map { it as? ComparisonExpr }.filterNotNull()
        s.append(pairs.map { it.column }.joinToString(", "))
        s.append(") VALUES (")
        s.append(pairs.map { "?" }.joinToString(", "))
        s.append(")")
    }
}

class UpdateExpr(val table: Table, parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        s.append("UPDATE ").append(table.tableName).append(" ")
        super.render(s)
    }
}

// TODO: Type safe
class GroupByExpr(sql: String, parent: Expr) : Expr(parent) {
    init {
        add(StringExpr(sql, this))
    }

    override fun render(s: StringBuilder) {
        s.append("\nGROUP BY ")
        super.render(s)
    }
}

class HavingExpr(parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        s.append("\nHAVING ")
        super.render(s)
    }
}

class SetExpr(parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        s.append("\nSET ")
        expressions.forEachIndexed { i, expr ->
            prefixWithSpace(s)
            expr.render(s)
            if (i < expressions.size - 1) s.append(",\n")
        }
    }
}

class DeleteExpr(val table: Table, parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        s.append("DELETE FROM $table ")
        if (expressions.firstOrNull() !is WhereExpr) s.append("\nWHERE ")
        super.render(s)
    }
}

class FromExpr(val fromTables: List<Table>, parent: Expr) : Expr(parent) {
    init {
        query.apply {
            fromTables.forEach { addTable(it) }
        }
    }

    override fun render(s: StringBuilder) {
        if (parent is Query<*>) s.append("\n")
        s.append("FROM ")
        if (fromTables.isNotEmpty())
            s.append(fromTables.joinToString(", "))
        super.render(s)
    }
}

class WhereExpr(parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        if (expressions.isEmpty()) return
        if (parent is Query<*>) s.append("\n")
        s.append("WHERE ")
        super.render(s)
    }
}

class InExpr(val column: Column<*>, parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        s.append(column.fullName)
        s.append(" IN (")
        super.render(s)
        s.append(")")
    }
}

class TextTransform(val type: Type, val value: Any?) {
    enum class Type { UPPER, LOWER }

    override fun toString(): String {
        val s = StringBuilder()
        s.append("$type($value)")
        return s.toString()
    }
}

class Param(val value: Any?) {
    var type: Int? = null
    var handler: TypeHandler<Any?>? = null

    override fun toString() = StringBuilder(value.toString()).let {
        if (type != null) it.append(" (SQL Type $type)")
        it.toString()
    }

}