package kdbc

import java.sql.Connection
import java.sql.ResultSet

class SelectBuilder(val sql: String, val connection: Connection) {

    private val qb = QueryBuilder(sql, connection)

    fun param(name: String, value: Any?, type: Int): SelectBuilder {
        qb.param(name,value,type)
        return this
    }
    fun param(name: String, value: Any): SelectBuilder {
        qb.param(name,value)
        return this
    }
    fun params(vararg params: Any): SelectBuilder {
        qb.params(params)
        return this
    }

    fun <T> get(rsMapper: (ResultSet) -> T) =
            object: Iterator<ResultSet> {
                val rs = qb.stmt.executeQuery()
                var hasMore = true

                override fun next() = rs

                override fun hasNext():Boolean {
                    hasMore = rs.next()
                    if (!hasMore) {
                        rs.close()
                        connection.close()
                    }
                    return hasMore
                }
            }.asSequence().map(rsMapper)
}

class UpdateBuilder(val sql: String, val connection: Connection) {
    private val qb = QueryBuilder(sql, connection)
    private var batchSize = 0

    fun batchSize(batchSize: Int): UpdateBuilder {
        this.batchSize = batchSize
        return this
    }

    fun param(name: String, value: Any?, type: Int): UpdateBuilder {
        qb.param(name,value,type)
        return this
    }
    fun param(name: String, value: Any): UpdateBuilder {
        qb.param(name,value)
        return this
    }
    fun params(vararg params: Any): UpdateBuilder {
        qb.params(params)
        return this
    }
    fun <T> count(rsMapper: (ResultSet) -> T) =
            object: Iterator<ResultSet> {
                val rs = qb.stmt.executeQuery()
                var hasMore = true

                override fun next() = rs

                override fun hasNext():Boolean {
                    hasMore = rs.next()
                    if (!hasMore) {
                        rs.close()
                        connection.close()
                    }
                    return hasMore
                }
            }.asSequence()
}