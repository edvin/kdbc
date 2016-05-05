package kdbc

import java.sql.*

class Database private constructor(val cp: ConnectionProvider) {

    fun select(sql: String, params: Array<*> = arrayOf<Any>()) =
        cp.get().prepareStatement(sql).apply {
            params.forEachIndexed { index, v ->
                val i = index + 1
                when (v) {
                    is Int -> setInt(i,v)
                    is Long -> setLong(i,v)
                    is String -> setString(i,v)
                    is Double -> setDouble(i,v)
                    is Float -> setFloat(i,v)
                    is Date -> setDate(i,v)
                    is Timestamp -> setTimestamp(i,v)
                    is Time -> setTime(i,v)
                    else -> setObject(i,v)
                }
            }
        }.asSequence()
}


fun PreparedStatement.asSequence(): Sequence<ResultSet> {
    return object: Iterator<ResultSet> {
        val rs = executeQuery()
        var hasMore = true
        init {
            hasMore = rs.next()
        }
        override fun next() = rs

        override fun hasNext():Boolean {
            hasMore = rs.next()
            if (!hasMore)
                rs.close()
            return hasMore
        }

    }.asSequence()
}