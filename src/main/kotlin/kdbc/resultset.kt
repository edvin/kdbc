package kdbc

import java.sql.ResultSet
import java.util.*

fun ResultSet.getUUID(label: String) = getString(label)?.let { UUID.fromString(it) }
fun ResultSet.getLocalTime(label: String) = getTime(label)?.toLocalTime()
fun ResultSet.getLocalDateTime(label: String) = getTimestamp(label)?.toLocalDateTime()
fun ResultSet.getLocalDate(label: String) = getDate(label)?.toLocalDate()

fun <T> ResultSet.iterator(mapper: (ResultSet) -> T) = ResultSetIterator(this,mapper)

fun <T> ResultSet.asSequence(mapper: (ResultSet) -> T) = iterator(mapper).asSequence()

class ResultSetIterator<out T>(val rs: ResultSet, val mapper: (ResultSet) -> T) : Iterator<T> {
    private var didNext = false
    private var hasNext = false

    override fun next(): T {
        if (!didNext) {
            rs.next()
        }
        didNext = false
        return mapper(rs)
    }

    override fun hasNext(): Boolean {
        if (!didNext) {
            hasNext = rs.next()
            didNext = true
        }
        return hasNext
    }

}