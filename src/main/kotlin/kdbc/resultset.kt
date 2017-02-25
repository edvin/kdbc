package kdbc

import java.sql.ResultSet
import java.util.*

fun ResultSet.getUUID(label: String) = UUID.fromString(getString(label))
fun ResultSet.getLocalTime(label: String) = getTime(label).toLocalTime()
fun ResultSet.getLocalDateTime(label: String) = getTimestamp(label).toLocalDateTime()
fun ResultSet.getLocalDate(label: String) = getDate(label).toLocalDate()