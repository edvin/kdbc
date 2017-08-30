package no.tornado.kdbc.tests.tables

import kdbc.Table
import java.util.*

class CUSTOMER : Table() {
    val id = column<Int>("id", "integer not null primary key auto_increment")
    val uuid = column<UUID>("uuid", "UUID not null")
    val name = column<String>("name", "text")
}