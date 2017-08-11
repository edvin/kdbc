package no.tornado.kdbc.tests.tables

import kdbc.Table

class CUSTOMER : Table() {
    val id = column<Int>("id", "integer not null primary key auto_increment")
    val name = column<String>("name", "text")
}