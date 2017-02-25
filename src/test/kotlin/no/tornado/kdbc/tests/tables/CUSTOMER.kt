package no.tornado.kdbc.tests.tables

import kdbc.Table

class CUSTOMER : Table() {
    val id by column<Int>("integer not null primary key auto_increment")
    val name by column<String>("text")
}