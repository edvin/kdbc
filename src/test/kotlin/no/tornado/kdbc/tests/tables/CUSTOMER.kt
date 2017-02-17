package no.tornado.kdbc.tests.tables

import kdbc.Table

class CUSTOMER : Table("customer") {
    val ID by column<Int>("integer not null primary key auto_increment")
    val NAME by column<String>("text")
}


