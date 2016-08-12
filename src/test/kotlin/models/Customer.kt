package models

import kdbc.AS
import kdbc.Insert
import kdbc.Query
import kdbc.Table
import java.sql.ResultSet

class Customer(val id: Int, val name: String) {
    constructor(t: CustomerTable): this(t.id(), t.name())
}

class CustomerTable : Table("customer") {
    val id by column { getInt(it) }
    val name by column { getString(it) }
}

class InsertCustomer(customer: Customer) : Insert() {
    val c = CustomerTable()

    init {
        SELECT(c.columns)
        FROM(c)
    }

}

class SelectCustomer() : Query<Customer>() {
    val c = CustomerTable()

    init {
        SELECT(c.columns)
        FROM(c)
    }

    fun byId(id: Int) = apply {
        WHERE {
            c.id EQ id
        }
    }

    fun search(name: String) = apply {
        WHERE {
            UPPER(c.name) LIKE UPPER("%$name%")
        }
    }

    override fun map(rs: ResultSet) = Customer(c)
}
