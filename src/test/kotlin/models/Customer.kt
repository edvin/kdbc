package models

import kdbc.Insert
import kdbc.Query
import kdbc.Table
import kdbc.Update
import java.sql.ResultSet
import java.sql.Types

data class Customer(var id: Int? = null, var name: String) {
    constructor(t: CustomerTable) : this(t.id(), t.name())
}

class CustomerTable : Table("customer") {
    val id by column { getInt(it) }
    val name by column { getString(it) }
}

class InsertCustomer(customer: Customer) : Insert() {
    val c = CustomerTable()

    init {
        INSERT(c) {
            c.name TO customer.name
        }
        generatedKeys {
            customer.id = getInt(1)
        }
    }
}

class SelectCustomer() : Query<Customer>() {
    val c = CustomerTable()

    init {
        SELECT(c.columns)
        FROM(c)
    }

    fun byId(id: Int): Customer = let {
        WHERE { c.id EQ id }
        first()
    }

    fun search(name: String): List<Customer> = let {
        WHERE { UPPER(c.name) LIKE UPPER("%$name%") }
        list()
    }

    override fun map(rs: ResultSet) = Customer(c)
}

class UpdateCustomer(customer: Customer) : Update() {
    val c = CustomerTable()
    init {
        UPDATE(c) {
            c.name TO customer.name
        }
        WHERE {
            c.id EQ customer.id
        }
    }
}