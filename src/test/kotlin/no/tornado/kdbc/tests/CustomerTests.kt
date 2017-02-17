package no.tornado.kdbc.tests

import kdbc.Insert
import kdbc.Query
import kdbc.Table
import kdbc.Update

data class Customer(var id: Int? = null, var name: String) {
    constructor(t: T_CUSTOMER) : this(t.ID(), t.NAME())
}

class T_CUSTOMER : Table("customer") {
    val ID by column<Int>("integer not null primary key auto_increment")
    val NAME by column<String>("text")
}

class InsertCustomer(customer: Customer) : Insert() {
    val C = T_CUSTOMER()

    init {
        INSERT(C) {
            C.NAME `=` customer.name
        }
        generatedKeys {
            customer.id = getInt(1)
        }
    }
}

class InsertCustomersInBatch(customers: List<Customer>) : Insert() {
    val C = T_CUSTOMER()

    init {
        // H2 Does not support generated keys in batch, so we can't retrieve them with `generatedKeys { }` here
        BATCH(customers) { customer ->
            INSERT(C) {
                C.NAME `=` customer.name
            }
        }
    }
}

class SelectCustomer : Query<Customer>() {
    val C = T_CUSTOMER()

    init {
        SELECT(C)
        FROM(C)
    }

    override fun rowItem() = Customer(C)

    fun byId(id: Int): Customer? {
        WHERE {
            C.ID `=` id
        }
        return firstOrNull()
    }
}

class UpdateCustomer(customer: Customer) : Update() {
    val C = T_CUSTOMER()

    init {
        UPDATE(C) {
            C.NAME `=` customer.name
        }
        WHERE {
            C.ID `=` customer.id
        }
    }
}

class DeleteCustomer(id: Int) : Query<Customer>() {
    val C = T_CUSTOMER()

    init {
        DELETE(C) {
            C.ID `=` id
        }
    }
}