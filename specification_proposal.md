
**PURPOSE**: A functional, fluent Kotlin wrapper API to work with JDBC and `ResultSet` Sequences

Please see [RxJava-JDBC](https://github.com/davidmoten/rxjava-jdbc) for inspiration behind these examples

####Example Declarations

```kotlin
data class User(val id: Int, val firstName: String, val lastName: String)

val ds: DataSource =  HikariConfig config = HikariConfig().apply {    
	        jdbcUrl = "jdbc:sqlite://C:/mydatabases/MyDb.db"
	        username = "bart"
	        password = "51mp50n"
	        minimumIdle = 1    
	        maximumPoolSize = 5 
          }.let { HikariDataSource(config) }

```

###Simple SELECT with Parameters

```kotlin
val user: Sequence<User> = ds.select("SELECT * FROM USER WHERE ID = #{id}")
   .parameter("id",2563)
   .get { User(it.getInt("ID"), it.getString("FIRST_NAME"), it.getString("LAST_NAME") } 

```

###Using a Sequence for Parameter Values

```kotlin
val ids = sequenceOf(2562,1212,322)

val user: Sequence<User> = ds.select("SELECT * FROM USER WHERE ID = ?")
   .parameters(ids)
   .get { User(it.getInt("ID"), it.getString("FIRST_NAME"), it.getString("LAST_NAME") } 

```

###Getting a Single `Sequence` of Values


```kotlin
val user: Sequence<String> = ds.select("SELECT FIRST_NAME || ' ' || LAST_NAME FROM USER")
   .getAs(String::class)
```


###Auto-Mapping

```kotlin
interface User {
    @Column("ID")
    val id: Int

    @Column("FIRST_NAME")
    val firstName: String

    @Column("LAST_NAME")
    val lastName: String
}


val user: Sequence<String> = ds.select("SELECT * FROM USER")
   .automap(User::class)
```


###INSERT with Generated Keys

```kotlin
val newUsers = sequenceOf(User(-1,"Anna","Smith"),User(-1,"Sam","Thompson"))

val newIds = newUsers.flatMap { 
    ds.insert("INSERT INTO USER (FIRST_NAME, LAST_NAME) VALUES (#{it.firstName},#{it.lastName})")
    .returnGeneratedKeys()
}
```

###Batching Inserts/Updates

```kotlin
val newUsers = ...

val newIds = newUsers.flatMap { 
    ds.insert("INSERT INTO USER (FIRST_NAME, LAST_NAME) VALUES (#{it.firstName},#{it.lastName})")
    .batchSize(1000)
    .returnGeneratedKeys()
}
```

