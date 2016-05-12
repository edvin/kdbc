
**PURPOSE**: A functional, fluent Kotlin wrapper API to work with JDBC and `ResultSet` Sequences

#SELECT Usage Examples


###Simple SELECT with Parameter
```kotlin
data class User(val id: Int, val firstName: String, val lastName: String)

val ds: DataSource =  HikariConfig config = HikariConfig().apply {    
	        jdbcUrl = "jdbc:sqlite://C:/mydatabases/MyDb.db"
	        username = "bart"
	        password = "51mp50n"
	        minimumIdle = 1    
	        maximumPoolSize = 5 
          }.let { HikariDataSource(config) }


val user: Sequence<User> = ds.select("SELECT * FROM USER WHERE ID = :id")
   .parameter("id",2563)
   .get { User(it.getInt("ID"), it.getString("FIRST_NAME"), it.getString("LAST_NAME") } 

```

###Using a Sequence for Parameter Values

```kotlin
val sequences = sequenceOf(2562,1212,322)

val user: Sequence<User> = ds.select("SELECT * FROM USER WHERE ID = ?")
   .parameters(sequences)
   .get { User(it.getInt("ID"), it.getString("FIRST_NAME"), it.getString("LAST_NAME") } 

```
###Getting a Single `Sequence` of Values


```kotlin
val user: Sequence<String> = ds.select("SELECT FIRST_NAME || ' ' || LAST_NAME FROM USER")
   .getAs(String::class)
```
