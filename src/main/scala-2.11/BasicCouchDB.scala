import com.ibm.couchdb.{CouchDb, TypeMapping}

import scalaz.{\/-, -\/}

/**
 * https://github.com/beloglazov/couchdb-scala
 */

object BasicCouchDB extends App {

  // Define a simple case class to represent our data model
  case class Person(name: String, age: Int)

  // Define a type mapping used to transform class names into the doc kind
  val typeMapping = TypeMapping(classOf[Person] -> "Person")

  // Define some sample data
  val alice = Person("Alice", 25)
  val bob   = Person("Bob", 30)
  val carl  = Person("Carl", 20)

  // Create a CouchDB client instance
  val couch  = CouchDb("127.0.0.1", 5984)
  // Define a database name
  val dbName = "couchdb-scala-basic-example"
  // Get an instance of the DB API by name and type mapping
  val db     = couch.db(dbName, typeMapping)

  val actions = for {
  // Delete the database or ignore the error if it doesn't exist
    _ <- couch.dbs.delete(dbName).ignoreError
    // Create a new database
    _ <- couch.dbs.create(dbName)
    // Insert documents into the database
    _ <- db.docs.createMany(Seq(alice, bob, carl))
    // Retrieve all documents from the database and unserialize to Person
    docs <- db.docs.getMany.queryIncludeDocs[Person]
  } yield docs.getDocsData

  // Execute the actions and process the result
  actions.attemptRun match {
    // In case of an error (left side of Either), print it
    case -\/(e) => println(e)
    // In case of a success (right side of Either), print each object
    case \/-(a) => a.map(println(_))
  }
}