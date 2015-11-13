import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.sources.EqualTo

import com.couchbase.spark._
import com.couchbase.spark.sql._
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject


val conf = new SparkConf().
  setAppName("couchbaseApiExample").
  setMaster("local[2]").
  set("com.couchbase.bucket.travel-sample", "")

val sc = new SparkContext(conf)

// Create RDD from existing database
//val docRDD = sc.couchbaseGet[JsonDocument](Seq("airline_10123", "airline_10748"))
sc.couchbaseGet[JsonDocument](Seq("airline_10123", "airline_10748")).collect().foreach(println)

// Save documents
sc.couchbaseGet[JsonDocument](Seq("airline_10123", "airline_10748")).
  map(oldDoc => {
    val id = "my_" + oldDoc.id()
    val content = JsonObject.create().put("name", oldDoc.content().getString("name"))
    JsonDocument.create(id, content)
  }).saveToCouchbase()

// Query
val sql = new SQLContext(sc)
val airlines = sql.read.couchbase(schemaFilter = EqualTo("type", "airline"))
airlines.collect()
airlines.printSchema()
airlines.select("name", "callsign").sort(airlines("name").desc)show(10)


