import org.apache.spark._
import com.ibm.couchdb._


/**
 * Created by panc25 on 11/5/15.
 *
 * Experimenting how to access CouchDB from Spark.
 */

val couch = CouchDb("127.0.0.1", 5984)
