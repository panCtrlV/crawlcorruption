import com.ibm.couchdb._
import org.jsoup.nodes.Document
import org.jsoup.{Connection, Jsoup}
import scalaz.{-\/, \/-}

/**
 * Crawl one news article from FBI's public corruption website.
 * Save the article as a CrawledArticle object.
 * Store the object as a CouchDB document.
 */

object CrawlOneArticleAndSaveInCouchDBExample extends App {
  // Define class to store a crawled article
  case class CrawledArticle(time: String, title: String, content: String)

  // Crawl a webpage and save the document as an Article
  def getArticleContent(doc: Document): String = {
    doc.getElementById("parent-fieldname-text").text()
  }

  def getArticleTitle(doc: Document): String = {
    doc.select("h1[class=documentFirstHeading]").text()
  }

  def getArticleTime(doc: Document): String = {
    doc.select("span[id=parent-fieldname-releaseDate]").text()
  }

  var url = "https://www.fbi.gov/atlanta/press-releases/2015/dekalb-county-sheriffs-captain-charged-with-encouraging-excessive-force-at-county-jail-and-obstruction-of-justice"
  val userAgentString: String = {"Mozilla/5.0 (X11; Linux x86_64) " +
    "AppleWebKit/535.21 (KHTML, like Gecko) " +
    "Chrome/19.0.1042.0 Safari/535.21"}
  var conn: Connection = Jsoup.connect(url).userAgent(userAgentString)
  var doc: Document = conn.get()

  var crawledArticle = CrawledArticle(getArticleTime(doc), getArticleTitle(doc), getArticleContent(doc))

  // Save the document object into a CouchDB database
  val typeMapping = TypeMapping(classOf[CrawledArticle] -> "CrawledArticle")

  // Create CouchDB database "fbi-public-corruption-news"
  val couch = CouchDb("127.0.0.1", 5984)
  val dbName = "fbi-public-corruption-news"
  val db = couch.db(dbName, typeMapping)

  val actions = for {
  // Delete the database or ignore the error if it doesn't exist
    _ <- couch.dbs.delete(dbName).ignoreError
    // Create a new database
    _ <- couch.dbs.create(dbName)
    // Insert documents into the database
    _ <- db.docs.createMany(Seq(crawledArticle))
    // Retrieve all documents from the database and unserialize to CrawledArticle
    docs <- db.docs.getMany.queryIncludeDocs[CrawledArticle]
  } yield docs.getDocsData

  // Execute the actions and process the result
  actions.attemptRun match {
    // In case of an error (left side of Either), print it
    case -\/(e) => println(e)
    // In case of a success (right side of Either), print each object
    case \/-(a) => a.map(println(_))
  }
}
