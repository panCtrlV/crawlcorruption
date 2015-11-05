import java.util._

import com.ibm.couchdb._

import scala.collection.JavaConversions._

import org.jsoup.nodes.Document
import org.jsoup.select.Elements
import org.jsoup.{Connection, Jsoup}

import scala.util.{Failure, Success, Try}
import scalaz.{-\/, \/-}


object CrawlMultipleArticlesAndSaveInCouchDBExample extends App {
  // Define class to store a crawled article
  case class CrawledCorruptionArticle(fetchTime: String,
                                      time: String,
                                      title: String,
                                      content: String,
                                      label: String = "corruption")

  val typeMapping = TypeMapping(classOf[CrawledCorruptionArticle] -> "CrawledCorruptionArticle")

  // Create a CouchDB client instance
  val couch = CouchDb("127.0.0.1", 5984)
  // Define a database name
  val dbName = "fbi-public-corruption-news"
  // Get an instance of the DB API by name and type mapping
  val db = couch.db(dbName, typeMapping)

  // Extraction methods
  def getArticleContent(doc: Document): String = {
    doc.getElementById("parent-fieldname-text").text()
  }

  def getArticleTitle(doc: Document): String = {
    doc.select("h1[class=documentFirstHeading]").text()
  }

  def getArticleTime(doc: Document): String = {
    doc.select("span[id=parent-fieldname-releaseDate]").text()
  }

  val userAgentString: String = "Mozilla/5.0 (X11; Linux x86_64) " + "AppleWebKit/535.21 (KHTML, like Gecko) " + "Chrome/45.0.2454.101 Safari/535.21"
  var pageIndex: Array[Int] = (0 to 1960 by 20).toArray  // 1960 as of Oct.28 2015
  val baseUrl: String = "https://www.fbi.gov/collections/public-corruption?b_start:int="
  var counter: Int = 0  // track crawled articles

  // Save all linked articles on one link page
  def saveAllFromOneLinkPage(index: Int, db: CouchDbApi): Unit = {
    val url: String = baseUrl + index
    val conn: Connection = Jsoup.connect(url).userAgent(userAgentString)
    val doc: Document = conn.get()

    // extract article links
    val articleLinks: Elements = doc.getElementsByTag("dl").select("a")
    val links: List[String] = articleLinks.toList.map {link => link.attr("href")}

    // save articles one-by-one
    for (link <- links) {
      counter += 1
      println(counter + " " + link)  // print each article link

      val fetchTime = Calendar.getInstance().getTime().toString
      val conn_to_link: Connection = Jsoup.connect(link).userAgent(userAgentString)
      val link_doc: Document = conn_to_link.get()

      // One can ignore an exception, for example:
      //  http://stackoverflow.com/questions/3960327/how-do-i-ignore-an-exception
      // But we can also catch the Exception and continue
      //  http://stackoverflow.com/questions/27434002/executing-a-code-block-even-after-catching-an-exception-in-scala
      Try {
            // create local object
            val crawledCorruptionArticle = CrawledCorruptionArticle(
              fetchTime,
              getArticleTime(link_doc),
              getArticleTitle(link_doc),
              getArticleContent(link_doc)
            )
            // attempt to write the object in CouchDB
            db.docs.create(crawledCorruptionArticle).attemptRun match {
              // In case of an error (left side of Either), print it
              case -\/(e) => println(e)
              // In case of a success (right side of Either), print each object
              case \/-(a) => println(a)
            }
      } match {
        // TODO: Log the exceptions
        case Success(x) => Some(x)
        case Failure(e) => println("Exception while parsing: " + e); None
      }
    }
  }

  // Save all
  // Create database. If already exists, ignore error and save in the existing database.
  couch.dbs.create(dbName).attemptRun match {
    // TODO: Log the exceptions
    case -\/(e) => println(e)
    case \/-(a) => println(a)
  }
  pageIndex.foreach(x => saveAllFromOneLinkPage(x, db))
}
