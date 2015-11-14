import scala.collection.JavaConversions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.jsoup.nodes.Document
import org.jsoup.select.Elements
import org.jsoup.{Jsoup, Connection}

import com.impanchao.crawlcorruption._


object CrawledDocumentProcessorRDDExample extends App {
  val conf = new SparkConf().setAppName("Crawl FBI Articles").setMaster("local[2]")
  val sc = new SparkContext(conf)

  /*
  Crawl articles and save on HDFS
   */

  val userAgentString: String = "Mozilla/5.0 (X11; Linux x86_64) " + "AppleWebKit/535.21 (KHTML, like Gecko) " + "Chrome/45.0.2454.101 Safari/535.21"
  //var counter: Int = 0  // track crawled articles
  // Use Spark accumulator as a counter
  val counter = sc.accumulator(0, "Crawled Article Counter")

  // Prepare links to articles
  var pageIndexArray: Array[Int] = (0 to 1980 by 20).toArray  // 1980 as of Nov. 11 2015
  val baseUrl: String = "https://www.fbi.gov/collections/public-corruption?b_start:int="

  // Get article links from one page
  def getLinksFromOnePage(index: Int) = {
    val url: String = baseUrl + index
    val connToLinkPage: Connection = Jsoup.connect(url).userAgent(userAgentString)
    val docOfLinkPage: Document = connToLinkPage.get()
    // extract all article links on the page
    val articleLinks: Elements = docOfLinkPage.getElementsByTag("dl").select("a")
    articleLinks.toList.map {link => link.attr("href")}
  }

  // Get all article links
  val pageIndexRDD =  sc.parallelize(pageIndexArray)
  //pageIndexRDD.take(10)
  val articleLinksRDD = pageIndexRDD.flatMap(ind => getLinksFromOnePage(ind)).persist()
  //val articleLinks = articleLinksRDD.collect()
  //articleLinksRDD.take(1)(0)

  //articleLinksRDD.take(20).foreach(println)  // order preserved

  // Crawl all article links
  val uri = "hdfs://127.0.0.1:9000"
  val dir = "/stat598bigdata/fbi-public-corruption-articles/"

  // Crawll all articles and save on HDFS
  val processorsRDD =
    articleLinksRDD.
      flatMap(link =>{
        try {
          Some(CrawledDocumentProcessor(link))
        } catch {
          case e: Exception => println("Error in creating CrawledDocumentProcessor " + e)
            None
        }
      })

  println(processorsRDD.take(1))
}
