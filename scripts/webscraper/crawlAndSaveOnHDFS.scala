/**
 * This script crawl and save the FBI documents on HDFS.
 *
 * Scaping articles are performed in parallel through Spark.
 * Then the articles are serialized as JSON and save on HDFS.
 */

import scala.collection.JavaConversions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.jsoup.nodes.Document
import org.jsoup.select.Elements
import org.jsoup.{Jsoup, Connection}

import com.impanchao.crawlcorruption._


////////////////////////////////////////////////////////////////////////////////////////
//import scala.sys.process._
//
//var process = Process(Seq("bash", "-c", "echo $YARN_CONF_DIR"),
//                      None,
//                      "YARN_CONF_DIR" -> "/usr/local/Cellar/hadoop/2.7.1/libexec/etc/hadoop/",
//                      "HADOOP_CONF_DIR" -> "/usr/local/Cellar/hadoop/2.7.1/libexec/etc/hadoop/")
//
//process.!
//
//
//// Create SparkContext
//val conf = new SparkConf().setAppName("Crawl FBI Articles").setMaster("yarn-client")
//val sc = new SparkContext(conf)
//
//// TODO: FAILED to launch Spark while using YARN as the cluster manager
////////////////////////////////////////////////////////////////////////////////////////

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
  }).collect()

processorsRDD.take(1)

//processorsRDD.
//  foreach(processor => {
//    counter += 1
//    val filename = Array("article", counter.value).mkString("-") + ".json"
//    println(filename + ": " + processor.url)
//    processor.saveOnHDFS(uri, dir, filename)
//    println("Done")
//  })



//var link = "https://www.fbi.gov/sanantonio/press-releases/2015/two-donna-school-board-members-indicted-on-bribery-and-attempted-extortion-charges"
//var processor = CrawledDocumentProcessor(link)
//processor.url








