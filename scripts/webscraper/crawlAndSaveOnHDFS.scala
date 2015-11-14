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
import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

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

val conf = new SparkConf().setAppName("Crawl FBI Articles").setMaster("local[4]")
val sc = new SparkContext(conf)

/*
Crawl articles and save on HDFS
 */
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
val articleLinksRDD = pageIndexRDD.flatMap(ind => getLinksFromOnePage(ind)).persist()

// HDFS URI and target folder
val uri = "hdfs://127.0.0.1:9000"
val dir = "/stat598bigdata/fbi-public-corruption-articles/"

// Crawll all articles and save on HDFS
val processorsRDD =
articleLinksRDD.
  map(link =>{
    try {
      Some(CrawledDocumentProcessor(link))
    } catch {
      case e: Exception => println("Error in creating CrawledDocumentProcessor " + e)
      None
    }
  }).
  map(_.get)

processorsRDD.
  foreach(processor => {
    counter += 1
    val filename = "article-" + UUID.randomUUID() + ".json"
    println(counter + ": " + filename + ": " + processor.url)
    try {
      processor.saveOnHDFS(uri, dir, filename)
    } catch {
      case e: Exception => println("Error writing to HDFS " + e); None
    }
  })

// Read from HDFS
val allFbiArticlesTextFilesRDD = sc.wholeTextFiles(uri+dir)
//allFbiArticles.collect().length
//allFbiArticles.take(10).foreach(println)

// Create mapper
val mapper = new ObjectMapper() with ScalaObjectMapper
mapper.registerModule(DefaultScalaModule)

// Deserialize JSON string to CrawledCorruptionArticle objects
val allFbiArticlesObjectsRDD = allFbiArticlesTextFilesRDD.
  flatMap(t => {
    try {
      Some(mapper.readValue(t._2, classOf[CrawledCorruptionArticle]))
    } catch {
      case e: Exception => println("Error in deserialization " + e); None
    }
  }).persist

allFbiArticlesObjectsRDD.foreach(x => println(x.getFetchTime))






